/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package reuse

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/ai-on-gke/tpu-provisioner/internal/cloud"
	containerv1beta1 "google.golang.org/api/container/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	jobset "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

const (
	singletonReconcileName      = "reuse-planner"
	singletonReconcileNamespace = "default"

	defaultUnhealthySLOGrace   = 30 * time.Second
	defaultReleasingMaxTimeout = 60 * time.Second
	defaultCreateBackoff       = 5 * time.Second
	maxAsyncGKEConcurrency     = 8

	labelNodeFailed = "cloud.google.com/gke-tpu-node-failed"
	restartsLabel   = "jobset.sigs.k8s.io/restart-attempt"
)

type inFlightCreate struct {
	PoolName   string
	PoolKey    string
	SpecHash   string
	Spot       bool
	TotalChips int
	JobSetKey  types.NamespacedName
	StartedAt  time.Time
}

type inFlightDelete struct {
	PoolName  string
	PoolKey   string
	StartedAt time.Time
}

type createCooldown struct {
	Until time.Time
	Err   string
}

// ReuseReconciler coordinates reservation- and cell-aware dynamic GKE NodePool reuse,
// strict delete-before-create sequencing, and JobSet.Spec.Suspend gang barriers.
type ReuseReconciler struct {
	client.Client
	APIReader client.Reader
	Scheme    *runtime.Scheme
	Recorder  record.EventRecorder

	Backend             cloud.ReusePoolBackend
	Capacity            *CapacityBook
	EnableNodePoolReuse bool

	// SynchronousGKEOps executes CreateRawNodePool/DeleteRawNodePool inline during Reconcile
	// (used in deterministic unit tests; false in production controller manager).
	SynchronousGKEOps bool
	UnhealthySLOGrace time.Duration

	mu              sync.Mutex
	reconcileMu     sync.Mutex
	inFlightCreates map[string]*inFlightCreate
	inFlightDeletes map[string]*inFlightDelete
	createCooldowns map[string]createCooldown
	seenHealthyAt   map[string]time.Time
	degradedSince   map[string]time.Time
	releasingSince  map[string]time.Time
	minJobSetRV     map[types.NamespacedName]uint64
	notifyCh        chan event.GenericEvent
	gkeSem          chan struct{}
}

// NewReuseReconciler initializes a ReuseReconciler with internal state maps.
func NewReuseReconciler(
	k8sClient client.Client,
	apiReader client.Reader,
	scheme *runtime.Scheme,
	recorder record.EventRecorder,
	backend cloud.ReusePoolBackend,
	capacity *CapacityBook,
	enableReuse bool,
) *ReuseReconciler {
	if apiReader == nil {
		apiReader = k8sClient
	}
	if capacity == nil {
		capacity, _ = NewCapacityBook(RawCapacityConfig{})
	}
	return &ReuseReconciler{
		Client:              k8sClient,
		APIReader:           apiReader,
		Scheme:              scheme,
		Recorder:            recorder,
		Backend:             backend,
		Capacity:            capacity,
		EnableNodePoolReuse: enableReuse,
		UnhealthySLOGrace:   defaultUnhealthySLOGrace,
		inFlightCreates:     make(map[string]*inFlightCreate),
		inFlightDeletes:     make(map[string]*inFlightDelete),
		createCooldowns:     make(map[string]createCooldown),
		seenHealthyAt:       make(map[string]time.Time),
		degradedSince:       make(map[string]time.Time),
		releasingSince:      make(map[string]time.Time),
		minJobSetRV:         make(map[types.NamespacedName]uint64),
		notifyCh:            make(chan event.GenericEvent, 1),
		gkeSem:              make(chan struct{}, maxAsyncGKEConcurrency),
	}
}

func (r *ReuseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	toSingleton := handler.EnqueueRequestsFromMapFunc(func(_ context.Context, _ client.Object) []reconcile.Request {
		return []reconcile.Request{{
			NamespacedName: types.NamespacedName{
				Namespace: singletonReconcileNamespace,
				Name:      singletonReconcileName,
			},
		}}
	})

	return ctrl.NewControllerManagedBy(mgr).
		Named("nodepool-reuse").
		Watches(&jobset.JobSet{}, toSingleton).
		Watches(&batchv1.Job{}, toSingleton).
		Watches(&corev1.Pod{}, toSingleton).
		Watches(&corev1.Node{}, toSingleton).
		WatchesRawSource(source.Channel(r.notifyCh, toSingleton)).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 1,
		}).
		Complete(r)
}

func (r *ReuseReconciler) triggerWakeup() {
	dummy := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:      singletonReconcileName,
			Namespace: singletonReconcileNamespace,
		},
	}
	select {
	case r.notifyCh <- event.GenericEvent{Object: dummy}:
	default:
	}
}

type sliceRequirement struct {
	SliceKey string
	PoolKey  string
	Limit    PoolLimit
	Spec     *cloud.ReusablePoolSpec
}

type gangPlan struct {
	JobSet       *jobset.JobSet
	Key          types.NamespacedName
	Slices       []sliceRequirement
	IsSpot       bool
	Priority     int32
	HasExisting  bool
	ExistingMap  map[string]string
	Provisioned  map[string]bool
	NeedSLOByKey map[string]int
	NeedSpotKey  map[string]int
}

type poolRuntime struct {
	NP               *containerv1beta1.NodePool
	Name             string
	PoolKey          string
	Limit            PoolLimit
	SpecHash         string
	Spot             bool
	Chips            int
	WantNodes        int
	ReadyNodes       int
	PlacedActivePods int
	HasFailedLabel   bool
	HealthyAndReady  bool
	Unhealthy        bool
	Stopping         bool
}

func (r *ReuseReconciler) Reconcile(ctx context.Context, _ ctrl.Request) (ctrl.Result, error) {
	r.reconcileMu.Lock()
	defer r.reconcileMu.Unlock()

	lg := ctrllog.FromContext(ctx)
	now := time.Now()

	// 1. Fetch cluster state: JobSets, Jobs, Pods, Nodes, PriorityClasses, and GKE NodePools.
	var jsList jobset.JobSetList
	if err := r.List(ctx, &jsList); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing JobSets: %w", err)
	}

	var activeReuseJS []*jobset.JobSet
	seenJSKeys := make(map[types.NamespacedName]bool, len(jsList.Items))
	for i := range jsList.Items {
		js := &jsList.Items[i]
		seenJSKeys[client.ObjectKeyFromObject(js)] = true
		js = r.ensureFreshJobSet(ctx, js)
		if js == nil || !js.DeletionTimestamp.IsZero() || isJobSetFinished(js) {
			continue
		}
		if js.Annotations != nil && js.Annotations[AnnotationReuseManaged] == "true" {
			activeReuseJS = append(activeReuseJS, js)
		}
	}
	r.pruneJobSetRV(seenJSKeys)

	rawPools, err := r.Backend.ListRawNodePools(ctx)
	if err != nil {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, fmt.Errorf("listing GKE nodepools: %w", err)
	}

	// Fast exit when reuse is disabled and zero reuse-managed JobSets or reusable GKE pools exist.
	if !r.EnableNodePoolReuse && len(activeReuseJS) == 0 && !hasAnyReusablePool(rawPools) {
		return ctrl.Result{}, nil
	}

	var nodeList corev1.NodeList
	if err := r.List(ctx, &nodeList); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing Nodes: %w", err)
	}
	var podList corev1.PodList
	if err := r.List(ctx, &podList); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing Pods: %w", err)
	}
	var jobList batchv1.JobList
	if err := r.List(ctx, &jobList); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing Jobs: %w", err)
	}
	prioMap := r.loadPriorityClasses(ctx)

	// Index Nodes and placed non-terminal Pods per nodepool.
	nodesByPool := make(map[string][]*corev1.Node)
	nodeToPool := make(map[string]string, len(nodeList.Items))
	for i := range nodeList.Items {
		n := &nodeList.Items[i]
		npName := n.Labels[cloud.GKENodePoolNameLabel]
		if npName != "" {
			nodesByPool[npName] = append(nodesByPool[npName], n)
			nodeToPool[n.Name] = npName
		}
	}

	placedActiveByPool := make(map[string][]*corev1.Pod)
	for i := range podList.Items {
		p := &podList.Items[i]
		if !isPlacedActiveTenantWorkloadPod(p) {
			continue
		}
		if npName := nodeToPool[p.Spec.NodeName]; npName != "" {
			placedActiveByPool[npName] = append(placedActiveByPool[npName], p)
		}
	}

	// Index current-attempt child Jobs per JobSet.
	jobsByJS := make(map[types.NamespacedName][]*batchv1.Job)
	needsReallocJS := make(map[types.NamespacedName]bool)
	for i := range jobList.Items {
		j := &jobList.Items[i]
		if !j.DeletionTimestamp.IsZero() {
			continue
		}
		jsName := ""
		if j.Labels != nil {
			jsName = j.Labels[jobset.JobSetNameKey]
		}
		if jsName == "" {
			continue
		}
		key := types.NamespacedName{Namespace: j.Namespace, Name: jsName}
		jobsByJS[key] = append(jobsByJS[key], j)
		if j.Annotations != nil && j.Annotations[AnnotationNeedsReallocation] == "true" {
			needsReallocJS[key] = true
		}
	}

	inFlightCreates, inFlightDeletes := r.snapshotInFlight()

	// 2. Inspect GKE NodePools: separate external (legacy/static) pools from dynamic-reusable pools.
	externalSLO := make(map[string]int)
	externalSpot := make(map[string]int)
	reusableByName := make(map[string]*poolRuntime)
	anyStoppingInGKE := false

	unhealthyGrace := r.UnhealthySLOGrace
	if unhealthyGrace <= 0 {
		unhealthyGrace = defaultUnhealthySLOGrace
	}

	seenReusableNames := make(map[string]bool)
	for _, np := range rawPools {
		if np == nil {
			continue
		}
		if np.Status == "STOPPING" {
			anyStoppingInGKE = true
		}
		var cell string
		if np.Config != nil && np.Config.Labels != nil {
			cell = np.Config.Labels[cloud.LocationHintLabel]
		}
		res := cloud.NodePoolReservation(np)
		limit := r.Capacity.Resolve(res, cell)
		poolKey := limit.Key
		k8sNodes := nodesByPool[np.Name]
		chips := cloud.NodePoolChips(np, len(k8sNodes))
		spot := np.Config != nil && np.Config.Spot

		if !cloud.IsReusableNodePool(np) {
			if np.Status != "STOPPING" && np.Status != "ERROR" {
				if spot {
					externalSpot[poolKey] += chips
				} else {
					externalSLO[poolKey] += chips
				}
			}
			continue
		}

		seenReusableNames[np.Name] = true
		wantNodes := int(np.InitialNodeCount)
		if wantNodes <= 0 {
			wantNodes = 1
		}
		readyCnt := 0
		hasFailedLabel := false
		for _, n := range k8sNodes {
			if n.Labels[labelNodeFailed] == "true" {
				hasFailedLabel = true
			}
			if isK8sNodeHealthyAndSchedulable(n) {
				readyCnt++
			}
		}

		_, delInFlight := inFlightDeletes[np.Name]
		stopping := np.Status == "STOPPING" || delInFlight
		healthyReady := !stopping &&
			(np.Status == "RUNNING" || np.Status == "") &&
			readyCnt >= wantNodes &&
			!hasFailedLabel

		unhealthy := false
		r.mu.Lock()
		if healthyReady {
			if _, ok := r.seenHealthyAt[np.Name]; !ok {
				r.seenHealthyAt[np.Name] = now
			}
			delete(r.degradedSince, np.Name)
		} else if !stopping {
			if np.Status == "ERROR" || np.Status == "RUNNING_WITH_ERROR" || hasFailedLabel {
				unhealthy = true
			} else if _, wasHealthy := r.seenHealthyAt[np.Name]; wasHealthy {
				if spot {
					// Preempted / degraded Spot pool is immediately marked unhealthy (never retained warm).
					unhealthy = true
				} else {
					since, ok := r.degradedSince[np.Name]
					if !ok {
						r.degradedSince[np.Name] = now
					} else if now.Sub(since) >= unhealthyGrace {
						unhealthy = true
					}
				}
			}
		}
		r.mu.Unlock()

		var specHash string
		if np.Config != nil && np.Config.Labels != nil {
			specHash = np.Config.Labels[cloud.LabelPoolSpecHash]
		}
		if specHash == "" {
			specHash, _ = cloud.ReusePoolSpecHash(np)
		}

		reusableByName[np.Name] = &poolRuntime{
			NP:               np,
			Name:             np.Name,
			PoolKey:          poolKey,
			Limit:            limit,
			SpecHash:         specHash,
			Spot:             spot,
			Chips:            chips,
			WantNodes:        wantNodes,
			ReadyNodes:       readyCnt,
			PlacedActivePods: len(placedActiveByPool[np.Name]),
			HasFailedLabel:   hasFailedLabel,
			HealthyAndReady:  healthyReady,
			Unhealthy:        unhealthy,
			Stopping:         stopping,
		}
	}

	// Clean up stale tracking entries for deleted pools.
	r.mu.Lock()
	for name := range r.seenHealthyAt {
		if !seenReusableNames[name] {
			delete(r.seenHealthyAt, name)
			delete(r.degradedSince, name)
			delete(r.releasingSince, name)
		}
	}
	r.mu.Unlock()

	requeueSoon := false
	setRequeue := func() { requeueSoon = true }

	// 3. Build gang plans and handle broken/preempted pool detachment on already-allocated JobSets.
	var gangs []*gangPlan
	allocatedPoolOwners := make(map[string]types.NamespacedName)

	for _, js := range activeReuseJS {
		plan, err := r.buildGangPlan(js, prioMap)
		if err != nil {
			lg.Error(err, "building gang plan for JobSet", "jobset", client.ObjectKeyFromObject(js))
			continue
		}
		if len(plan.Slices) == 0 {
			continue
		}

		// Check if any already-provisioned pool in ExistingMap vanished or turned unhealthy.
		brokenDetected := needsReallocJS[plan.Key]
		for sliceKey, poolName := range plan.ExistingMap {
			pr, exists := reusableByName[poolName]
			if plan.Provisioned[poolName] {
				if !exists || pr.Stopping || pr.Unhealthy || !pr.HealthyAndReady {
					// If JobSet is suspended OR needs reallocation OR has 0 placed active pods on that pool, detach broken pool.
					jsSuspended := js.Spec.Suspend != nil && *js.Spec.Suspend
					placedCnt := 0
					if exists {
						placedCnt = pr.PlacedActivePods
					}
					if jsSuspended || needsReallocJS[plan.Key] || placedCnt == 0 {
						lg.Info("Detaching broken/preempted pool from JobSet allocation",
							"jobset", plan.Key, "slice", sliceKey, "nodePool", poolName)
						delete(plan.ExistingMap, sliceKey)
						delete(plan.Provisioned, poolName)
						if exists {
							pr.Unhealthy = true
						}
						brokenDetected = true
					}
				}
			} else if exists && (pr.Stopping || pr.Unhealthy) {
				delete(plan.ExistingMap, sliceKey)
				brokenDetected = true
			}
		}

		if brokenDetected {
			if err := r.suspendAndResetBrokenChildren(ctx, js, jobsByJS[plan.Key], plan.ExistingMap, plan.Provisioned); err != nil {
				lg.Error(err, "re-suspending JobSet for broken pool reallocation", "jobset", plan.Key)
			}
			setRequeue()
		}

		for _, poolName := range plan.ExistingMap {
			allocatedPoolOwners[poolName] = plan.Key
		}
		gangs = append(gangs, plan)
	}

	// 4. Classify unallocated reusable pools into IdleWarm, Releasing, or Unhealthy/Drain targets.
	idleWarmByHash := make(map[string][]*poolRuntime)
	idleWarmAll := make(map[string]*poolRuntime)
	releasingPoolsByKey := make(map[string][]*poolRuntime)
	poolsToDelete := make(map[string]string) // poolName -> reason

	for _, pr := range reusableByName {
		if pr.Stopping {
			continue
		}
		if _, isAllocated := allocatedPoolOwners[pr.Name]; isAllocated {
			r.clearReleasingSince(pr.Name)
			continue
		}

		if pr.PlacedActivePods > 0 {
			since := r.markReleasingSince(pr.Name, now)
			if now.Sub(since) < defaultReleasingMaxTimeout {
				releasingPoolsByKey[pr.PoolKey] = append(releasingPoolsByKey[pr.PoolKey], pr)
				setRequeue()
				continue
			}
			// Timed out releasing -> treat as unhealthy if pods finally cleared, else keep waiting.
			continue
		}
		r.clearReleasingSince(pr.Name)

		if pr.Unhealthy {
			poolsToDelete[pr.Name] = "reusable node pool is unhealthy or preempted"
			continue
		}
		if !r.EnableNodePoolReuse {
			poolsToDelete[pr.Name] = "ENABLE_NODEPOOL_REUSE is false (draining idle reusable warm pool)"
			continue
		}
		if pr.HealthyAndReady {
			idleWarmByHash[pr.SpecHash] = append(idleWarmByHash[pr.SpecHash], pr)
			idleWarmAll[pr.Name] = pr
		} else {
			// Still warming up after CreateNodePool; wait for Ready nodes.
			setRequeue()
		}
	}

	for hash := range idleWarmByHash {
		sort.Slice(idleWarmByHash[hash], func(i, j int) bool {
			return idleWarmByHash[hash][i].Name < idleWarmByHash[hash][j].Name
		})
	}

	// 5. Sort gangs: AlreadyAllocated DESC -> Tier (SLO before Spot) -> Priority DESC -> CreationTimestamp ASC.
	sort.SliceStable(gangs, func(i, j int) bool {
		a, b := gangs[i], gangs[j]
		if a.HasExisting != b.HasExisting {
			return a.HasExisting
		}
		if a.IsSpot != b.IsSpot {
			return !a.IsSpot // SLO (false) before Spot (true)
		}
		if a.Priority != b.Priority {
			return a.Priority > b.Priority
		}
		if !a.JobSet.CreationTimestamp.Equal(&b.JobSet.CreationTimestamp) {
			return a.JobSet.CreationTimestamp.Before(&b.JobSet.CreationTimestamp)
		}
		return a.Key.String() < b.Key.String()
	})

	// 6. All-or-Nothing Gang Admission & 1:1 Pool Matching.
	committedSLO := make(map[string]int)
	committedSpot := make(map[string]int)

	var desiredCreates []pendingCreateReq
	var admittedGangs []*gangPlan

	for _, g := range gangs {
		if !g.HasExisting && !r.EnableNodePoolReuse {
			// Feature gate toggled off before this JobSet was allocated; skip new reuse allocations.
			continue
		}

		// Wait briefly if a pool in any target CapacityPool is currently Releasing terminating pods,
		// so back-to-back JobSets reuse the warm pool cleanly instead of cold-creating or queueing spuriously.
		if !g.HasExisting && hasReleasingPoolForGang(g, releasingPoolsByKey) {
			setRequeue()
			continue
		}

		if !canAdmitGang(g, committedSLO, committedSpot, externalSLO, externalSpot) {
			lg.V(2).Info("Holding suspended JobSet in reuse queue (insufficient configured capacity)", "jobset", g.Key)
			continue
		}

		// Gang is admitted! Reserve its chip footprint in committedSLO / committedSpot.
		for k, chips := range g.NeedSLOByKey {
			committedSLO[k] += chips
		}
		for k, chips := range g.NeedSpotKey {
			committedSpot[k] += chips
		}
		admittedGangs = append(admittedGangs, g)

		allocChanged := false
		provChanged := false

		for _, sl := range g.Slices {
			existingPool := g.ExistingMap[sl.SliceKey]
			if existingPool != "" {
				if pr, ok := reusableByName[existingPool]; ok && !pr.Stopping && !pr.Unhealthy {
					if pr.HealthyAndReady && !g.Provisioned[existingPool] {
						g.Provisioned[existingPool] = true
						provChanged = true
					}
					continue
				}
				if _, creating := inFlightCreates[existingPool]; creating {
					continue
				}
				// Cold pool was pre-named on a prior pass while waiting at the delete gate;
				// check if an IdleWarm pool of the same SpecHash became available in the meantime!
				if warm := popIdleWarm(idleWarmByHash, idleWarmAll, sl.Spec.SpecHash); warm != nil {
					g.ExistingMap[sl.SliceKey] = warm.Name
					g.Provisioned[warm.Name] = true
					allocChanged = true
					provChanged = true
					continue
				}
				npToCreate := cloneNodePoolWithName(sl.Spec.DesiredNodePool, existingPool)
				cpSpec := *sl.Spec
				cpSpec.DesiredNodePool = npToCreate
				desiredCreates = append(desiredCreates, pendingCreateReq{
					PoolName: existingPool,
					PoolKey:  sl.PoolKey,
					Limit:    sl.Limit,
					Spec:     &cpSpec,
					JobSet:   g.JobSet,
				})
				continue
			}

			// Try matching an IdleWarm pool first.
			if warm := popIdleWarm(idleWarmByHash, idleWarmAll, sl.Spec.SpecHash); warm != nil {
				g.ExistingMap[sl.SliceKey] = warm.Name
				g.Provisioned[warm.Name] = true
				allocChanged = true
				provChanged = true
				continue
			}

			// Otherwise pre-assign a unique tp-... pool name and queue a cold create.
			newName := cloud.GenerateReusablePoolName(sl.Spec.Accelerator, sl.Spec.Topology, sl.Spec.SpecHash)
			g.ExistingMap[sl.SliceKey] = newName
			allocChanged = true

			npToCreate := cloneNodePoolWithName(sl.Spec.DesiredNodePool, newName)
			cpSpec := *sl.Spec
			cpSpec.DesiredNodePool = npToCreate
			desiredCreates = append(desiredCreates, pendingCreateReq{
				PoolName: newName,
				PoolKey:  sl.PoolKey,
				Limit:    sl.Limit,
				Spec:     &cpSpec,
				JobSet:   g.JobSet,
			})
		}

		if allocChanged || provChanged {
			if err := r.patchJobSetAllocation(ctx, g.JobSet, g.ExistingMap, g.Provisioned); err != nil {
				lg.Error(err, "patching nodepool-allocation on JobSet", "jobset", g.Key)
				return ctrl.Result{RequeueAfter: time.Second}, err
			}
		}
	}

	// 7. Lazy Warm-Pool Eviction Selection (ONLY when cold creates would overflow configured capacity).
	evictions := selectLazyWarmEvictions(
		desiredCreates,
		reusableByName,
		idleWarmAll,
		inFlightCreates,
		externalSLO,
		externalSpot,
	)
	for poolName, why := range evictions {
		poolsToDelete[poolName] = why
	}

	// 8. Execute Deletions FIRST (verifying 0 placed active pods via direct APIReader).
	for poolName, why := range poolsToDelete {
		if _, already := inFlightDeletes[poolName]; already {
			continue
		}
		pr := reusableByName[poolName]
		empty, err := r.verifyZeroPlacedActivePodsDirect(ctx, poolName, nodesByPool[poolName])
		if err != nil {
			lg.Error(err, "verifying zero pods via APIReader before DeleteNodePool", "nodePool", poolName)
			setRequeue()
			continue
		}
		if !empty {
			lg.Info("Skipping DeleteNodePool because direct APIReader found active pod(s)", "nodePool", poolName)
			setRequeue()
			continue
		}
		r.cordonPoolNodes(ctx, nodesByPool[poolName])
		poolKey := ""
		if pr != nil {
			poolKey = pr.PoolKey
		}
		r.dispatchDelete(ctx, poolName, poolKey, why)
	}

	// 9. STRICT DELETE-BEFORE-CREATE GATE:
	// Zero CreateNodePool calls may start while ANY delete is scheduled/in-flight or ANY GKE pool is STOPPING.
	_, currentDeletes := r.snapshotInFlight()
	strictDeleteGateBlocked := len(poolsToDelete) > 0 || len(currentDeletes) > 0 || anyStoppingInGKE
	if strictDeleteGateBlocked && len(desiredCreates) > 0 {
		lg.Info("Holding CreateNodePool calls behind strict delete-before-create gate",
			"pendingCreates", len(desiredCreates),
			"scheduledDeletes", len(poolsToDelete),
			"inFlightDeletes", len(currentDeletes),
			"anyStoppingInGKE", anyStoppingInGKE)
		setRequeue()
	} else {
		for _, cr := range desiredCreates {
			if _, exists := reusableByName[cr.PoolName]; exists {
				continue
			}
			if _, creating := inFlightCreates[cr.PoolName]; creating {
				continue
			}
			if r.inCooldown(cr.PoolName, now) {
				setRequeue()
				continue
			}
			r.dispatchCreate(ctx, cr.Spec.DesiredNodePool, cr.PoolKey, cr.Spec.SpecHash, cr.Spec.Spot, cr.Spec.TotalChips, cr.JobSet)
		}
	}

	// Refresh state if SynchronousGKEOps ran inline during tests.
	if r.SynchronousGKEOps && (len(poolsToDelete) > 0 || len(desiredCreates) > 0) {
		setRequeue()
	}

	// 10. Gang Readiness Barrier & Atomic Job gke-nodepool Injection + Unsuspend.
	for _, g := range admittedGangs {
		jsSuspended := g.JobSet.Spec.Suspend != nil && *g.JobSet.Spec.Suspend
		if !jsSuspended {
			continue
		}
		ready, err := r.tryUnsuspendReadyGang(ctx, g, reusableByName, nodesByPool, jobsByJS[g.Key])
		if err != nil {
			lg.Error(err, "evaluating gang readiness barrier", "jobset", g.Key)
			setRequeue()
			continue
		}
		if !ready {
			setRequeue()
		}
	}

	_, remDeletes := r.snapshotInFlight()
	remCreates, _ := r.snapshotInFlight()
	if requeueSoon || len(remDeletes) > 0 || len(remCreates) > 0 || anyStoppingInGKE {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

func (r *ReuseReconciler) tryUnsuspendReadyGang(
	ctx context.Context,
	g *gangPlan,
	reusableByName map[string]*poolRuntime,
	nodesByPool map[string][]*corev1.Node,
	childJobs []*batchv1.Job,
) (bool, error) {
	if len(g.ExistingMap) < len(g.Slices) {
		return false, nil
	}

	// 1. Verify every allocated pool is RUNNING, has all K_i Ready Nodes, and has 0 foreign active pods via APIReader.
	for _, sl := range g.Slices {
		poolName := g.ExistingMap[sl.SliceKey]
		if poolName == "" {
			return false, nil
		}
		pr, exists := reusableByName[poolName]
		if !exists || !pr.HealthyAndReady || pr.Stopping || pr.Unhealthy {
			return false, nil
		}
		empty, err := r.verifyNoForeignActivePodsDirect(ctx, nodesByPool[poolName], g.JobSet.Namespace, g.JobSet.Name, g.JobSet.Status.Restarts)
		if err != nil || !empty {
			return false, err
		}
	}

	// 2. Verify all non-DependsOn dynamic TPU child Jobs exist for the current restart-attempt.
	targetRestart := strconv.Itoa(int(g.JobSet.Status.Restarts))
	currentJobsBySlice := make(map[string]*batchv1.Job)
	for _, j := range childJobs {
		if j.Labels[restartsLabel] != targetRestart {
			continue
		}
		if j.Spec.Template.Annotations == nil || j.Spec.Template.Annotations[AnnotationReuseManaged] != "true" {
			continue
		}
		if sk, ok := jobSliceKey(j); ok {
			currentJobsBySlice[sk] = j
		}
	}

	for i := range g.JobSet.Spec.ReplicatedJobs {
		rj := &g.JobSet.Spec.ReplicatedJobs[i]
		if !isDynamicTPUReplicatedJob(rj) || len(rj.DependsOn) > 0 {
			continue
		}
		for idx := 0; idx < int(rj.Replicas); idx++ {
			sk := SliceAllocationKey(rj.Name, idx)
			if _, exists := currentJobsBySlice[sk]; !exists {
				// JobSetController hasn't created all suspended child Jobs yet.
				return false, nil
			}
		}
	}

	// 3. Patch nodeSelector[cloud.google.com/gke-nodepool] = assignedPool onto every suspended child Job.
	needRecreate := false
	for sk, j := range currentJobsBySlice {
		wantPool := g.ExistingMap[sk]
		if wantPool == "" {
			return false, nil
		}
		curPool := ""
		if j.Spec.Template.Spec.NodeSelector != nil {
			curPool = j.Spec.Template.Spec.NodeSelector[cloud.GKENodePoolNameLabel]
		}
		// If a child Job already started (StartTime != nil) with a stale/sentinel pool, delete it
		// while JobSet is suspended so JobSetController recreates it cleanly with StartTime == nil.
		if j.Status.StartTime != nil && curPool != wantPool {
			bg := metav1.DeletePropagationBackground
			if err := r.Delete(ctx, j, &client.DeleteOptions{PropagationPolicy: &bg}); client.IgnoreNotFound(err) != nil {
				return false, err
			}
			needRecreate = true
			continue
		}

		_, hasJobKey := j.Spec.Template.Spec.NodeSelector[cloud.LegacyJobKeySelectorLabel]
		_, hasNeedsRealloc := j.Annotations[AnnotationNeedsReallocation]
		if curPool == wantPool && !hasJobKey && !hasNeedsRealloc {
			continue
		}

		orig := j.DeepCopy()
		if j.Spec.Template.Spec.NodeSelector == nil {
			j.Spec.Template.Spec.NodeSelector = make(map[string]string)
		}
		j.Spec.Template.Spec.NodeSelector[cloud.GKENodePoolNameLabel] = wantPool
		delete(j.Spec.Template.Spec.NodeSelector, cloud.LegacyJobKeySelectorLabel)
		if j.Annotations != nil {
			delete(j.Annotations, AnnotationNeedsReallocation)
		}
		if err := r.Patch(ctx, j, client.MergeFrom(orig)); err != nil {
			return false, fmt.Errorf("patching gke-nodepool selector onto Job %s/%s: %w", j.Namespace, j.Name, err)
		}
	}
	if needRecreate {
		return false, nil
	}

	// 4. Mark all pools provisioned and unsuspend the JobSet!
	for _, poolName := range g.ExistingMap {
		g.Provisioned[poolName] = true
	}
	origJS := g.JobSet.DeepCopy()
	if g.JobSet.Annotations == nil {
		g.JobSet.Annotations = make(map[string]string)
	}
	allocBytes, _ := json.Marshal(g.ExistingMap)
	g.JobSet.Annotations[AnnotationNodePoolAllocation] = string(allocBytes)
	g.JobSet.Annotations[AnnotationProvisionedPools] = formatProvisionedSet(g.Provisioned)
	unsuspend := false
	g.JobSet.Spec.Suspend = &unsuspend
	if err := r.Patch(ctx, g.JobSet, client.MergeFrom(origJS)); err != nil {
		return false, fmt.Errorf("unsuspending JobSet %s: %w", g.Key, err)
	}
	r.recordPatchedJobSetRV(g.JobSet)
	if r.Recorder != nil {
		r.Recorder.Eventf(g.JobSet, corev1.EventTypeNormal, "JobSetUnsuspended",
			"All %d allocated TPU nodepool(s) are Ready; unsuspended JobSet", len(g.Slices))
	}
	return true, nil
}

func (r *ReuseReconciler) suspendAndResetBrokenChildren(
	ctx context.Context,
	js *jobset.JobSet,
	childJobs []*batchv1.Job,
	alloc map[string]string,
	provisioned map[string]bool,
) error {
	orig := js.DeepCopy()
	if js.Annotations == nil {
		js.Annotations = make(map[string]string)
	}
	allocBytes, _ := json.Marshal(alloc)
	js.Annotations[AnnotationNodePoolAllocation] = string(allocBytes)
	js.Annotations[AnnotationProvisionedPools] = formatProvisionedSet(provisioned)
	suspend := true
	js.Spec.Suspend = &suspend
	if err := r.Patch(ctx, js, client.MergeFrom(orig)); err != nil {
		return err
	}
	r.recordPatchedJobSetRV(js)

	bg := metav1.DeletePropagationBackground
	for _, j := range childJobs {
		if !isJobActive(j) {
			continue
		}
		curSel := ""
		if j.Spec.Template.Spec.NodeSelector != nil {
			curSel = j.Spec.Template.Spec.NodeSelector[cloud.GKENodePoolNameLabel]
		}
		sk, ok := jobSliceKey(j)
		wantPool := ""
		if ok {
			wantPool = alloc[sk]
		}
		if curSel == PendingReallocationNodePool ||
			(j.Annotations != nil && j.Annotations[AnnotationNeedsReallocation] == "true") ||
			(j.Status.StartTime != nil && curSel != wantPool) {
			_ = r.Delete(ctx, j, &client.DeleteOptions{PropagationPolicy: &bg})
		}
	}
	return nil
}

func (r *ReuseReconciler) patchJobSetAllocation(
	ctx context.Context,
	js *jobset.JobSet,
	alloc map[string]string,
	provisioned map[string]bool,
) error {
	orig := js.DeepCopy()
	if js.Annotations == nil {
		js.Annotations = make(map[string]string)
	}
	allocBytes, err := json.Marshal(alloc)
	if err != nil {
		return err
	}
	js.Annotations[AnnotationNodePoolAllocation] = string(allocBytes)
	js.Annotations[AnnotationProvisionedPools] = formatProvisionedSet(provisioned)
	if err := r.Patch(ctx, js, client.MergeFrom(orig)); err != nil {
		return err
	}
	r.recordPatchedJobSetRV(js)
	return nil
}

func (r *ReuseReconciler) recordPatchedJobSetRV(js *jobset.JobSet) {
	if js == nil || js.ResourceVersion == "" {
		return
	}
	rv, err := strconv.ParseUint(js.ResourceVersion, 10, 64)
	if err != nil || rv == 0 {
		return
	}
	key := client.ObjectKeyFromObject(js)
	r.mu.Lock()
	if rv > r.minJobSetRV[key] {
		r.minJobSetRV[key] = rv
	}
	r.mu.Unlock()
}

func (r *ReuseReconciler) ensureFreshJobSet(ctx context.Context, cached *jobset.JobSet) *jobset.JobSet {
	if cached == nil {
		return nil
	}
	key := client.ObjectKeyFromObject(cached)
	r.mu.Lock()
	minRV, ok := r.minJobSetRV[key]
	r.mu.Unlock()
	if !ok {
		return cached
	}
	if curRV, err := strconv.ParseUint(cached.ResourceVersion, 10, 64); err == nil && curRV >= minRV {
		r.mu.Lock()
		if r.minJobSetRV[key] <= curRV {
			delete(r.minJobSetRV, key)
		}
		r.mu.Unlock()
		return cached
	}
	var fresh jobset.JobSet
	if err := r.APIReader.Get(ctx, key, &fresh); err != nil {
		if apierrors.IsNotFound(err) {
			r.mu.Lock()
			delete(r.minJobSetRV, key)
			r.mu.Unlock()
			return nil
		}
		return cached
	}
	return &fresh
}

func (r *ReuseReconciler) pruneJobSetRV(seen map[types.NamespacedName]bool) {
	r.mu.Lock()
	for k := range r.minJobSetRV {
		if !seen[k] {
			delete(r.minJobSetRV, k)
		}
	}
	r.mu.Unlock()
}

func (r *ReuseReconciler) buildGangPlan(js *jobset.JobSet, prioMap map[string]int32) (*gangPlan, error) {
	existing := ParseAllocationAnnotation(js)
	provisioned := parseProvisionedSet(js)
	plan := &gangPlan{
		JobSet:       js,
		Key:          client.ObjectKeyFromObject(js),
		ExistingMap:  existing,
		Provisioned:  provisioned,
		HasExisting:  len(existing) > 0,
		NeedSLOByKey: make(map[string]int),
		NeedSpotKey:  make(map[string]int),
		Priority:     math.MinInt32,
	}

	allSpot := true
	hasAny := false
	var highestPrio int32

	for i := range js.Spec.ReplicatedJobs {
		rj := &js.Spec.ReplicatedJobs[i]
		if !isDynamicTPUReplicatedJob(rj) {
			continue
		}
		podTemplate := &rj.Template.Spec.Template
		if pcName := podTemplate.Spec.PriorityClassName; pcName != "" {
			if p, ok := prioMap[pcName]; ok && (!hasAny || p > highestPrio) {
				highestPrio = p
			}
		} else if podTemplate.Spec.Priority != nil && (!hasAny || *podTemplate.Spec.Priority > highestPrio) {
			highestPrio = *podTemplate.Spec.Priority
		}

		synthPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        fmt.Sprintf("%s-%s-0", js.Name, rj.Name),
				Namespace:   js.Namespace,
				Labels:      cloneStringMap(podTemplate.Labels),
				Annotations: cloneStringMap(podTemplate.Annotations),
			},
			Spec: *podTemplate.Spec.DeepCopy(),
		}
		spec, err := r.Backend.BuildReusableNodePool(synthPod)
		if err != nil {
			return nil, fmt.Errorf("replicatedJob %s: %w", rj.Name, err)
		}
		limit := r.Capacity.Resolve(spec.Reservation, spec.Cell)
		if !spec.Spot {
			allSpot = false
		}
		hasAny = true

		for idx := 0; idx < int(rj.Replicas); idx++ {
			sk := SliceAllocationKey(rj.Name, idx)
			plan.Slices = append(plan.Slices, sliceRequirement{
				SliceKey: sk,
				PoolKey:  limit.Key,
				Limit:    limit,
				Spec:     spec,
			})
			if spec.Spot {
				plan.NeedSpotKey[limit.Key] += spec.TotalChips
			} else {
				plan.NeedSLOByKey[limit.Key] += spec.TotalChips
			}
		}
	}

	if hasAny {
		plan.IsSpot = allSpot
		plan.Priority = highestPrio
	}
	return plan, nil
}

func canAdmitGang(
	g *gangPlan,
	committedSLO map[string]int,
	committedSpot map[string]int,
	externalSLO map[string]int,
	externalSpot map[string]int,
) bool {
	limitsByKey := make(map[string]PoolLimit)
	for _, sl := range g.Slices {
		limitsByKey[sl.PoolKey] = sl.Limit
	}

	for poolKey, needSLO := range g.NeedSLOByKey {
		lim := limitsByKey[poolKey]
		if !lim.HasLimit {
			continue
		}
		// SLO admission intentionally ignores running Spot pools so Reserved creates proceed
		// and trigger GCE Spot preemption when live healthy chips are oversubscribed.
		if committedSLO[poolKey]+externalSLO[poolKey]+needSLO > lim.SLOCapacity {
			return false
		}
	}

	for poolKey, needSpot := range g.NeedSpotKey {
		lim := limitsByKey[poolKey]
		if !lim.HasLimit {
			continue
		}
		committedAll := committedSLO[poolKey] + committedSpot[poolKey] + g.NeedSLOByKey[poolKey]
		externalAll := externalSLO[poolKey] + externalSpot[poolKey]
		if committedAll+externalAll+needSpot > lim.RawCapacity {
			return false
		}
	}
	return true
}

type pendingCreateReq struct {
	PoolName string
	PoolKey  string
	Limit    PoolLimit
	Spec     *cloud.ReusablePoolSpec
	JobSet   *jobset.JobSet
}

type createSummary struct {
	Limit   PoolLimit
	NewSLO  int
	NewSpot int
}

func selectLazyWarmEvictions(
	desiredCreates []pendingCreateReq,
	reusableByName map[string]*poolRuntime,
	remainingIdleWarm map[string]*poolRuntime,
	inFlightCreates map[string]*inFlightCreate,
	externalSLO map[string]int,
	externalSpot map[string]int,
) map[string]string {
	evict := make(map[string]string)
	if len(desiredCreates) == 0 || len(remainingIdleWarm) == 0 {
		return evict
	}

	byKey := make(map[string]*createSummary)
	creatingNames := make(map[string]bool)
	for _, cr := range desiredCreates {
		if _, exists := reusableByName[cr.PoolName]; exists {
			continue
		}
		if _, inFlight := inFlightCreates[cr.PoolName]; inFlight {
			continue
		}
		creatingNames[cr.PoolName] = true
		s := byKey[cr.PoolKey]
		if s == nil {
			s = &createSummary{Limit: cr.Limit}
			byKey[cr.PoolKey] = s
		}
		if cr.Spec.Spot {
			s.NewSpot += cr.Spec.TotalChips
		} else {
			s.NewSLO += cr.Spec.TotalChips
		}
	}

	// Compute currently active chips per CapacityPool (excluding unclaimed IdleWarm and STOPPING pools).
	activeSLO := make(map[string]int)
	activeSpot := make(map[string]int)
	for k, v := range externalSLO {
		activeSLO[k] += v
	}
	for k, v := range externalSpot {
		activeSpot[k] += v
	}
	for name, pr := range reusableByName {
		if pr.Stopping || pr.Unhealthy {
			continue
		}
		if _, isIdle := remainingIdleWarm[name]; isIdle {
			continue
		}
		if pr.Spot {
			activeSpot[pr.PoolKey] += pr.Chips
		} else {
			activeSLO[pr.PoolKey] += pr.Chips
		}
	}
	for name, inf := range inFlightCreates {
		if _, exists := reusableByName[name]; exists || creatingNames[name] {
			continue
		}
		if inf.Spot {
			activeSpot[inf.PoolKey] += inf.TotalChips
		} else {
			activeSLO[inf.PoolKey] += inf.TotalChips
		}
	}

	// Group unclaimed IdleWarm pools by CapacityPool (smallest Chips first, then Name).
	warmSLOByKey := make(map[string][]*poolRuntime)
	warmSpotByKey := make(map[string][]*poolRuntime)
	for _, pr := range remainingIdleWarm {
		if pr.Spot {
			warmSpotByKey[pr.PoolKey] = append(warmSpotByKey[pr.PoolKey], pr)
		} else {
			warmSLOByKey[pr.PoolKey] = append(warmSLOByKey[pr.PoolKey], pr)
		}
	}
	sortPools := func(list []*poolRuntime) {
		sort.Slice(list, func(i, j int) bool {
			if list[i].Chips != list[j].Chips {
				return list[i].Chips < list[j].Chips
			}
			return list[i].Name < list[j].Name
		})
	}
	for k := range warmSLOByKey {
		sortPools(warmSLOByKey[k])
	}
	for k := range warmSpotByKey {
		sortPools(warmSpotByKey[k])
	}

	sumChips := func(list []*poolRuntime) int {
		tot := 0
		for _, p := range list {
			tot += p.Chips
		}
		return tot
	}

	for poolKey, summary := range byKey {
		if !summary.Limit.HasLimit {
			continue
		}
		wSpot := warmSpotByKey[poolKey]
		wSLO := warmSLOByKey[poolKey]
		actSLO := activeSLO[poolKey]
		actAll := actSLO + activeSpot[poolKey]

		if summary.NewSLO > 0 {
			// 1. Evict IdleWarm Spot pools if total footprint + NewSLO exceeds SLOCapacity.
			for len(wSpot) > 0 && actAll+sumChips(wSLO)+sumChips(wSpot)+summary.NewSLO > summary.Limit.SLOCapacity {
				victim := wSpot[0]
				wSpot = wSpot[1:]
				evict[victim.Name] = fmt.Sprintf("reclaiming %d warm Spot chips in %s for incoming SLO nodepool", victim.Chips, poolKey)
			}
			// 2. Evict IdleWarm SLO pools if SLO footprint + NewSLO exceeds SLOCapacity.
			for len(wSLO) > 0 && actSLO+sumChips(wSLO)+summary.NewSLO > summary.Limit.SLOCapacity {
				victim := wSLO[0]
				wSLO = wSLO[1:]
				evict[victim.Name] = fmt.Sprintf("reclaiming %d warm SLO chips in %s for incoming SLO nodepool", victim.Chips, poolKey)
			}
			actSLO += summary.NewSLO
			actAll += summary.NewSLO
		}

		if summary.NewSpot > 0 {
			for len(wSpot) > 0 && actAll+sumChips(wSLO)+sumChips(wSpot)+summary.NewSpot > summary.Limit.RawCapacity {
				victim := wSpot[0]
				wSpot = wSpot[1:]
				evict[victim.Name] = fmt.Sprintf("reclaiming %d warm Spot chips in %s for incoming Spot nodepool", victim.Chips, poolKey)
			}
			for len(wSLO) > 0 && actAll+sumChips(wSLO)+sumChips(wSpot)+summary.NewSpot > summary.Limit.RawCapacity {
				victim := wSLO[0]
				wSLO = wSLO[1:]
				evict[victim.Name] = fmt.Sprintf("reclaiming %d warm SLO chips in %s for incoming Spot nodepool", victim.Chips, poolKey)
			}
		}
	}

	return evict
}

func (r *ReuseReconciler) dispatchDelete(ctx context.Context, poolName, poolKey, why string) {
	r.mu.Lock()
	if _, exists := r.inFlightDeletes[poolName]; exists {
		r.mu.Unlock()
		return
	}
	r.inFlightDeletes[poolName] = &inFlightDelete{
		PoolName:  poolName,
		PoolKey:   poolKey,
		StartedAt: time.Now(),
	}
	r.mu.Unlock()

	run := func(opCtx context.Context) {
		r.gkeSem <- struct{}{}
		defer func() {
			<-r.gkeSem
			r.mu.Lock()
			delete(r.inFlightDeletes, poolName)
			r.mu.Unlock()
			r.triggerWakeup()
		}()
		_ = r.Backend.DeleteRawNodePool(opCtx, poolName, nil, why)
	}

	if r.SynchronousGKEOps {
		run(ctx)
	} else {
		go run(context.Background())
	}
}

func (r *ReuseReconciler) dispatchCreate(
	ctx context.Context,
	np *containerv1beta1.NodePool,
	poolKey string,
	specHash string,
	spot bool,
	totalChips int,
	js *jobset.JobSet,
) {
	poolName := np.Name
	jsKey := client.ObjectKeyFromObject(js)
	jsCopy := js.DeepCopy()

	r.mu.Lock()
	if _, exists := r.inFlightCreates[poolName]; exists {
		r.mu.Unlock()
		return
	}
	r.inFlightCreates[poolName] = &inFlightCreate{
		PoolName:   poolName,
		PoolKey:    poolKey,
		SpecHash:   specHash,
		Spot:       spot,
		TotalChips: totalChips,
		JobSetKey:  jsKey,
		StartedAt:  time.Now(),
	}
	r.mu.Unlock()

	why := fmt.Sprintf("provisioning reusable pool for JobSet %s", jsKey)
	run := func(opCtx context.Context) {
		r.gkeSem <- struct{}{}
		defer func() {
			<-r.gkeSem
			r.mu.Lock()
			delete(r.inFlightCreates, poolName)
			r.mu.Unlock()
			r.triggerWakeup()
		}()
		if err := r.Backend.CreateRawNodePool(opCtx, np, jsCopy, why); err != nil {
			r.mu.Lock()
			r.createCooldowns[poolName] = createCooldown{
				Until: time.Now().Add(defaultCreateBackoff),
				Err:   err.Error(),
			}
			r.mu.Unlock()
		} else {
			r.mu.Lock()
			delete(r.createCooldowns, poolName)
			r.mu.Unlock()
		}
	}

	if r.SynchronousGKEOps {
		run(ctx)
	} else {
		go run(context.Background())
	}
}

func (r *ReuseReconciler) verifyZeroPlacedActivePodsDirect(ctx context.Context, _ string, nodes []*corev1.Node) (bool, error) {
	for _, n := range nodes {
		var podList corev1.PodList
		if err := r.APIReader.List(ctx, &podList, &client.ListOptions{
			FieldSelector: fields.OneTermEqualSelector("spec.nodeName", n.Name),
		}); err != nil {
			return false, err
		}
		for i := range podList.Items {
			if isPlacedActiveTenantWorkloadPod(&podList.Items[i]) {
				return false, nil
			}
		}
	}
	return true, nil
}

func (r *ReuseReconciler) verifyNoForeignActivePodsDirect(
	ctx context.Context,
	nodes []*corev1.Node,
	wantNamespace string,
	wantJobSetName string,
	wantRestarts int32,
) (bool, error) {
	wantRestartStr := strconv.Itoa(int(wantRestarts))
	for _, n := range nodes {
		var podList corev1.PodList
		if err := r.APIReader.List(ctx, &podList, &client.ListOptions{
			FieldSelector: fields.OneTermEqualSelector("spec.nodeName", n.Name),
		}); err != nil {
			return false, err
		}
		for i := range podList.Items {
			p := &podList.Items[i]
			if !isPlacedActiveTenantWorkloadPod(p) {
				continue
			}
			if p.Namespace != wantNamespace ||
				p.Labels[jobset.JobSetNameKey] != wantJobSetName ||
				p.Labels[restartsLabel] != wantRestartStr {
				return false, nil
			}
		}
	}
	return true, nil
}

func (r *ReuseReconciler) cordonPoolNodes(ctx context.Context, nodes []*corev1.Node) {
	for _, n := range nodes {
		if n.Spec.Unschedulable || !n.DeletionTimestamp.IsZero() {
			continue
		}
		orig := n.DeepCopy()
		n.Spec.Unschedulable = true
		_ = r.Patch(ctx, n, client.MergeFrom(orig))
	}
}

func (r *ReuseReconciler) loadPriorityClasses(ctx context.Context) map[string]int32 {
	out := make(map[string]int32)
	var pcList schedulingv1.PriorityClassList
	if err := r.List(ctx, &pcList); err != nil {
		return out
	}
	for i := range pcList.Items {
		out[pcList.Items[i].Name] = pcList.Items[i].Value
	}
	return out
}

func (r *ReuseReconciler) snapshotInFlight() (map[string]*inFlightCreate, map[string]*inFlightDelete) {
	r.mu.Lock()
	defer r.mu.Unlock()
	creates := make(map[string]*inFlightCreate, len(r.inFlightCreates))
	for k, v := range r.inFlightCreates {
		cp := *v
		creates[k] = &cp
	}
	deletes := make(map[string]*inFlightDelete, len(r.inFlightDeletes))
	for k, v := range r.inFlightDeletes {
		cp := *v
		deletes[k] = &cp
	}
	return creates, deletes
}

func (r *ReuseReconciler) inCooldown(poolName string, now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	cd, ok := r.createCooldowns[poolName]
	if !ok {
		return false
	}
	if now.After(cd.Until) {
		delete(r.createCooldowns, poolName)
		return false
	}
	return true
}

func (r *ReuseReconciler) markReleasingSince(poolName string, now time.Time) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t, ok := r.releasingSince[poolName]; ok {
		return t
	}
	r.releasingSince[poolName] = now
	return now
}

func (r *ReuseReconciler) clearReleasingSince(poolName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.releasingSince, poolName)
}

func popIdleWarm(byHash map[string][]*poolRuntime, all map[string]*poolRuntime, specHash string) *poolRuntime {
	candidates := byHash[specHash]
	if len(candidates) == 0 {
		return nil
	}
	chosen := candidates[0]
	byHash[specHash] = candidates[1:]
	delete(all, chosen.Name)
	return chosen
}

func hasReleasingPoolForGang(g *gangPlan, releasingByKey map[string][]*poolRuntime) bool {
	for _, sl := range g.Slices {
		if len(releasingByKey[sl.PoolKey]) > 0 {
			return true
		}
	}
	return false
}

func hasAnyReusablePool(raw []*containerv1beta1.NodePool) bool {
	for _, np := range raw {
		if cloud.IsReusableNodePool(np) {
			return true
		}
	}
	return false
}

func isJobSetFinished(js *jobset.JobSet) bool {
	for _, c := range js.Status.Conditions {
		if (c.Type == string(jobset.JobSetCompleted) || c.Type == string(jobset.JobSetFailed)) &&
			c.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

func isJobActive(j *batchv1.Job) bool {
	for _, c := range j.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return false
		}
	}
	return true
}

func isPodTerminal(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed
}

// isPlacedActiveTenantWorkloadPod reports whether a Pod bound to a Node is an active
// tenant workload pod (excluding DaemonSet/system-daemon pods such as kube-proxy, kindnet,
// node-exporter, gke-metadata-server, or tpu-device-plugin).
func isPlacedActiveTenantWorkloadPod(p *corev1.Pod) bool {
	if p == nil || p.Spec.NodeName == "" || isPodTerminal(p) {
		return false
	}
	if p.Annotations != nil && p.Annotations["kubernetes.io/config.mirror"] != "" {
		return false
	}
	for _, ref := range p.OwnerReferences {
		if ref.Kind == "DaemonSet" {
			return false
		}
	}
	if p.Labels != nil && p.Labels[jobset.JobSetNameKey] != "" {
		return true
	}
	return podSpecTPURequest(&p.Spec) > 0
}

func isK8sNodeHealthyAndSchedulable(n *corev1.Node) bool {
	if n == nil || !n.DeletionTimestamp.IsZero() || n.Spec.Unschedulable {
		return false
	}
	if n.Labels[labelNodeFailed] == "true" {
		return false
	}
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func expectedNodeCountBySlice(js *jobset.JobSet) map[string]int {
	out := make(map[string]int)
	if js == nil {
		return out
	}
	for i := range js.Spec.ReplicatedJobs {
		rj := &js.Spec.ReplicatedJobs[i]
		if !isDynamicTPUReplicatedJob(rj) {
			continue
		}
		topo := rj.Template.Spec.Template.Spec.NodeSelector[cloud.GKETPUNodeSelector]
		nodes := topoNodeCountFallback(topo)
		for idx := 0; idx < int(rj.Replicas); idx++ {
			out[SliceAllocationKey(rj.Name, idx)] = nodes
		}
	}
	return out
}

func topoNodeCountFallback(topo string) int {
	parts := strings.Split(topo, "x")
	prod := 1
	for _, s := range parts {
		v, err := strconv.Atoi(s)
		if err != nil || v <= 0 {
			return 1
		}
		prod *= v
	}
	n := (prod + 3) / 4
	if n <= 0 {
		return 1
	}
	return n
}

func parseProvisionedSet(js *jobset.JobSet) map[string]bool {
	out := make(map[string]bool)
	if js == nil || js.Annotations == nil {
		return out
	}
	for _, item := range strings.Split(js.Annotations[AnnotationProvisionedPools], ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			out[item] = true
		}
	}
	return out
}

func formatProvisionedSet(m map[string]bool) string {
	keys := make([]string, 0, len(m))
	for k, ok := range m {
		if ok && k != "" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

func cloneNodePoolWithName(src *containerv1beta1.NodePool, name string) *containerv1beta1.NodePool {
	raw, _ := json.Marshal(src)
	var dst containerv1beta1.NodePool
	_ = json.Unmarshal(raw, &dst)
	dst.Name = name
	return &dst
}

func cloneStringMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	cp := make(map[string]string, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}

var _ = apierrors.IsNotFound
