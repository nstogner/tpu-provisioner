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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/ai-on-gke/tpu-provisioner/internal/cloud"
	"github.com/GoogleCloudPlatform/ai-on-gke/tpu-provisioner/internal/utils"
	admissionv1 "k8s.io/api/admission/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	jobset "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("clientgoscheme: %v", err)
	}
	if err := jobset.AddToScheme(s); err != nil {
		t.Fatalf("jobset scheme: %v", err)
	}
	if err := schedulingv1.AddToScheme(s); err != nil {
		t.Fatalf("scheduling scheme: %v", err)
	}
	return s
}

func newFakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	s := testScheme(t)
	return fake.NewClientBuilder().
		WithScheme(s).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
			return []string{o.(*corev1.Pod).Spec.NodeName}
		}).
		WithObjects(objs...).
		Build()
}

func makeTPUJobSet(name string, replicas int32, topo string, extraSelector map[string]string, createdAt time.Time) *jobset.JobSet {
	sel := map[string]string{
		cloud.GKEAcceleratorNodeSelector: cloud.V6eSliceAccelerator,
		cloud.GKETPUNodeSelector:         topo,
	}
	for k, v := range extraSelector {
		sel[k] = v
	}
	suspend := true
	return &jobset.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "default",
			CreationTimestamp: metav1.NewTime(createdAt),
			Annotations: map[string]string{
				AnnotationReuseManaged: "true",
			},
		},
		Spec: jobset.JobSetSpec{
			Suspend: &suspend,
			ReplicatedJobs: []jobset.ReplicatedJob{
				{
					Name:     "workers",
					Replicas: replicas,
					Template: batchv1.JobTemplateSpec{
						Spec: batchv1.JobSpec{
							Template: corev1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Annotations: map[string]string{
										AnnotationReuseManaged:             "true",
										utils.DisableAutoProvisioningLabel: "true",
									},
								},
								Spec: corev1.PodSpec{
									NodeSelector: sel,
									Containers: []corev1.Container{
										{
											Name:  "tpu-worker",
											Image: "busybox",
											Resources: corev1.ResourceRequirements{
												Requests: corev1.ResourceList{
													corev1.ResourceName(cloud.GoogleTPUResource): resource.MustParse("4"),
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func makeSuspendedChildJobs(js *jobset.JobSet) []*batchv1.Job {
	var out []*batchv1.Job
	suspend := js.Spec.Suspend != nil && *js.Spec.Suspend
	for _, rj := range js.Spec.ReplicatedJobs {
		for idx := 0; idx < int(rj.Replicas); idx++ {
			j := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      SliceAllocationKey(js.Name+"-"+rj.Name, idx),
					Namespace: js.Namespace,
					Labels: map[string]string{
						jobset.JobSetNameKey:        js.Name,
						jobset.ReplicatedJobNameKey: rj.Name,
						jobset.JobIndexKey:          string(rune('0' + idx)),
						restartsLabel:               "0",
						jobset.JobKey:               "abcde",
					},
				},
				Spec: batchv1.JobSpec{
					Suspend:  &suspend,
					Template: *rj.Template.Spec.Template.DeepCopy(),
				},
			}
			out = append(out, j)
		}
	}
	return out
}

func materializePoolNodes(ctx context.Context, t *testing.T, c client.Client, backend *cloud.MockReuseBackend, poolName string) []*corev1.Node {
	t.Helper()
	pools, err := backend.ListRawNodePools(ctx)
	if err != nil {
		t.Fatalf("ListRawNodePools: %v", err)
	}
	var count int
	for _, np := range pools {
		if np.Name == poolName {
			count = int(np.InitialNodeCount)
			break
		}
	}
	var created []*corev1.Node
	for i := 0; i < count; i++ {
		n := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: poolName + "-node-" + string(rune('0'+i)),
				Labels: map[string]string{
					cloud.GKENodePoolNameLabel: poolName,
					cloud.LabelNodepoolManager: cloud.LabelNodepoolManagerTPUPodinator,
				},
			},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				},
			},
		}
		if err := c.Create(ctx, n); err != nil {
			t.Fatalf("creating node %s: %v", n.Name, err)
		}
		// Simulate real-cluster DaemonSet pods (e.g. kube-proxy / kindnet) bound to every node
		// so all tests verify DaemonSet pods never block unsuspending or warm-pool eviction.
		dsPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "kube-proxy-" + n.Name,
				Namespace: "kube-system",
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "apps/v1",
						Kind:       "DaemonSet",
						Name:       "kube-proxy",
						UID:        "ds-uid-1",
					},
				},
			},
			Spec: corev1.PodSpec{
				NodeName:   n.Name,
				Containers: []corev1.Container{{Name: "kube-proxy", Image: "registry.k8s.io/kube-proxy:v1.30.0"}},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		if err := c.Create(ctx, dsPod); err != nil {
			t.Fatalf("creating daemonset pod %s: %v", dsPod.Name, err)
		}
		created = append(created, n)
	}
	return created
}

func mustCapacityBook(t *testing.T, cfg RawCapacityConfig) *CapacityBook {
	t.Helper()
	book, err := NewCapacityBook(cfg)
	if err != nil {
		t.Fatalf("NewCapacityBook: %v", err)
	}
	return book
}

func TestLoadCapacityConfigFile(t *testing.T) {
	// Empty path returns unconstrained CapacityBook.
	emptyBook, err := LoadCapacityConfigFile("")
	if err != nil {
		t.Fatalf("LoadCapacityConfigFile empty path: %v", err)
	}
	if emptyBook.Resolve("any-res", "").HasLimit {
		t.Fatalf("expected unconstrained book for empty path")
	}

	tmpDir := t.TempDir()
	yamlCfg := `
reservations:
  res-a:
    sloChips: 16
cells:
  cell-a:
    reservation: res-a
    rawChips: 24
  cell-spot-only:
    sloChips: 0
    rawChips: 32
`
	cfgPath := filepath.Join(tmpDir, "capacity.yaml")
	if err := os.WriteFile(cfgPath, []byte(yamlCfg), 0o600); err != nil {
		t.Fatalf("writing temp capacity.yaml: %v", err)
	}

	book, err := LoadCapacityConfigFile(cfgPath)
	if err != nil {
		t.Fatalf("LoadCapacityConfigFile: %v", err)
	}

	limRes := book.Resolve("projects/p1/reservations/res-a", "")
	limSubblock := book.Resolve("res-a/reservationBlocks/blk-0/reservationSubBlocks/sub-1", "")
	limCell := book.Resolve("", "cell-a")
	if limRes.Key != limCell.Key || limSubblock.Key != limRes.Key {
		t.Fatalf("expected unified key for res-a, subblock, and cell-a, got %q vs %q vs %q", limRes.Key, limSubblock.Key, limCell.Key)
	}
	if !limRes.HasLimit || limRes.SLOCapacity != 16 || limRes.RawCapacity != 24 {
		t.Fatalf("unexpected unified PoolLimit: %+v", limRes)
	}

	limSpot := book.Resolve("", "cell-spot-only")
	if !limSpot.HasLimit || limSpot.SLOCapacity != 0 || limSpot.RawCapacity != 32 {
		t.Fatalf("unexpected spot-only cell limit: %+v", limSpot)
	}

	limUnknown := book.Resolve("unlisted-res", "")
	if limUnknown.HasLimit {
		t.Fatalf("expected unlisted reservation to have HasLimit=false, got %+v", limUnknown)
	}

	if _, err := LoadCapacityConfigFile("../etc/passwd"); err == nil {
		t.Fatalf("expected path traversal rejection for ../etc/passwd")
	}

	badPath := filepath.Join(tmpDir, "bad.yaml")
	if err := os.WriteFile(badPath, []byte("reservations:\n  bad:\n    sloChips: 16\n    rawChips: 8\n"), 0o600); err != nil {
		t.Fatalf("writing bad.yaml: %v", err)
	}
	if _, err := LoadCapacityConfigFile(badPath); err == nil {
		t.Fatalf("expected error when rawChips < sloChips")
	}
}

func TestReuseWebhooks(t *testing.T) {
	ctx := context.Background()
	s := testScheme(t)
	dec := admission.NewDecoder(s)

	rawJS := &jobset.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "train-js",
			Namespace: "default",
			Annotations: map[string]string{
				jobset.ExclusiveKey: "cloud.google.com/gke-nodepool",
			},
		},
		Spec: jobset.JobSetSpec{
			ReplicatedJobs: []jobset.ReplicatedJob{
				{
					Name:     "coordinator",
					Replicas: 1,
					Template: batchv1.JobTemplateSpec{
						Spec: batchv1.JobSpec{
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{{Name: "cpu", Image: "busybox"}},
								},
							},
						},
					},
				},
				{
					Name:     "workers",
					Replicas: 1,
					Template: batchv1.JobTemplateSpec{
						Spec: batchv1.JobSpec{
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									NodeSelector: map[string]string{
										cloud.GKEAcceleratorNodeSelector: cloud.V6eSliceAccelerator,
										cloud.GKETPUNodeSelector:         "2x2",
									},
									Containers: []corev1.Container{{
										Name:  "tpu",
										Image: "busybox",
										Resources: corev1.ResourceRequirements{
											Requests: corev1.ResourceList{
												corev1.ResourceName(cloud.GoogleTPUResource): resource.MustParse("4"),
											},
										},
									}},
								},
							},
						},
					},
				},
			},
		},
	}
	jsBytes, _ := json.Marshal(rawJS)

	// 1. Disabled mode passes through with zero patches.
	offMut := &ReuseJobSetMutator{Decoder: dec, EnableNodePoolReuse: false}
	respOff := offMut.Handle(ctx, admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{Object: runtime.RawExtension{Raw: jsBytes}},
	})
	if !respOff.Allowed || len(respOff.Patches) != 0 {
		t.Fatalf("expected allowed with 0 patches when disabled, got allowed=%v patches=%d", respOff.Allowed, len(respOff.Patches))
	}

	// 2. Enabled mode sets Suspend=true, reuse-managed, disable-autoprovisioning on TPU rj, leaves CPU rj untouched.
	onMut := &ReuseJobSetMutator{Decoder: dec, EnableNodePoolReuse: true}
	respOn := onMut.Handle(ctx, admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{Object: runtime.RawExtension{Raw: jsBytes}},
	})
	if !respOn.Allowed || len(respOn.Patches) == 0 {
		t.Fatalf("expected mutation patches when enabled, got allowed=%v patches=%d", respOn.Allowed, len(respOn.Patches))
	}

	// 3. ReuseJobMutator strips job-key and handles healthy vs broken restart injection.
	unsuspended := false
	parentJS := makeTPUJobSet("train-js", 1, "2x2", nil, time.Now())
	parentJS.Spec.Suspend = &unsuspended
	parentJS.Annotations[AnnotationNodePoolAllocation] = `{"workers-0":"tp-6e-2x2-aaaaaa-11111"}`

	readyNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "n-0",
			Labels: map[string]string{cloud.GKENodePoolNameLabel: "tp-6e-2x2-aaaaaa-11111"},
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
	c := newFakeClient(t, parentJS, readyNode)
	jobMut := &ReuseJobMutator{Client: c, Decoder: dec}

	restartJob := makeSuspendedChildJobs(parentJS)[0]
	restartJob.Spec.Template.Spec.NodeSelector[cloud.LegacyJobKeySelectorLabel] = "legacy-hash"
	jobBytes, _ := json.Marshal(restartJob)

	respHealthy := jobMut.Handle(ctx, admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Namespace: "default",
			Object:    runtime.RawExtension{Raw: jobBytes},
		},
	})
	if !respHealthy.Allowed || len(respHealthy.Patches) == 0 {
		t.Fatalf("expected healthy restart job patches")
	}

	// Delete readyNode so allocated pool is broken -> expect pending-reallocation + needs-reallocation.
	_ = c.Delete(ctx, readyNode)
	respBroken := jobMut.Handle(ctx, admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Namespace: "default",
			Object:    runtime.RawExtension{Raw: jobBytes},
		},
	})
	if !respBroken.Allowed || len(respBroken.Patches) == 0 {
		t.Fatalf("expected broken restart job patches")
	}
	foundPending := false
	for _, p := range respBroken.Patches {
		if p.Value == PendingReallocationNodePool {
			foundPending = true
		}
	}
	if !foundPending {
		t.Fatalf("expected %q in patches, got %+v", PendingReallocationNodePool, respBroken.Patches)
	}
}

func TestReuseReconciler_SequentialWarmReuse(t *testing.T) {
	ctx := context.Background()
	backend := cloud.NewMockReuseBackend()
	capBook := mustCapacityBook(t, RawCapacityConfig{
		Reservations: map[string]ReservationLimit{"res-4": {SLOChips: 4}},
	})

	js1 := makeTPUJobSet("job-1", 1, "2x2", map[string]string{"cloud.google.com/reservation-name": "res-4"}, time.Now())
	jobs1 := makeSuspendedChildJobs(js1)
	c := newFakeClient(t, js1, jobs1[0])

	rec := NewReuseReconciler(c, c, testScheme(t), record.NewFakeRecorder(32), backend, capBook, true)
	rec.SynchronousGKEOps = true

	// Pass 1: Cold pool created; JobSet stays suspended until K8s Node registers Ready.
	if _, err := rec.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile 1: %v", err)
	}
	if len(backend.CreateHistory) != 1 {
		t.Fatalf("expected 1 CreateNodePool call, got %d", len(backend.CreateHistory))
	}
	poolName := backend.CreateHistory[0]

	var gotJS1 jobset.JobSet
	_ = c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "job-1"}, &gotJS1)
	if gotJS1.Spec.Suspend == nil || !*gotJS1.Spec.Suspend {
		t.Fatalf("expected job-1 to stay suspended before Node is Ready")
	}

	// Materialize Ready K8s node for poolName -> Pass 2 patches Job gke-nodepool and unsuspends JobSet!
	materializePoolNodes(ctx, t, c, backend, poolName)
	if _, err := rec.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile 2: %v", err)
	}

	_ = c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "job-1"}, &gotJS1)
	if gotJS1.Spec.Suspend == nil || *gotJS1.Spec.Suspend {
		t.Fatalf("expected job-1 to be unsuspended once pool is Ready")
	}
	var gotJob1 batchv1.Job
	_ = c.Get(ctx, client.ObjectKeyFromObject(jobs1[0]), &gotJob1)
	if got := gotJob1.Spec.Template.Spec.NodeSelector[cloud.GKENodePoolNameLabel]; got != poolName {
		t.Fatalf("expected Job nodeSelector gke-nodepool=%q, got %q", poolName, got)
	}

	// Mark job-1 Completed -> Pass 3 keeps pool IdleWarm (zero deletions!).
	gotJS1.Status.Conditions = []metav1.Condition{{
		Type:   string(jobset.JobSetCompleted),
		Status: metav1.ConditionTrue,
	}}
	_ = c.Update(ctx, &gotJS1)
	if _, err := rec.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile 3: %v", err)
	}
	if len(backend.DeleteHistory) != 0 {
		t.Fatalf("expected 0 DeleteNodePool calls (lazy retention), got %v", backend.DeleteHistory)
	}

	// Submit job-2 with identical shape -> Pass 4 claims warm pool immediately with 0 new creates!
	js2 := makeTPUJobSet("job-2", 1, "2x2", map[string]string{"cloud.google.com/reservation-name": "res-4"}, time.Now().Add(time.Second))
	jobs2 := makeSuspendedChildJobs(js2)
	_ = c.Create(ctx, js2)
	_ = c.Create(ctx, jobs2[0])

	if _, err := rec.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile 4: %v", err)
	}
	if len(backend.CreateHistory) != 1 {
		t.Fatalf("expected warm reuse with 0 extra CreateNodePool calls, got %v", backend.CreateHistory)
	}
	var gotJS2 jobset.JobSet
	_ = c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "job-2"}, &gotJS2)
	if gotJS2.Spec.Suspend == nil || *gotJS2.Spec.Suspend {
		t.Fatalf("expected job-2 to unsuspend immediately on IdleWarm hit")
	}
	var gotJob2 batchv1.Job
	_ = c.Get(ctx, client.ObjectKeyFromObject(jobs2[0]), &gotJob2)
	if got := gotJob2.Spec.Template.Spec.NodeSelector[cloud.GKENodePoolNameLabel]; got != poolName {
		t.Fatalf("expected job-2 child Job bound to warm pool %q, got %q", poolName, got)
	}
}

func TestReuseReconciler_MultiSliceGangContentionAndQueueing(t *testing.T) {
	ctx := context.Background()
	backend := cloud.NewMockReuseBackend()
	// res-8 has 8 SLO chips: enough for ONE 2-slice (2 * 4 = 8 chips) JobSet at a time.
	capBook := mustCapacityBook(t, RawCapacityConfig{
		Reservations: map[string]ReservationLimit{"res-8": {SLOChips: 8}},
	})

	t0 := time.Now()
	jsA := makeTPUJobSet("gang-a", 2, "2x2", map[string]string{"cloud.google.com/reservation-name": "res-8"}, t0)
	jsB := makeTPUJobSet("gang-b", 2, "2x2", map[string]string{"cloud.google.com/reservation-name": "res-8"}, t0.Add(time.Second))
	objs := []client.Object{jsA, jsB}
	for _, j := range append(makeSuspendedChildJobs(jsA), makeSuspendedChildJobs(jsB)...) {
		objs = append(objs, j)
	}
	c := newFakeClient(t, objs...)
	rec := NewReuseReconciler(c, c, testScheme(t), record.NewFakeRecorder(32), backend, capBook, true)
	rec.SynchronousGKEOps = true

	if _, err := rec.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile 1: %v", err)
	}
	if len(backend.CreateHistory) != 2 {
		t.Fatalf("expected exactly 2 pools created for gang-a, got %v", backend.CreateHistory)
	}

	// Verify gang-b has ZERO partial pool allocations while queued.
	var gotB jobset.JobSet
	_ = c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "gang-b"}, &gotB)
	if len(ParseAllocationAnnotation(&gotB)) != 0 {
		t.Fatalf("expected gang-b to hold 0 pools while queued, got %v", ParseAllocationAnnotation(&gotB))
	}

	for _, pool := range backend.CreateHistory {
		materializePoolNodes(ctx, t, c, backend, pool)
	}
	if _, err := rec.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile 2: %v", err)
	}

	var gotA jobset.JobSet
	_ = c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "gang-a"}, &gotA)
	if gotA.Spec.Suspend == nil || *gotA.Spec.Suspend {
		t.Fatalf("expected gang-a unsuspended")
	}
	_ = c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "gang-b"}, &gotB)
	if gotB.Spec.Suspend == nil || !*gotB.Spec.Suspend {
		t.Fatalf("expected gang-b still suspended while gang-a is active")
	}

	// Complete gang-a -> gang-b claims both warm pools without creating any new pools!
	gotA.Status.Conditions = []metav1.Condition{{Type: string(jobset.JobSetCompleted), Status: metav1.ConditionTrue}}
	_ = c.Update(ctx, &gotA)
	if _, err := rec.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile 3: %v", err)
	}
	if len(backend.CreateHistory) != 2 || len(backend.DeleteHistory) != 0 {
		t.Fatalf("expected 2 total creates and 0 deletes, got creates=%v deletes=%v", backend.CreateHistory, backend.DeleteHistory)
	}
	_ = c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "gang-b"}, &gotB)
	if gotB.Spec.Suspend == nil || *gotB.Spec.Suspend {
		t.Fatalf("expected gang-b unsuspended using gang-a's warm pools")
	}
}

func TestReuseReconciler_StrictDeleteBeforeCreate(t *testing.T) {
	ctx := context.Background()
	backend := cloud.NewMockReuseBackend()
	capBook := mustCapacityBook(t, RawCapacityConfig{
		Reservations: map[string]ReservationLimit{"res-8": {SLOChips: 8}},
	})

	// Seed an IdleWarm 2x4 (8-chip) pool in res-8.
	jsOld := makeTPUJobSet("old-2x4", 1, "2x4", map[string]string{"cloud.google.com/reservation-name": "res-8"}, time.Now())
	jobsOld := makeSuspendedChildJobs(jsOld)
	c := newFakeClient(t, jsOld, jobsOld[0])
	rec := NewReuseReconciler(c, c, testScheme(t), record.NewFakeRecorder(32), backend, capBook, true)
	rec.SynchronousGKEOps = true

	_, _ = rec.Reconcile(ctx, ctrl.Request{})
	oldPool := backend.CreateHistory[0]
	oldNodes := materializePoolNodes(ctx, t, c, backend, oldPool)
	_, _ = rec.Reconcile(ctx, ctrl.Request{})

	var gotOld jobset.JobSet
	_ = c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "old-2x4"}, &gotOld)
	gotOld.Status.Conditions = []metav1.Condition{{Type: string(jobset.JobSetCompleted), Status: metav1.ConditionTrue}}
	_ = c.Update(ctx, &gotOld)
	_, _ = rec.Reconcile(ctx, ctrl.Request{})

	// Configure MockReuseBackend so DeleteRawNodePool leaves oldPool in STOPPING state!
	backend.KeepStoppingOnDelete = true

	// Submit new JobSet requesting 4x2 (8 chips, incompatible topology hash -> requires eviction + cold create).
	jsNew := makeTPUJobSet("new-4x2", 1, "4x2", map[string]string{"cloud.google.com/reservation-name": "res-8"}, time.Now().Add(time.Minute))
	jobsNew := makeSuspendedChildJobs(jsNew)
	_ = c.Create(ctx, jsNew)
	_ = c.Create(ctx, jobsNew[0])

	// Pass 1 with new-4x2: triggers DeleteRawNodePool(oldPool) (putting it in STOPPING) and MUST hold CreateRawNodePool!
	if _, err := rec.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile during STOPPING start: %v", err)
	}
	if len(backend.CreateHistory) != 1 {
		t.Fatalf("strict delete-before-create violated: CreateHistory=%v while delete started", backend.CreateHistory)
	}

	// Pass 2 while oldPool is STILL STOPPING in GKE: CreateRawNodePool MUST remain blocked!
	if _, err := rec.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile while STOPPING: %v", err)
	}
	if len(backend.CreateHistory) != 1 {
		t.Fatalf("strict delete-before-create violated while pool STOPPING: CreateHistory=%v", backend.CreateHistory)
	}

	// Now finish GKE deletion of oldPool and its K8s nodes -> next Reconcile creates the 4x2 pool!
	delete(backend.Pools, oldPool)
	for _, n := range oldNodes {
		_ = c.Delete(ctx, n)
	}
	if _, err := rec.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile after STOPPING cleared: %v", err)
	}
	if len(backend.CreateHistory) != 2 {
		t.Fatalf("expected 4x2 pool creation after delete completed, got CreateHistory=%v", backend.CreateHistory)
	}
}

func TestReuseReconciler_SpotOversubscriptionAndReservedAdmission(t *testing.T) {
	ctx := context.Background()
	backend := cloud.NewMockReuseBackend()
	// Unified cell-a <-> res-a: sloChips=4, rawChips=8.
	capBook := mustCapacityBook(t, RawCapacityConfig{
		Reservations: map[string]ReservationLimit{"res-a": {SLOChips: 4}},
		Cells:        map[string]CellLimit{"cell-a": {Reservation: "res-a", RawChips: 8}},
	})

	t0 := time.Now()
	spot1 := makeTPUJobSet("spot-1", 1, "2x2", map[string]string{
		cloud.LocationHintLabel:     "cell-a",
		"cloud.google.com/gke-spot": "true",
	}, t0)
	spot2 := makeTPUJobSet("spot-2", 1, "2x2", map[string]string{
		cloud.LocationHintLabel:     "cell-a",
		"cloud.google.com/gke-spot": "true",
	}, t0.Add(time.Second))
	spot3 := makeTPUJobSet("spot-3", 1, "2x2", map[string]string{
		cloud.LocationHintLabel:     "cell-a",
		"cloud.google.com/gke-spot": "true",
	}, t0.Add(2*time.Second))

	objs := []client.Object{spot1, spot2, spot3}
	for _, j := range append(append(makeSuspendedChildJobs(spot1), makeSuspendedChildJobs(spot2)...), makeSuspendedChildJobs(spot3)...) {
		objs = append(objs, j)
	}
	c := newFakeClient(t, objs...)
	rec := NewReuseReconciler(c, c, testScheme(t), record.NewFakeRecorder(32), backend, capBook, true)
	rec.SynchronousGKEOps = true

	// 1. spot-1 and spot-2 (4 + 4 = 8 <= rawChips 8) are admitted; spot-3 queues!
	if _, err := rec.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile spot oversubscription: %v", err)
	}
	if len(backend.CreateHistory) != 2 {
		t.Fatalf("expected 2 Spot pools up to rawChips=8, got %v", backend.CreateHistory)
	}
	spotNodes0 := materializePoolNodes(ctx, t, c, backend, backend.CreateHistory[0])
	materializePoolNodes(ctx, t, c, backend, backend.CreateHistory[1])
	_, _ = rec.Reconcile(ctx, ctrl.Request{})

	// Place active pod on spot-1's node so spot-1 is ActiveRunning, and complete spot-2 so its pool becomes IdleWarm Spot.
	activePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spot-1-pod",
			Namespace: "default",
			Labels:    map[string]string{jobset.JobSetNameKey: "spot-1", restartsLabel: "0"},
		},
		Spec:   corev1.PodSpec{NodeName: spotNodes0[0].Name},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	_ = c.Create(ctx, activePod)

	var gotSpot2 jobset.JobSet
	_ = c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "spot-2"}, &gotSpot2)
	gotSpot2.Status.Conditions = []metav1.Condition{{Type: string(jobset.JobSetCompleted), Status: metav1.ConditionTrue}}
	_ = c.Update(ctx, &gotSpot2)

	// Also remove spot-3 so spot-2's pool sits IdleWarm, then submit Reserved SLO JobSet (4 chips in res-a).
	_ = c.Delete(ctx, spot3)
	_, _ = rec.Reconcile(ctx, ctrl.Request{})

	sloJS := makeTPUJobSet("slo-1", 1, "2x2", map[string]string{
		"cloud.google.com/reservation-name": "res-a",
	}, t0.Add(10*time.Second))
	sloJobs := makeSuspendedChildJobs(sloJS)
	_ = c.Create(ctx, sloJS)
	_ = c.Create(ctx, sloJobs[0])

	// Pass 1 for slo-1:
	// - SLO admission ignores running spot-1 (0 + 4 <= sloChips 4) -> ADMITTED!
	// - Lazy eviction sees activeSpot(4) + warmSpot(4) + newSLO(4) = 12 > sloChips(4) -> evicts IdleWarm Spot pool first!
	if _, err := rec.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile slo-1 eviction pass: %v", err)
	}
	if len(backend.DeleteHistory) != 1 || backend.DeleteHistory[0] != backend.CreateHistory[1] {
		t.Fatalf("expected IdleWarm Spot pool %s evicted before SLO create, got DeleteHistory=%v", backend.CreateHistory[1], backend.DeleteHistory)
	}

	// Pass 2 after IdleWarm Spot delete finished: SLO pool is created while running spot-1 is untouched (GCE Spot preemption handles spot-1)!
	if _, err := rec.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile slo-1 create pass: %v", err)
	}
	if len(backend.CreateHistory) != 3 {
		t.Fatalf("expected SLO pool created alongside running Spot pool, got CreateHistory=%v", backend.CreateHistory)
	}

	// Simulate GCE preempting spot-1 (pod terminates and K8s node disappears) -> preempted Spot pool is deleted, never kept warm!
	_ = c.Delete(ctx, activePod)
	_ = c.Delete(ctx, spotNodes0[0])
	var gotSpot1 jobset.JobSet
	_ = c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "spot-1"}, &gotSpot1)
	gotSpot1.Status.Conditions = []metav1.Condition{{Type: string(jobset.JobSetFailed), Status: metav1.ConditionTrue}}
	_ = c.Update(ctx, &gotSpot1)

	if _, err := rec.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile preempted Spot cleanup: %v", err)
	}
	if len(backend.DeleteHistory) != 2 || backend.DeleteHistory[1] != backend.CreateHistory[0] {
		t.Fatalf("expected preempted Spot pool %s deleted immediately, got DeleteHistory=%v", backend.CreateHistory[0], backend.DeleteHistory)
	}
}

func TestReuseReconciler_DrainModeOnDisable(t *testing.T) {
	ctx := context.Background()
	backend := cloud.NewMockReuseBackend()
	capBook := mustCapacityBook(t, RawCapacityConfig{
		Reservations: map[string]ReservationLimit{"res-8": {SLOChips: 8}},
	})

	jsActive := makeTPUJobSet("active-js", 1, "2x2", map[string]string{"cloud.google.com/reservation-name": "res-8"}, time.Now())
	jsIdle := makeTPUJobSet("idle-js", 1, "2x2", map[string]string{"cloud.google.com/reservation-name": "res-8"}, time.Now().Add(time.Second))
	objs := []client.Object{jsActive, jsIdle}
	for _, j := range append(makeSuspendedChildJobs(jsActive), makeSuspendedChildJobs(jsIdle)...) {
		objs = append(objs, j)
	}
	c := newFakeClient(t, objs...)
	rec := NewReuseReconciler(c, c, testScheme(t), record.NewFakeRecorder(32), backend, capBook, true)
	rec.SynchronousGKEOps = true

	_, _ = rec.Reconcile(ctx, ctrl.Request{})
	for _, p := range backend.CreateHistory {
		materializePoolNodes(ctx, t, c, backend, p)
	}
	_, _ = rec.Reconcile(ctx, ctrl.Request{})

	// Complete idle-js so its pool is IdleWarm, while active-js remains running.
	var gotIdle jobset.JobSet
	_ = c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "idle-js"}, &gotIdle)
	idlePool := ParseAllocationAnnotation(&gotIdle)["workers-0"]
	gotIdle.Status.Conditions = []metav1.Condition{{Type: string(jobset.JobSetCompleted), Status: metav1.ConditionTrue}}
	_ = c.Update(ctx, &gotIdle)

	// Toggle EnableNodePoolReuse = false (Drain Mode): idlePool is deleted while active-js's pool stays untouched!
	rec.EnableNodePoolReuse = false
	if _, err := rec.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile drain mode: %v", err)
	}
	if len(backend.DeleteHistory) != 1 || backend.DeleteHistory[0] != idlePool {
		t.Fatalf("expected only idlePool %s drained when disabled, got DeleteHistory=%v", idlePool, backend.DeleteHistory)
	}
}

type staleListClient struct {
	client.Client
	staleJS *jobset.JobSet
}

func (s *staleListClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if jsList, ok := list.(*jobset.JobSetList); ok && s.staleJS != nil {
		jsList.Items = []jobset.JobSet{*s.staleJS.DeepCopy()}
		return nil
	}
	return s.Client.List(ctx, list, opts...)
}

func TestReuseReconciler_StaleInformerDoesNotDoubleCreate(t *testing.T) {
	ctx := context.Background()
	backend := cloud.NewMockReuseBackend()
	capBook := mustCapacityBook(t, RawCapacityConfig{
		Reservations: map[string]ReservationLimit{"res-8": {SLOChips: 8}},
	})

	js := makeTPUJobSet("race-js", 1, "2x2", map[string]string{"cloud.google.com/reservation-name": "res-8"}, time.Now())
	jobs := makeSuspendedChildJobs(js)
	authClient := newFakeClient(t, js, jobs[0])

	// Capture pre-patch stale snapshot of js (ResourceVersion before allocation patch).
	var initialJS jobset.JobSet
	_ = authClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "race-js"}, &initialJS)

	lagging := &staleListClient{Client: authClient}
	rec := NewReuseReconciler(lagging, authClient, testScheme(t), record.NewFakeRecorder(32), backend, capBook, true)
	rec.SynchronousGKEOps = true

	// Pass 1: patches allocation onto authClient and creates 1 pool (still warming, 0 Ready K8s nodes).
	if _, err := rec.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile 1: %v", err)
	}
	if len(backend.CreateHistory) != 1 {
		t.Fatalf("expected 1 pool created on Pass 1, got %v", backend.CreateHistory)
	}

	// Pass 2: simulate informer cache still returning initialJS (pre-allocation ResourceVersion)!
	lagging.staleJS = &initialJS
	if _, err := rec.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile 2 with stale informer: %v", err)
	}
	if len(backend.CreateHistory) != 1 {
		t.Fatalf("expected 0 duplicate CreateNodePool calls under informer lag, got CreateHistory=%v", backend.CreateHistory)
	}
}
