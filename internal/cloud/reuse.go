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

package cloud

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	containerv1beta1 "google.golang.org/api/container/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	"sigs.k8s.io/controller-runtime/pkg/client"
	jobset "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

const (
	// LabelProvisionerMode marks GKE NodePools provisioned in reusable dynamic mode.
	LabelProvisionerMode           = "google.com/tpu-provisioner-mode"
	ProvisionerModeDynamicReusable = "dynamic-reusable"

	// LabelPoolSpecHash stores the topology-complete shape hash on reusable NodePools.
	LabelPoolSpecHash = "google.com/tpu-provisioner-pool-spec-hash"

	// LegacyJobKeySelectorLabel is injected by external admission_controller.py in legacy mode.
	LegacyJobKeySelectorLabel = "job-key"

	maxGKENodePoolNameLen = 40
)

var nonAlphaNumDash = regexp.MustCompile(`[^a-z0-9-]+`)

// ReusablePoolSpec describes a normalized, shape-hashed GKE NodePool template for dynamic reuse.
type ReusablePoolSpec struct {
	SpecHash        string
	Accelerator     string
	Topology        string
	NodeCount       int
	ChipsPerNode    int
	TotalChips      int
	Spot            bool
	Reservation     string
	Cell            string
	DesiredNodePool *containerv1beta1.NodePool
}

// ReusePoolBackend abstracts GKE NodePool operations needed by internal/reuse without
// modifying the existing Provider interface or gke.go.
type ReusePoolBackend interface {
	BuildReusableNodePool(synthPod *corev1.Pod) (*ReusablePoolSpec, error)
	ListRawNodePools(ctx context.Context) ([]*containerv1beta1.NodePool, error)
	CreateRawNodePool(ctx context.Context, np *containerv1beta1.NodePool, eventObj client.Object, why string) error
	DeleteRawNodePool(ctx context.Context, name string, eventObj client.Object, why string) error
	ForceOnDemand() bool
}

// NewReusePoolBackend wraps an existing cloud.Provider (either *GKE or *Mock) in a ReusePoolBackend.
func NewReusePoolBackend(p Provider) (ReusePoolBackend, error) {
	switch impl := p.(type) {
	case *GKE:
		return &ReuseGKEAdapter{GKE: impl}, nil
	case *Mock:
		return NewMockReuseBackend(), nil
	default:
		return nil, fmt.Errorf("unsupported cloud provider type %T for nodepool reuse", p)
	}
}

// ReuseGKEAdapter adapts *GKE to ReusePoolBackend using g.nodePoolForPod() and g.NodePools.
type ReuseGKEAdapter struct {
	GKE *GKE
}

var _ ReusePoolBackend = &ReuseGKEAdapter{}

func (a *ReuseGKEAdapter) ForceOnDemand() bool {
	return a.GKE.ClusterContext.ForceOnDemand
}

// BuildReusableNodePool delegates to g.nodePoolForPod() on a synthetic Pod copy and strips
// workload-specific identifiers so the resulting NodePool can be shared sequentially across JobSets.
func (a *ReuseGKEAdapter) BuildReusableNodePool(synthPod *corev1.Pod) (*ReusablePoolSpec, error) {
	if synthPod == nil {
		return nil, fmt.Errorf("synthetic pod is nil")
	}
	pCopy := synthPod.DeepCopy()
	if pCopy.Name == "" {
		pCopy.Name = "reuse-template"
	}
	if pCopy.Namespace == "" {
		pCopy.Namespace = "default"
	}
	if len(pCopy.OwnerReferences) == 0 || metav1.GetControllerOf(pCopy) == nil {
		isCtrl := true
		pCopy.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion: "batch/v1",
				Kind:       "Job",
				Name:       "reuse-job",
				UID:        types.UID("reuse-uid"),
				Controller: &isCtrl,
			},
		}
	}
	if pCopy.Labels == nil {
		pCopy.Labels = make(map[string]string)
	}
	if pCopy.Labels[jobset.JobSetNameKey] == "" {
		pCopy.Labels[jobset.JobSetNameKey] = "reuse"
	}
	if len(pCopy.Labels[jobset.JobKey]) < jobKeySuffixLength {
		pCopy.Labels[jobset.JobKey] = "00000"
	}
	if pCopy.Spec.NodeSelector != nil {
		delete(pCopy.Spec.NodeSelector, LegacyJobKeySelectorLabel)
		delete(pCopy.Spec.NodeSelector, GKENodePoolNameLabel)
	}

	np, err := a.GKE.nodePoolForPod(pCopy)
	if err != nil {
		return nil, err
	}

	if np.Config == nil {
		np.Config = &containerv1beta1.NodeConfig{}
	}
	if np.Config.Labels == nil {
		np.Config.Labels = make(map[string]string)
	}

	// Remove per-JobSet / per-Job labels so NodePoolGarbageCollector and DeletionReconciler
	// ignore this pool and sequential JobSets can match it cleanly.
	for _, k := range []string{
		LabelJobSetName,
		LabelJobSetNamespace,
		LabelParentKind,
		LabelParentName,
		LabelParentNamespace,
		LabelNodePoolHash,
		LegacyJobKeySelectorLabel,
	} {
		delete(np.Config.Labels, k)
	}
	np.Config.Labels[LabelProvisionerMode] = ProvisionerModeDynamicReusable

	specHash, err := ReusePoolSpecHash(np)
	if err != nil {
		return nil, fmt.Errorf("computing reusable pool spec hash: %w", err)
	}
	np.Config.Labels[LabelPoolSpecHash] = specHash
	np.Name = ""

	accel := pCopy.Spec.NodeSelector[GKEAcceleratorNodeSelector]
	topo := pCopy.Spec.NodeSelector[GKETPUNodeSelector]
	chipsPerNode := ChipsPerMachineType(np.Config.MachineType)
	nodeCount := int(np.InitialNodeCount)

	return &ReusablePoolSpec{
		SpecHash:        specHash,
		Accelerator:     accel,
		Topology:        topo,
		NodeCount:       nodeCount,
		ChipsPerNode:    chipsPerNode,
		TotalChips:      nodeCount * chipsPerNode,
		Spot:            np.Config.Spot,
		Reservation:     NodePoolReservation(np),
		Cell:            np.Config.Labels[LocationHintLabel],
		DesiredNodePool: np,
	}, nil
}

func (a *ReuseGKEAdapter) ListRawNodePools(ctx context.Context) ([]*containerv1beta1.NodePool, error) {
	resp, err := a.GKE.NodePools.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing raw node pools: %w", err)
	}
	return resp.NodePools, nil
}

func (a *ReuseGKEAdapter) CreateRawNodePool(ctx context.Context, np *containerv1beta1.NodePool, eventObj client.Object, why string) error {
	if np == nil || np.Name == "" {
		return fmt.Errorf("reusable node pool name must be set before CreateRawNodePool")
	}
	if _, inProgress := a.GKE.inProgressCreatesNPName.Load(np.Name); inProgress {
		return fmt.Errorf("creation ongoing for node pool name %s: %w", np.Name, ErrDuplicateRequest)
	}
	a.GKE.inProgressCreatesNPName.Store(np.Name, struct{}{})
	defer a.GKE.inProgressCreatesNPName.Delete(np.Name)

	req := &containerv1beta1.CreateNodePoolRequest{
		NodePool: np,
		Parent:   a.GKE.ClusterContext.ClusterName(),
	}

	if a.GKE.Recorder != nil && eventObj != nil {
		a.GKE.Recorder.Eventf(eventObj, corev1.EventTypeNormal, EventNodePoolCreationStarted,
			"Starting creation of reusable Node Pool %s (size = %d) because %s", np.Name, np.InitialNodeCount, why)
	}
	log.Info("creating reusable node pool", "nodePool", np.Name, "initialNodeCount", np.InitialNodeCount, "why", why)

	return a.GKE.NodePools.Create(ctx, req, OpCallbacks{
		ReqFailure: func(err error) {
			if a.GKE.Recorder != nil && eventObj != nil {
				a.GKE.Recorder.Eventf(eventObj, corev1.EventTypeWarning, EventNodePoolCreationFailed,
					"Request to create reusable Node Pool %s failed: %v.", np.Name, err)
			}
		},
		OpFailure: func(err error) {
			if a.GKE.Recorder != nil && eventObj != nil {
				a.GKE.Recorder.Eventf(eventObj, corev1.EventTypeWarning, EventNodePoolCreationFailed,
					"Operation to create reusable Node Pool %s failed: %v.", np.Name, err)
			}
		},
		Success: func() {
			if a.GKE.Recorder != nil && eventObj != nil {
				a.GKE.Recorder.Eventf(eventObj, corev1.EventTypeNormal, EventNodePoolCreationSucceeded,
					"Successfully created reusable Node Pool %s.", np.Name)
			}
		},
	})
}

func (a *ReuseGKEAdapter) DeleteRawNodePool(ctx context.Context, name string, eventObj client.Object, why string) error {
	if _, inProgress := a.GKE.inProgressDeletesNPName.Load(name); inProgress {
		return ErrDuplicateRequest
	}
	a.GKE.inProgressDeletesNPName.Store(name, struct{}{})
	defer a.GKE.inProgressDeletesNPName.Delete(name)

	if a.GKE.Recorder != nil && eventObj != nil {
		a.GKE.Recorder.Eventf(eventObj, corev1.EventTypeNormal, EventNodePoolDeletionStarted,
			"Starting deletion of reusable Node Pool %s because %s", name, why)
	}
	log.Info("deleting reusable node pool", "nodePool", name, "why", why)

	return a.GKE.NodePools.Delete(ctx, name, OpCallbacks{
		NotFound: func() {
			if a.GKE.Recorder != nil && eventObj != nil {
				a.GKE.Recorder.Eventf(eventObj, corev1.EventTypeNormal, EventNodePoolNotFound,
					"Node pool %s not found - ignoring deletion attempt.", name)
			}
		},
		ReqFailure: func(err error) {
			if a.GKE.Recorder != nil && eventObj != nil {
				a.GKE.Recorder.Eventf(eventObj, corev1.EventTypeWarning, EventNodePoolDeletionFailed,
					"Request to delete reusable Node Pool %s failed: %v.", name, err)
			}
		},
		OpFailure: func(err error) {
			if a.GKE.Recorder != nil && eventObj != nil {
				a.GKE.Recorder.Eventf(eventObj, corev1.EventTypeWarning, EventNodePoolDeletionFailed,
					"Operation to delete reusable Node Pool %s failed: %v.", name, err)
			}
		},
		Success: func() {
			if a.GKE.Recorder != nil && eventObj != nil {
				a.GKE.Recorder.Eventf(eventObj, corev1.EventTypeNormal, EventNodePoolDeletionSucceeded,
					"Successfully deleted reusable Node Pool %s.", name)
			}
		},
	})
}

// ReusePoolSpecHash computes a deterministic 12-char hex digest over all topology- and
// node-shape-defining fields of a reusable GKE NodePool.
func ReusePoolSpecHash(np *containerv1beta1.NodePool) (string, error) {
	if np == nil {
		return "", fmt.Errorf("nodePool is nil")
	}
	type reuseHashPayload struct {
		InitialNodeCount          int64                                 `json:"initialNodeCount"`
		Locations                 []string                              `json:"locations,omitempty"`
		PlacementPolicy           *containerv1beta1.PlacementPolicy     `json:"placementPolicy,omitempty"`
		MachineType               string                                `json:"machineType,omitempty"`
		Spot                      bool                                  `json:"spot,omitempty"`
		ReservationAffinity       *containerv1beta1.ReservationAffinity `json:"reservationAffinity,omitempty"`
		ServiceAccount            string                                `json:"serviceAccount,omitempty"`
		DiskType                  string                                `json:"diskType,omitempty"`
		BootDiskKmsKey            string                                `json:"bootDiskKmsKey,omitempty"`
		EnableConfidentialStorage bool                                  `json:"enableConfidentialStorage,omitempty"`
		SecondaryBootDisks        []*containerv1beta1.SecondaryBootDisk `json:"secondaryBootDisks,omitempty"`
		NetworkConfig             *containerv1beta1.NodeNetworkConfig   `json:"networkConfig,omitempty"`
		Labels                    map[string]string                     `json:"labels,omitempty"`
	}

	payload := reuseHashPayload{
		InitialNodeCount: np.InitialNodeCount,
		Locations:        np.Locations,
		PlacementPolicy:  np.PlacementPolicy,
		NetworkConfig:    np.NetworkConfig,
	}
	if np.Config != nil {
		payload.MachineType = np.Config.MachineType
		payload.Spot = np.Config.Spot
		payload.ReservationAffinity = np.Config.ReservationAffinity
		payload.ServiceAccount = np.Config.ServiceAccount
		payload.DiskType = np.Config.DiskType
		payload.BootDiskKmsKey = np.Config.BootDiskKmsKey
		payload.EnableConfidentialStorage = np.Config.EnableConfidentialStorage
		payload.SecondaryBootDisks = np.Config.SecondaryBootDisks
		if len(np.Config.Labels) > 0 {
			payload.Labels = make(map[string]string, len(np.Config.Labels))
			for k, v := range np.Config.Labels {
				if k == LabelPoolSpecHash || k == LabelNodePoolHash {
					continue
				}
				payload.Labels[k] = v
			}
		}
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])[:12], nil
}

// GenerateReusablePoolName returns a valid GKE NodePool name (<= 40 chars) encoding
// short accelerator family, topology, spec hash prefix, and 5-char random suffix.
func GenerateReusablePoolName(accel, topo, specHash string) string {
	shortAccel := shortenAccelerator(accel)
	cleanTopo := sanitizeDNSFragment(topo)
	if len(cleanTopo) > 9 {
		cleanTopo = cleanTopo[:9]
	}
	hashPrefix := sanitizeDNSFragment(specHash)
	if len(hashPrefix) > 6 {
		hashPrefix = hashPrefix[:6]
	}
	if hashPrefix == "" {
		hashPrefix = "000000"
	}
	suffix := rand.String(5)
	name := fmt.Sprintf("tp-%s-%s-%s-%s", shortAccel, cleanTopo, hashPrefix, suffix)
	if len(name) > maxGKENodePoolNameLen {
		// Trim middle prefix while preserving the unique random suffix.
		prefixMax := maxGKENodePoolNameLen - 1 - len(suffix)
		prefix := strings.TrimRight(name[:prefixMax], "-")
		name = fmt.Sprintf("%s-%s", prefix, suffix)
	}
	return name
}

func shortenAccelerator(accel string) string {
	switch accel {
	case V7xSliceAccelerator:
		return "7x"
	case V6eSliceAccelerator:
		return "6e"
	case V5pPodSliceAccelerator:
		return "5p"
	case V5ePodSliceAccelerator:
		return "5e"
	case V4PodSliceAccelerator:
		return "v4"
	default:
		s := sanitizeDNSFragment(accel)
		if len(s) > 4 {
			s = s[:4]
		}
		if s == "" {
			return "tpu"
		}
		return strings.Trim(s, "-")
	}
}

func sanitizeDNSFragment(s string) string {
	lower := strings.ToLower(strings.TrimSpace(s))
	cleaned := nonAlphaNumDash.ReplaceAllString(lower, "-")
	return strings.Trim(cleaned, "-")
}

// ChipsPerMachineType parses the "-<N>t" suffix of GKE TPU machine types (e.g. tpu7x-standard-4t -> 4).
func ChipsPerMachineType(machineType string) int {
	mt := strings.ToLower(strings.TrimSpace(machineType))
	if strings.HasSuffix(mt, "t") {
		idx := strings.LastIndex(mt, "-")
		if idx != -1 && idx+1 < len(mt)-1 {
			if n, err := strconv.Atoi(mt[idx+1 : len(mt)-1]); err == nil && n > 0 {
				return n
			}
		}
	}
	return 4
}

// NodePoolChips returns total TPU chips represented by a GKE NodePool (using max(InitialNodeCount, observedNodes)).
func NodePoolChips(np *containerv1beta1.NodePool, observedNodes int) int {
	if np == nil {
		return 0
	}
	nodes := int(np.InitialNodeCount)
	if observedNodes > nodes {
		nodes = observedNodes
	}
	if nodes <= 0 {
		return 0
	}
	var mt string
	if np.Config != nil {
		mt = np.Config.MachineType
	}
	return nodes * ChipsPerMachineType(mt)
}

// NodePoolReservation extracts the specific reservation string (if any) from a GKE NodePool.
func NodePoolReservation(np *containerv1beta1.NodePool) string {
	if np == nil || np.Config == nil || np.Config.ReservationAffinity == nil {
		return ""
	}
	ra := np.Config.ReservationAffinity
	if ra.ConsumeReservationType != "" && ra.ConsumeReservationType != "SPECIFIC_RESERVATION" {
		return ""
	}
	if len(ra.Values) == 0 {
		return ""
	}
	return strings.TrimSpace(ra.Values[0])
}

// IsReusableNodePool reports whether a GKE NodePool was provisioned in dynamic-reusable mode.
func IsReusableNodePool(np *containerv1beta1.NodePool) bool {
	return np != nil &&
		np.Config != nil &&
		np.Config.Labels != nil &&
		np.Config.Labels[LabelProvisionerMode] == ProvisionerModeDynamicReusable
}

// MockReuseBackend is an in-memory ReusePoolBackend for unit tests and PROVIDER=mock.
type MockReuseBackend struct {
	mu                   sync.Mutex
	ForceOD              bool
	KeepStoppingOnDelete bool
	Pools                map[string]*containerv1beta1.NodePool
	CreateHistory        []string
	DeleteHistory        []string
	OnCreate             func(ctx context.Context, np *containerv1beta1.NodePool) error
	OnDelete             func(ctx context.Context, name string) error
	adapter              *ReuseGKEAdapter
}

func NewMockReuseBackend() *MockReuseBackend {
	return &MockReuseBackend{
		Pools: make(map[string]*containerv1beta1.NodePool),
		adapter: &ReuseGKEAdapter{
			GKE: &GKE{
				ClusterContext: GKEContext{
					ProjectID:       "mock-project",
					ClusterLocation: "us-central1-a",
					Cluster:         "mock-cluster",
					NodeZone:        "us-central1-a",
					MaxPodsPerNode:  16,
				},
			},
		},
	}
}

func (m *MockReuseBackend) ForceOnDemand() bool { return m.ForceOD }

func (m *MockReuseBackend) BuildReusableNodePool(synthPod *corev1.Pod) (*ReusablePoolSpec, error) {
	m.adapter.GKE.ClusterContext.ForceOnDemand = m.ForceOD
	return m.adapter.BuildReusableNodePool(synthPod)
}

func (m *MockReuseBackend) ListRawNodePools(_ context.Context) ([]*containerv1beta1.NodePool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*containerv1beta1.NodePool, 0, len(m.Pools))
	for _, np := range m.Pools {
		raw, _ := json.Marshal(np)
		var cloned containerv1beta1.NodePool
		_ = json.Unmarshal(raw, &cloned)
		out = append(out, &cloned)
	}
	return out, nil
}

func (m *MockReuseBackend) CreateRawNodePool(ctx context.Context, np *containerv1beta1.NodePool, _ client.Object, _ string) error {
	if m.OnCreate != nil {
		if err := m.OnCreate(ctx, np); err != nil {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, _ := json.Marshal(np)
	var cloned containerv1beta1.NodePool
	_ = json.Unmarshal(raw, &cloned)
	if cloned.Status == "" {
		cloned.Status = "RUNNING"
	}
	m.Pools[cloned.Name] = &cloned
	m.CreateHistory = append(m.CreateHistory, cloned.Name)
	return nil
}

func (m *MockReuseBackend) DeleteRawNodePool(ctx context.Context, name string, _ client.Object, _ string) error {
	if m.OnDelete != nil {
		if err := m.OnDelete(ctx, name); err != nil {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DeleteHistory = append(m.DeleteHistory, name)
	if m.KeepStoppingOnDelete {
		if existing, ok := m.Pools[name]; ok && existing != nil {
			existing.Status = "STOPPING"
		}
		return nil
	}
	delete(m.Pools, name)
	return nil
}
