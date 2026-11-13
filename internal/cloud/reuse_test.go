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
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func makeSynthTPUPod(accel, topo string, selectors map[string]string) *corev1.Pod {
	ns := map[string]string{
		GKEAcceleratorNodeSelector: accel,
		GKETPUNodeSelector:         topo,
	}
	for k, v := range selectors {
		ns[k] = v
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "worker-0",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			NodeSelector: ns,
			Containers: []corev1.Container{
				{
					Name: "jax",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							GoogleTPUResource: resource.MustParse("4"),
						},
					},
				},
			},
		},
	}
}

func TestBuildReusableNodePool_StripsWorkloadLabelsAndHashesTopology(t *testing.T) {
	backend := NewMockReuseBackend()

	pod2x2x2 := makeSynthTPUPod(V5pPodSliceAccelerator, "2x2x2", map[string]string{
		"cloud.google.com/reservation-name":  "res-alpha",
		"cloud.google.com/gke-location-hint": "cell-example-a",
		LegacyJobKeySelectorLabel:            "deadbeef",
		"cloud.google.com/gke-nodepool":      "should-be-ignored",
		"custom-team-selector":               "team-alpha",
	})
	specSmall, err := backend.BuildReusableNodePool(pod2x2x2)
	if err != nil {
		t.Fatalf("BuildReusableNodePool(2x2x2) error = %v", err)
	}

	if specSmall.TotalChips != 8 || specSmall.NodeCount != 2 || specSmall.ChipsPerNode != 4 {
		t.Fatalf("unexpected chip sizing: %+v", specSmall)
	}
	if specSmall.Reservation != "res-alpha" {
		t.Fatalf("Reservation = %q, want res-alpha", specSmall.Reservation)
	}
	if specSmall.Cell != "cell-example-a" {
		t.Fatalf("Cell = %q, want cell-example-a", specSmall.Cell)
	}

	labels := specSmall.DesiredNodePool.Config.Labels
	for _, forbidden := range []string{
		LabelJobSetName,
		LabelJobSetNamespace,
		LabelParentKind,
		LabelParentName,
		LabelParentNamespace,
		LabelNodePoolHash,
		LegacyJobKeySelectorLabel,
	} {
		if _, exists := labels[forbidden]; exists {
			t.Errorf("expected label %q to be stripped from reusable pool, got %q", forbidden, labels[forbidden])
		}
	}
	if labels[LabelProvisionerMode] != ProvisionerModeDynamicReusable {
		t.Errorf("mode label = %q, want %q", labels[LabelProvisionerMode], ProvisionerModeDynamicReusable)
	}
	if labels["custom-team-selector"] != "team-alpha" {
		t.Errorf("expected custom nodeSelector label to be preserved, got %v", labels)
	}

	// Same ct5p-hightpu-4t machine type, different topology (4x4x4) MUST yield a different SpecHash.
	pod4x4x4 := makeSynthTPUPod(V5pPodSliceAccelerator, "4x4x4", map[string]string{
		"cloud.google.com/reservation-name":  "res-alpha",
		"cloud.google.com/gke-location-hint": "cell-example-a",
		"custom-team-selector":               "team-alpha",
	})
	specLarge, err := backend.BuildReusableNodePool(pod4x4x4)
	if err != nil {
		t.Fatalf("BuildReusableNodePool(4x4x4) error = %v", err)
	}
	if specSmall.SpecHash == specLarge.SpecHash {
		t.Fatalf("expected 2x2x2 and 4x4x4 pools to have distinct SpecHashes, both got %q", specSmall.SpecHash)
	}

	// Spot vs Reserved MUST yield distinct SpecHashes.
	podSpot := makeSynthTPUPod(V5pPodSliceAccelerator, "2x2x2", map[string]string{
		"cloud.google.com/gke-spot":          "true",
		"cloud.google.com/gke-location-hint": "cell-example-a",
		"custom-team-selector":               "team-alpha",
	})
	specSpot, err := backend.BuildReusableNodePool(podSpot)
	if err != nil {
		t.Fatalf("BuildReusableNodePool(spot) error = %v", err)
	}
	if !specSpot.Spot || specSpot.SpecHash == specSmall.SpecHash {
		t.Fatalf("expected Spot SpecHash to differ from Reserved (%q vs %q)", specSpot.SpecHash, specSmall.SpecHash)
	}

	poolName := GenerateReusablePoolName(specSmall.Accelerator, specSmall.Topology, specSmall.SpecHash)
	if len(poolName) > maxGKENodePoolNameLen {
		t.Fatalf("poolName %q length %d exceeds %d", poolName, len(poolName), maxGKENodePoolNameLen)
	}
}
