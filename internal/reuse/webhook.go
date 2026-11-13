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
	"net/http"
	"strconv"

	"github.com/GoogleCloudPlatform/ai-on-gke/tpu-provisioner/internal/cloud"
	"github.com/GoogleCloudPlatform/ai-on-gke/tpu-provisioner/internal/utils"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	jobset "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

const (
	// LabelNodePoolReuse allows per-JobSet opt-out when set to "false".
	LabelNodePoolReuse = "tpu-provisioner.cloud.google.com/nodepool-reuse"

	// AnnotationReuseManaged marks JobSets and dynamic TPU PodTemplates managed by internal/reuse.
	AnnotationReuseManaged = "tpu-provisioner.cloud.google.com/reuse-managed"

	// AnnotationNodePoolAllocation stores the JSON map {"<replicatedJob>-<jobIndex>": "<poolName>"} on JobSet.
	AnnotationNodePoolAllocation = "tpu-provisioner.cloud.google.com/nodepool-allocation"

	// AnnotationProvisionedPools stores comma-separated pool names observed RUNNING in GKE for the JobSet.
	AnnotationProvisionedPools = "tpu-provisioner.cloud.google.com/nodepool-provisioned"

	// AnnotationNeedsReallocation flags a recreated child Job when a gang pool is unhealthy on restart.
	AnnotationNeedsReallocation = "tpu-provisioner.cloud.google.com/needs-reallocation"

	// PendingReallocationNodePool holds recreated child Jobs Pending until ReuseReconciler swaps a broken pool.
	PendingReallocationNodePool = "pending-reallocation"
)

// ReuseJobSetMutator handles JobSet CREATE admission at /mutate-reuse-jobset.
type ReuseJobSetMutator struct {
	Decoder             admission.Decoder
	EnableNodePoolReuse bool
	ForceOnDemand       bool
}

func (h *ReuseJobSetMutator) Handle(_ context.Context, req admission.Request) admission.Response {
	if !h.EnableNodePoolReuse {
		return admission.Allowed("nodepool reuse disabled")
	}

	js := &jobset.JobSet{}
	if err := h.Decoder.Decode(req, js); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	if (js.Labels != nil && js.Labels[LabelNodePoolReuse] == "false") ||
		(js.Annotations != nil && js.Annotations[LabelNodePoolReuse] == "false") {
		return admission.Allowed("nodepool reuse opted out on JobSet")
	}
	if utils.SliceProvisioningEnabled(js) {
		return admission.Allowed("slice provisioning enabled")
	}
	if utils.AutoProvisioningDisabledForJobSet(js) {
		return admission.Allowed("autoprovisioning disabled by user")
	}

	var dynamicRJIndices []int
	for i := range js.Spec.ReplicatedJobs {
		if isDynamicTPUReplicatedJob(&js.Spec.ReplicatedJobs[i]) {
			dynamicRJIndices = append(dynamicRJIndices, i)
		}
	}
	if len(dynamicRJIndices) == 0 {
		return admission.Allowed("no dynamic TPU replicatedJobs")
	}

	if js.Annotations == nil {
		js.Annotations = make(map[string]string)
	}
	js.Annotations[AnnotationReuseManaged] = "true"
	delete(js.Annotations, jobset.ExclusiveKey)

	suspend := true
	js.Spec.Suspend = &suspend

	for _, idx := range dynamicRJIndices {
		rj := &js.Spec.ReplicatedJobs[idx]
		if rj.Template.Annotations != nil {
			delete(rj.Template.Annotations, jobset.ExclusiveKey)
		}
		if rj.Template.Spec.Template.Annotations == nil {
			rj.Template.Spec.Template.Annotations = make(map[string]string)
		}
		delete(rj.Template.Spec.Template.Annotations, jobset.ExclusiveKey)
		rj.Template.Spec.Template.Annotations[AnnotationReuseManaged] = "true"
		rj.Template.Spec.Template.Annotations[utils.DisableAutoProvisioningLabel] = "true"

		if h.ForceOnDemand && rj.Template.Spec.Template.Spec.NodeSelector != nil {
			delete(rj.Template.Spec.Template.Spec.NodeSelector, "cloud.google.com/reservation-name")
			delete(rj.Template.Spec.Template.Spec.NodeSelector, "cloud.google.com/reservation-project")
			delete(rj.Template.Spec.Template.Spec.NodeSelector, "cloud.google.com/gke-spot")
		}
	}

	marshaled, err := json.Marshal(js)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, marshaled)
}

// ReuseJobMutator handles batch/v1 Job CREATE admission at /mutate-reuse-job.
type ReuseJobMutator struct {
	Client  client.Client
	Decoder admission.Decoder
}

func (h *ReuseJobMutator) Handle(ctx context.Context, req admission.Request) admission.Response {
	job := &batchv1.Job{}
	if err := h.Decoder.Decode(req, job); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	if job.Spec.Template.Annotations == nil || job.Spec.Template.Annotations[AnnotationReuseManaged] != "true" {
		return admission.Allowed("job is not reuse-managed")
	}

	mutated := false
	if job.Spec.Template.Spec.NodeSelector != nil {
		if _, hasJobKey := job.Spec.Template.Spec.NodeSelector[cloud.LegacyJobKeySelectorLabel]; hasJobKey {
			delete(job.Spec.Template.Spec.NodeSelector, cloud.LegacyJobKeySelectorLabel)
			mutated = true
		}
	}
	if job.Annotations != nil {
		if _, hasExcl := job.Annotations[jobset.ExclusiveKey]; hasExcl {
			delete(job.Annotations, jobset.ExclusiveKey)
			mutated = true
		}
	}
	if _, hasExcl := job.Spec.Template.Annotations[jobset.ExclusiveKey]; hasExcl {
		delete(job.Spec.Template.Annotations, jobset.ExclusiveKey)
		mutated = true
	}

	ns := job.Namespace
	if ns == "" {
		ns = req.Namespace
	}
	jsName := ""
	if job.Labels != nil {
		jsName = job.Labels[jobset.JobSetNameKey]
	}
	if jsName == "" && job.Annotations != nil {
		jsName = job.Annotations[jobset.JobSetNameKey]
	}

	if h.Client != nil && jsName != "" && ns != "" {
		var js jobset.JobSet
		if err := h.Client.Get(ctx, types.NamespacedName{Namespace: ns, Name: jsName}, &js); err == nil {
			// When JobSet is already unsuspended (e.g. restart-attempt > 0 or DependsOn stage),
			// inject the allocated gke-nodepool synchronously or gate on pending-reallocation if unhealthy.
			isSuspended := js.Spec.Suspend != nil && *js.Spec.Suspend
			alloc := ParseAllocationAnnotation(&js)
			sliceKey, ok := jobSliceKey(job)
			if !isSuspended && ok && len(alloc) > 0 {
				if assignedPool, exists := alloc[sliceKey]; exists && assignedPool != "" {
					if job.Spec.Template.Spec.NodeSelector == nil {
						job.Spec.Template.Spec.NodeSelector = make(map[string]string)
					}
					if allAllocatedNodesReady(ctx, h.Client, &js, alloc) {
						job.Spec.Template.Spec.NodeSelector[cloud.GKENodePoolNameLabel] = assignedPool
						mutated = true
					} else {
						if job.Annotations == nil {
							job.Annotations = make(map[string]string)
						}
						job.Annotations[AnnotationNeedsReallocation] = "true"
						job.Spec.Template.Spec.NodeSelector[cloud.GKENodePoolNameLabel] = PendingReallocationNodePool
						mutated = true
					}
				}
			}
		}
	}

	if !mutated {
		return admission.Allowed("no job mutation needed")
	}

	marshaled, err := json.Marshal(job)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, marshaled)
}

func isDynamicTPUReplicatedJob(rj *jobset.ReplicatedJob) bool {
	if rj == nil {
		return false
	}
	podSpec := &rj.Template.Spec.Template.Spec
	if podSpec.NodeSelector == nil {
		return false
	}
	if podSpec.NodeSelector[cloud.GKENodePoolNameLabel] != "" {
		return false
	}
	if podSpec.NodeSelector[cloud.GKETPUNodeSelector] == "" ||
		podSpec.NodeSelector[cloud.GKEAcceleratorNodeSelector] == "" {
		return false
	}
	return podSpecTPURequest(podSpec) > 0
}

func podSpecTPURequest(podSpec *corev1.PodSpec) int64 {
	var total int64
	for _, c := range podSpec.Containers {
		if q, ok := c.Resources.Requests[corev1.ResourceName(cloud.GoogleTPUResource)]; ok {
			total += q.Value()
		}
	}
	var maxInit int64
	for _, ic := range podSpec.InitContainers {
		if q, ok := ic.Resources.Requests[corev1.ResourceName(cloud.GoogleTPUResource)]; ok {
			if q.Value() > maxInit {
				maxInit = q.Value()
			}
		}
	}
	if maxInit > total {
		total = maxInit
	}
	return total
}

func jobSliceKey(job *batchv1.Job) (string, bool) {
	if job == nil {
		return "", false
	}
	rjName := ""
	idxStr := ""
	if job.Labels != nil {
		rjName = job.Labels[jobset.ReplicatedJobNameKey]
		idxStr = job.Labels[jobset.JobIndexKey]
	}
	if rjName == "" && job.Annotations != nil {
		rjName = job.Annotations[jobset.ReplicatedJobNameKey]
	}
	if idxStr == "" && job.Annotations != nil {
		idxStr = job.Annotations[jobset.JobIndexKey]
	}
	if rjName == "" || idxStr == "" {
		return "", false
	}
	idx, err := strconv.Atoi(idxStr)
	if err != nil {
		return "", false
	}
	return SliceAllocationKey(rjName, idx), true
}

// SliceAllocationKey formats the canonical key "<replicatedJobName>-<jobIndex>" in nodepool-allocation.
func SliceAllocationKey(replicatedJobName string, jobIndex int) string {
	return fmt.Sprintf("%s-%d", replicatedJobName, jobIndex)
}

// ParseAllocationAnnotation decodes JobSet's nodepool-allocation JSON map.
func ParseAllocationAnnotation(js *jobset.JobSet) map[string]string {
	out := make(map[string]string)
	if js == nil || js.Annotations == nil {
		return out
	}
	raw := js.Annotations[AnnotationNodePoolAllocation]
	if raw == "" {
		return out
	}
	_ = json.Unmarshal([]byte(raw), &out)
	return out
}

func allAllocatedNodesReady(ctx context.Context, c client.Client, js *jobset.JobSet, alloc map[string]string) bool {
	expectedPerPool := expectedNodeCountBySlice(js)
	var nodeList corev1.NodeList
	if err := c.List(ctx, &nodeList); err != nil {
		return false
	}
	readyByPool := make(map[string]int)
	for i := range nodeList.Items {
		n := &nodeList.Items[i]
		pool := n.Labels[cloud.GKENodePoolNameLabel]
		if pool != "" && isK8sNodeHealthyAndSchedulable(n) {
			readyByPool[pool]++
		}
	}
	for sliceKey, poolName := range alloc {
		want := expectedPerPool[sliceKey]
		if want <= 0 {
			want = 1
		}
		if readyByPool[poolName] < want {
			return false
		}
	}
	return true
}
