// Copyright (c) 2026, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package informers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestRequirePodTerminationKeepsUnreadyAndTerminatingPods(t *testing.T) {
	unready := richDrainEligiblePod("workload", "unready", "node-a")
	unready.DeletionTimestamp = nil
	unready.Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionFalse,
		LastTransitionTime: metav1.NewTime(time.Now().Add(-time.Hour))}}
	terminating := richDrainEligiblePod("workload", "terminating", "node-a")
	terminating.DeletionTimestamp = new(metav1.NewTime(time.Now().Add(-time.Hour)))
	completed := richDrainEligiblePod("workload", "completed", "node-a")
	completed.Status.Phase = v1.PodSucceeded
	failed := completed.DeepCopy()
	failed.Status.Phase = v1.PodFailed
	pods := []*v1.Pod{unready, terminating, completed, failed}
	instance := &Informers{}
	require.Empty(t, instance.filterEvictablePods(pods, nil), "default behavior stays compatible")
	instance.SetRequirePodTermination(true)
	require.Equal(t, []*v1.Pod{unready, terminating}, instance.filterEvictablePods(pods, nil))
}

func TestRequirePodTerminationPreservesGracePeriodAndPDB(t *testing.T) {
	environment := &envtest.Environment{}
	cfg, err := environment.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, environment.Stop()) })
	client, err := kubernetes.NewForConfig(cfg)
	require.NoError(t, err)
	ctx := t.Context()
	instance, err := NewInformers(client, 0, new(5), false, false, "", nil)
	require.NoError(t, err)
	instance.SetRequirePodTermination(true)
	require.NoError(t, instance.Run(ctx))

	// envtest has no kubelet: the terminating pod remains Running after its deadline.
	// The second pod is healthy and its Eviction request is rejected by a PDB.
	pods := make([]*v1.Pod, 0, 3)
	for _, name := range []string{"terminating", "protected", "graceful"} {
		grace := int64(1)
		if name == "graceful" {
			grace = 120
		}
		pod, createErr := client.CoreV1().Pods("default").Create(ctx, &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"app": name}},
			Spec: v1.PodSpec{NodeName: "node-a", TerminationGracePeriodSeconds: &grace,
				Containers: []v1.Container{{Name: "worker", Image: "busybox"}}},
		}, metav1.CreateOptions{})
		require.NoError(t, createErr)
		pod.Status.Phase = v1.PodRunning
		pod.Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}}
		pod, err = client.CoreV1().Pods("default").UpdateStatus(ctx, pod, metav1.UpdateOptions{})
		require.NoError(t, err)
		pods = append(pods, pod)
	}
	pdb, err := client.PolicyV1().PodDisruptionBudgets("default").Create(ctx, &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "protected"},
		Spec: policyv1.PodDisruptionBudgetSpec{MinAvailable: new(intstr.FromInt32(1)),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "protected"}}},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	pdb.Status.ObservedGeneration = pdb.Generation
	pdb.Status.DisruptionsAllowed = 0
	_, err = client.PolicyV1().PodDisruptionBudgets("default").UpdateStatus(ctx, pdb, metav1.UpdateOptions{})
	require.NoError(t, err)
	err = instance.sendEvictionRequestForPod(ctx, "default", time.Second, pods[1])
	require.True(t, apierrors.IsTooManyRequests(err), "expected PDB rejection, got %v", err)
	require.NoError(t, instance.sendEvictionRequestForPod(ctx, "default", time.Second, pods[2]))
	pods[2], err = client.CoreV1().Pods("default").Get(ctx, "graceful", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(120), *pods[2].DeletionGracePeriodSeconds, "preserve the workload's grace period")
	require.NoError(t, instance.sendEvictionRequestForPod(ctx, "default", time.Second, pods[0]))
	terminating, err := client.CoreV1().Pods("default").Get(ctx, "terminating", metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, terminating.DeletionTimestamp)
	deadline := terminating.DeletionTimestamp.Add(2 * time.Second)
	require.Eventually(t, func() bool { return time.Now().After(deadline) }, 5*time.Second, 20*time.Millisecond)
	pods[0] = terminating
	require.False(t, instance.CheckIfObservedPodsAreEvictedInImmediateMode(ctx,
		[]string{"default"}, "node-a", time.Second, nil, pods))
	for _, pod := range pods {
		current, getErr := client.CoreV1().Pods("default").Get(ctx, pod.Name, metav1.GetOptions{})
		require.NoError(t, getErr, "a timeout must not force-delete %s", pod.Name)
		require.Equal(t, pod.DeletionTimestamp, current.DeletionTimestamp)
	}
	require.True(t, instance.CheckIfObservedPodsAreEvictedInImmediateMode(ctx,
		[]string{"default"}, "node-a", time.Second, nil, nil), "empty scope completes")
}
