// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
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

package reconciler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/nvidia/nvsentinel/data-models/pkg/model"
	"github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/common"
	"github.com/nvidia/nvsentinel/node-drainer/pkg/config"
	"github.com/nvidia/nvsentinel/node-drainer/pkg/informers"
	"github.com/nvidia/nvsentinel/node-drainer/pkg/reconciler"
	sdkclient "github.com/nvidia/nvsentinel/store-client/pkg/client"
	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"
)

func TestStrictDrainRequiresLiveWorkloadAndHeartbeatChecks(t *testing.T) {
	const node, ns = "strict-node", "training"
	cfg := config.TomlConfig{
		RequirePodTermination: true,
		UserNamespaces:        []config.UserNamespace{{Name: "*", Mode: config.ModeAllowCompletion}},
	}
	s := setupConfiguredTest(t, cfg, false)
	ctx := s.ctx
	prior := &protos.HealthEvent{Id: "prior", NodeName: node, CheckName: "ecc", IsFatal: true}
	annotation, err := createHealthEventAnnotationsMap([]*protos.HealthEvent{prior})
	require.NoError(t, err)
	createNodeWithLabelsAndAnnotations(ctx, t, s.client, node,
		map[string]string{"dgxc.nvidia.com/nvsentinel-state": "quarantined"},
		map[string]string{common.QuarantineHealthEventAnnotationKey: annotation})
	createNamespace(ctx, t, s.client, ns)
	createPod(ctx, t, s.client, ns, "trainer", node, v1.PodRunning, nil, nil, nil)
	s.healthEventStore.healthEvents = []datastore.HealthEventWithStatus{{
		HealthEvent: prior, HealthEventStatus: datastore.HealthEventStatus{
			UserPodsEvictionStatus: datastore.OperationStatus{Status: datastore.StatusSucceeded},
		},
	}}
	require.Eventually(t, func() bool {
		pods, err := s.informersInstance.FindEvictablePodsInNamespaceAndNode(ctx, ns, node, nil)
		return err == nil && len(pods) == 1
	}, 5*time.Second, 20*time.Millisecond)
	check := func(t *testing.T, status model.Status, skip, wantSuccess bool) {
		t.Helper()
		event := createHealthEvent(healthEventOptions{
			nodeName: node, eventID: "next", nodeQuarantined: status,
			recommendedAction: protos.RecommendedAction_RESTART_BM,
		})
		event["healthevent"].(*protos.HealthEvent).DrainOverrides = &protos.BehaviourOverrides{Skip: skip}
		var written []string
		s.mockCollection.UpdateDocumentFunc = func(_ context.Context, filter, update any) (*sdkclient.UpdateResult, error) {
			fields := update.(map[string]any)["$set"].(map[string]any)
			if v, ok := fields["healtheventstatus.userpodsevictionstatus"]; ok {
				written = append(written, v.(map[string]any)["status"].(string))
			}
			return &sdkclient.UpdateResult{MatchedCount: 1, ModifiedCount: 1}, nil
		}
		err := s.reconciler.ProcessEventGeneric(ctx, event, s.mockCollection, s.healthEventStore, node)
		if wantSuccess {
			require.NoError(t, err)
			require.Equal(t, []string{"Succeeded"}, written)
		} else {
			require.Empty(t, written)
			require.Error(t, err)
		}
	}
	t.Run("prior drain", func(t *testing.T) { check(t, model.AlreadyQuarantined, false, false) })
	t.Run("skip override", func(t *testing.T) { check(t, model.Quarantined, true, false) })
	pods := s.client.CoreV1().Pods(ns)
	p, err := pods.Get(ctx, "trainer", metav1.GetOptions{})
	require.NoError(t, err)
	p.Status.Phase = v1.PodSucceeded
	_, err = pods.UpdateStatus(ctx, p, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		pods, err := s.informersInstance.FindEvictablePodsInNamespaceAndNode(ctx, ns, node, nil)
		return err == nil && len(pods) == 0
	}, 5*time.Second, 20*time.Millisecond)
	t.Run("missing heartbeat", func(t *testing.T) { check(t, model.Quarantined, false, false) })
	leases := s.client.CoordinationV1().Leases("kube-node-lease")
	old := metav1.NewMicroTime(time.Now().Add(-time.Hour))
	liveNode, err := s.client.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
	require.NoError(t, err)
	owner := metav1.OwnerReference{APIVersion: "v1", Kind: "Node", Name: node, UID: liveNode.UID}
	lease, err := leases.Create(ctx, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: node, OwnerReferences: []metav1.OwnerReference{owner}},
		Spec:       coordinationv1.LeaseSpec{HolderIdentity: new(node), LeaseDurationSeconds: new(int32(40)), RenewTime: &old},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	t.Run("expired heartbeat", func(t *testing.T) { check(t, model.Quarantined, false, false) })
	now := metav1.NowMicro()
	lease.Spec.RenewTime = &now
	lease.OwnerReferences[0].UID = "previous-node"
	_, err = leases.Update(ctx, lease, metav1.UpdateOptions{})
	require.NoError(t, err)
	t.Run("heartbeat for replaced node", func(t *testing.T) { check(t, model.Quarantined, false, false) })
	lease, err = leases.Get(ctx, node, metav1.GetOptions{})
	require.NoError(t, err)
	lease.OwnerReferences[0] = owner
	_, err = leases.Update(ctx, lease, metav1.UpdateOptions{})
	require.NoError(t, err)
	t.Run("completed workload", func(t *testing.T) { check(t, model.AlreadyQuarantined, true, true) })
	t.Run("namespace missing from cache", func(t *testing.T) {
		// Model an informer behind the apiserver using its initially empty indexes.
		cache, err := informers.NewInformers(s.client, 0, new(2), false, false, "", nil)
		require.NoError(t, err)
		cache.SetRequirePodTermination(true)
		current, err := reconciler.NewReconciler(s.reconciler.Config, false, s.client, cache, s.mockCollection, s.healthEventStore, nil, nil)
		require.NoError(t, err)
		t.Cleanup(current.Shutdown)
		s.reconciler = current
		createNamespace(ctx, t, s.client, "new-training")
		createPod(ctx, t, s.client, "new-training", "late-pod", node, v1.PodRunning, nil, nil, nil)
		check(t, model.Quarantined, false, false)
	})
}
