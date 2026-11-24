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

package validation

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/common"
	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/config"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestExternalReceiptSessionIdentity(t *testing.T) {
	proof := externalReceipt{"request", "node", "session", "old", "new"}
	obj := &unstructured.Unstructured{}
	obj.SetName("maintenance")
	obj.SetUID("request")
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "gpu", UID: "node", Annotations: map[string]string{
		rebooterAnnotation + "session": "session", rebooterAnnotation + "source-boot-id": "old", rebooterAnnotation + "verified-boot-id": "new",
		rebooterAnnotation + "phase": "returning", rebooterAnnotation + "reboot-at": "2026-01-01T01:00:00Z",
		rebooterAnnotation + "nvsentinel-requests": `[{"Name":"maintenance","Node":"gpu","UID":"request"}]`,
	}}, Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{BootID: "new"}, Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
	require.NoError(t, proof.checkSession(node, obj))
	for name, edit := range map[string]func(*corev1.Node, *unstructured.Unstructured){
		"recreated request": func(_ *corev1.Node, r *unstructured.Unstructured) { r.SetUID("replacement") },
		"recreated node":    func(n *corev1.Node, _ *unstructured.Unstructured) { n.UID = "replacement" },
		"not attached": func(n *corev1.Node, _ *unstructured.Unstructured) {
			n.Annotations[rebooterAnnotation+"nvsentinel-requests"] = "[]"
		},
		"replaced session": func(n *corev1.Node, _ *unstructured.Unstructured) {
			n.Annotations[rebooterAnnotation+"session"] = "second"
		},
		"old boot": func(n *corev1.Node, _ *unstructured.Unstructured) { n.Status.NodeInfo.BootID = "old" },
		"missing source": func(n *corev1.Node, _ *unstructured.Unstructured) {
			delete(n.Annotations, rebooterAnnotation+"source-boot-id")
		},
		"not ready": func(n *corev1.Node, _ *unstructured.Unstructured) {
			n.Status.Conditions[0].Status = corev1.ConditionUnknown
		},
		"hardware support": func(n *corev1.Node, _ *unstructured.Unstructured) {
			n.Spec.Taints = []corev1.Taint{{Key: "nvsentinel.meshy.ai/hardware-support", Effect: corev1.TaintEffectNoSchedule}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			n, r := node.DeepCopy(), obj.DeepCopy()
			edit(n, r)
			require.Error(t, proof.checkSession(n, r))
		})
	}

	obj.SetGeneration(2)
	cond := metav1.Condition{Type: "ForlesGPUValidated", Status: metav1.ConditionTrue, Reason: "EightGPUsVerified", ObservedGeneration: 2}
	message, err := json.Marshal(proof)
	require.NoError(t, err)
	cond.Message = string(message)
	for name, edit := range map[string]func(*metav1.Condition){
		"old generation": func(c *metav1.Condition) { c.ObservedGeneration = 1 },
		"failed":         func(c *metav1.Condition) { c.Status = metav1.ConditionFalse },
		"other reason":   func(c *metav1.Condition) { c.Reason = "Skipped" },
		"malformed":      func(c *metav1.Condition) { c.Message = "invalid" },
	} {
		t.Run(name, func(t *testing.T) {
			c := cond
			edit(&c)
			_, err := readExternalReceipt(obj, []metav1.Condition{c})
			require.Error(t, err)
		})
	}
}

func TestExternalReceiptTracksDistinctFaultsBeyondNativeTestHistory(t *testing.T) {
	client, err := NewValidationClient(config.TomlConfig{RequireExternalRebootReceipt: true, HoldContactSupport: true}, nil, nil)
	require.NoError(t, err)
	history := ""
	count := maxQuarantineValidationHealthEvents + 2
	for i := range count {
		id := fmt.Sprint(i)
		for range 2 {
			history, err = client.UpdateQuarantineValidationAnnotation(t.Context(), history, &protos.HealthEvent{Id: id, Message: "fault details"})
			require.NoError(t, err)
		}
	}
	var events []common.HealthEventWithTests
	require.NoError(t, json.Unmarshal([]byte(history), &events))
	require.Len(t, events, count)
	for i, event := range events {
		require.Equal(t, fmt.Sprint(i), event.GetId())
		require.Empty(t, event.GetMessage())
	}
}
