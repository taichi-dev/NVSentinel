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
	"context"
	"encoding/json"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/nvidia/nvsentinel/commons/pkg/annotation"
)

const rebooterAnnotation = "gpu-node-rebooter.meshy.ai/"

type externalReceipt struct {
	RequestUID     string `json:"requestUID"`
	NodeUID        string `json:"nodeUID"`
	Session        string `json:"session"`
	SourceBootID   string `json:"sourceBootID"`
	VerifiedBootID string `json:"verifiedBootID"`
}

// CheckExternalRebootReceipt runs inside the resourceVersion-guarded Node patch.
func (c *ValidationClient) CheckExternalRebootReceipt(ctx context.Context, node *corev1.Node) error {
	name, err := c.sessionRestart(ctx, node, "", nil)
	if err != nil {
		return err
	}

	cr, err := c.k8sClient.DynamicClient.Resource(rebootNodeGVR).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read external RebootNode %s: %w", name, err)
	}

	return verifyExternalReceipt(node, cr)
}

func verifyExternalReceipt(node *corev1.Node, cr *unstructured.Unstructured) error {
	var request struct {
		Spec struct {
			NodeName string `json:"nodeName"`
			Force    bool   `json:"force"`
		} `json:"spec"`
		Status struct {
			Conditions []metav1.Condition `json:"conditions"`
		} `json:"status"`
	}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(cr.Object, &request); err != nil {
		return fmt.Errorf("read external reboot receipt: %w", err)
	}

	if cr.GetDeletionTimestamp() != nil || !annotation.HasNodeOwner(cr, node) || request.Spec.NodeName != node.Name ||
		request.Spec.Force || cr.GetLabels()["app.kubernetes.io/managed-by"] != "nvsentinel" ||
		cr.GetLabels()["meshy.ai/reboot-executor"] != "gpu-node-rebooter" {
		return fmt.Errorf("RebootNode %s is not an owned external reboot for %s", cr.GetName(), node.Name)
	}

	receipt, err := readExternalReceipt(cr, request.Status.Conditions)
	if err != nil {
		return err
	}

	return receipt.checkSession(node, cr)
}

func readExternalReceipt(cr *unstructured.Unstructured, conditions []metav1.Condition) (externalReceipt, error) {
	terminal := meta.FindStatusCondition(conditions, "NodeReady")
	if terminal != nil && terminal.Status == metav1.ConditionFalse {
		return externalReceipt{}, fmt.Errorf("RebootNode %s failed", cr.GetName())
	}

	validated := meta.FindStatusCondition(conditions, "ForlesGPUValidated")
	if validated == nil || validated.Status != metav1.ConditionTrue || validated.Reason != "EightGPUsVerified" ||
		cr.GetGeneration() == 0 || validated.ObservedGeneration != cr.GetGeneration() {
		return externalReceipt{}, fmt.Errorf("waiting for current GPU validation on RebootNode %s", cr.GetName())
	}

	var receipt externalReceipt
	if err := json.Unmarshal([]byte(validated.Message), &receipt); err != nil {
		return externalReceipt{}, fmt.Errorf("decode GPU validation receipt: %w", err)
	}

	return receipt, nil
}

func (r externalReceipt) checkSession(node *corev1.Node, cr *unstructured.Unstructured) error {
	ann := node.Annotations

	want := externalReceipt{RequestUID: string(cr.GetUID()), NodeUID: string(node.UID),
		Session: ann[rebooterAnnotation+"session"], SourceBootID: ann[rebooterAnnotation+"source-boot-id"],
		VerifiedBootID: ann[rebooterAnnotation+"verified-boot-id"]}
	if r != want || r.RequestUID == "" || r.Session == "" || r.SourceBootID == "" ||
		r.VerifiedBootID == "" || r.VerifiedBootID == r.SourceBootID || r.VerifiedBootID != node.Status.NodeInfo.BootID {
		return fmt.Errorf("GPU receipt does not match the current request, node, session and boot")
	}

	if !returningNodeReady(node) {
		return fmt.Errorf("waiting for Ready node %s in the verified reboot session", node.Name)
	}

	return checkAttachedRequest(node, cr)
}

func returningNodeReady(node *corev1.Node) bool {
	ready := slices.ContainsFunc(node.Status.Conditions, func(c corev1.NodeCondition) bool {
		return c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue
	})
	support := slices.ContainsFunc(node.Spec.Taints, func(t corev1.Taint) bool {
		return t.Key == "nvsentinel.meshy.ai/hardware-support" && t.Effect == corev1.TaintEffectNoSchedule
	})

	return ready && !support && node.DeletionTimestamp == nil &&
		node.Annotations[rebooterAnnotation+"phase"] == "returning" && node.Annotations[rebooterAnnotation+"reboot-at"] != ""
}

func checkAttachedRequest(node *corev1.Node, cr *unstructured.Unstructured) error {
	var attached []struct{ Name, UID, Node string }
	if err := json.Unmarshal([]byte(node.Annotations[rebooterAnnotation+"nvsentinel-requests"]), &attached); err != nil {
		return fmt.Errorf("read attached RebootNodes: %w", err)
	}

	for _, ref := range attached {
		if ref.Name == cr.GetName() && ref.UID == string(cr.GetUID()) && ref.Node == node.Name {
			return nil
		}
	}

	return fmt.Errorf("RebootNode %s is not attached to this session", cr.GetName())
}
