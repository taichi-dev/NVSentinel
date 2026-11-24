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
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/nvidia/nvsentinel/commons/pkg/annotation"
	"github.com/nvidia/nvsentinel/commons/pkg/drain"
	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/common"
	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"
)

type restartMaintenance struct {
	MaintenanceCR string    `json:"maintenanceCR"`
	CreatedAt     time.Time `json:"createdAt"`
}

// The restart group uses the maintenance creation time to decide which equivalent events it covers.
func (c *ValidationClient) completedSessionReboot(ctx context.Context, node *corev1.Node,
	session string) (string, error) {
	var state struct {
		EquivalenceGroups map[string]restartMaintenance `json:"equivalenceGroups"`
	}
	if err := json.Unmarshal([]byte(node.Annotations["latestFaultRemediationState"]), &state); err != nil {
		return "", fmt.Errorf("read restart maintenance state: %w", err)
	}

	restart := state.EquivalenceGroups["restart"]

	var events []common.HealthEventWithTests

	raw := node.Annotations[common.QuarantineValidationHealthEventAnnotationKey]
	if err := json.Unmarshal([]byte(raw), &events); err != nil {
		return "", fmt.Errorf("read validation events: %w", err)
	}

	var ids []string

	matched := false

	for _, event := range events {
		if event.HealthEvent == nil || len(event.Tests) == 0 {
			continue
		}

		ids = append(ids, event.Id)

		drained, err := c.restartCoversEvent(ctx, node.Name, restart, event.Id)
		if err != nil {
			return "", err
		}

		matched = matched || drained
	}

	sort.Strings(ids)

	if !matched || session != validationSessionID(ids) {
		return "", fmt.Errorf("waiting for full drain and matching restart request for session %s", session)
	}

	return c.completedRebootUID(ctx, node, restart.MaintenanceCR)
}

// A later fault needs its own repair. Only the request's initiating event proves the full drain.
func (c *ValidationClient) restartCoversEvent(ctx context.Context, node string, restart restartMaintenance,
	id string) (bool, error) {
	record, health, err := drain.GetHealthEventByID(ctx, c.healthEventStore, node, id)
	if err != nil {
		return false, err
	}

	if id == "" || health.NodeName != node || health.IsHealthy || record.CreatedAt.IsZero() ||
		record.CreatedAt.After(restart.CreatedAt) {
		return false, fmt.Errorf("restart %s does not cover fault %s on %s", restart.MaintenanceCR, id, node)
	}

	if restart.MaintenanceCR != "maintenance-"+node+"-"+id {
		return false, nil
	}

	partial, err := drain.PartialDrainEntity(health, c.partialDrainEnabled)
	if err != nil {
		return false, err
	}

	return partial == nil && record.HealthEventStatus.UserPodsEvictionStatus.Status == datastore.StatusSucceeded, nil
}

// The generic provider stores its pre-reboot boot ID in the SignalSent condition.
func (c *ValidationClient) completedRebootUID(ctx context.Context, node *corev1.Node, crName string) (string, error) {
	gvr := schema.GroupVersionResource{Group: "janitor.dgxc.nvidia.com", Version: "v1alpha1", Resource: "rebootnodes"}

	cr, err := c.k8sClient.DynamicClient.Resource(gvr).Get(ctx, crName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("read RebootNode %s: %w", crName, err)
	}

	var receipt struct {
		Spec struct {
			NodeName string `json:"nodeName"`
		} `json:"spec"`
		Status struct {
			CompletionTime *metav1.Time       `json:"completionTime"`
			Conditions     []metav1.Condition `json:"conditions"`
		} `json:"status"`
	}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(cr.Object, &receipt); err != nil {
		return "", fmt.Errorf("read RebootNode receipt: %w", err)
	}

	if !annotation.HasNodeOwner(cr, node) || cr.GetDeletionTimestamp() != nil || receipt.Spec.NodeName != node.Name {
		return "", fmt.Errorf("RebootNode %s does not belong to current node", crName)
	}

	if receipt.Status.CompletionTime.IsZero() || !rebootSucceeded(receipt.Status.Conditions, node.Status.NodeInfo.BootID) {
		return "", fmt.Errorf("waiting for completed reboot and new boot ID for %s", crName)
	}

	return string(cr.GetUID()), nil
}

func rebootSucceeded(conditions []metav1.Condition, currentBoot string) bool {
	sent := meta.FindStatusCondition(conditions, "SignalSent")

	ready := meta.FindStatusCondition(conditions, "NodeReady")
	if !conditionSucceeded(sent) || !conditionSucceeded(ready) {
		return false
	}

	return strings.TrimSpace(sent.Message) != "" && currentBoot != "" && currentBoot != sent.Message
}

func conditionSucceeded(c *metav1.Condition) bool {
	return c != nil && c.Status == metav1.ConditionTrue && c.Reason == "Succeeded"
}
