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
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/nvidia/nvsentinel/commons/pkg/annotation"
	"github.com/nvidia/nvsentinel/commons/pkg/drain"
	"github.com/nvidia/nvsentinel/commons/pkg/eventutil"
	"github.com/nvidia/nvsentinel/data-models/pkg/model"
	"github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/common"
	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"
	"github.com/nvidia/nvsentinel/store-client/pkg/query"
)

type restartMaintenance struct {
	MaintenanceCR string    `json:"maintenanceCR"`
	CreatedAt     time.Time `json:"createdAt"`
}

var rebootNodeGVR = schema.GroupVersionResource{
	Group: "janitor.dgxc.nvidia.com", Version: "v1alpha1", Resource: "rebootnodes",
}

// The restart group uses the maintenance creation time to decide which equivalent events it covers.
func (c *ValidationClient) completedSessionReboot(ctx context.Context, node *corev1.Node,
	session string, requestedTests []string) (string, error) {
	name, err := c.sessionRestart(ctx, node, session, requestedTests)
	if err != nil {
		return "", err
	}

	return c.completedRebootUID(ctx, node, name)
}

func (c *ValidationClient) sessionRestart(ctx context.Context, node *corev1.Node,
	session string, requestedTests []string) (string, error) {
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

	tests, ids := c.validationRequirements(events)
	if len(ids) == 0 || ids[0] == "" || (!c.externalRebootReceipt &&
		(session != validationSessionID(ids) || !slices.Equal(requestedTests, tests))) {
		return "", fmt.Errorf("validation session changed on node %s", node.Name)
	}

	allowEvicted := !c.externalRebootReceipt && c.knownOverflowTests(events, ids, tests)
	if err := c.verifyRestartDrain(ctx, node.Name, restart, ids, allowEvicted); err != nil {
		return "", err
	}

	return restart.MaintenanceCR, nil
}

// Older capped annotations have no test summary. Their retained tests suffice when they cover every enabled rule.
func (c *ValidationClient) knownOverflowTests(events []common.HealthEventWithTests, ids, tests []string) bool {
	if len(slices.Compact(slices.Clone(ids))) != maxQuarantineValidationHealthEvents || ids[0] == "" {
		return false
	}

	if len(events[0].SessionTests) > 0 {
		return true
	}

	for _, rule := range c.validationRuleSets {
		for _, test := range c.validationRuleSetTestsMap[rule.GetName()] {
			if !slices.Contains(tests, test) {
				return false
			}
		}
	}

	return true
}

func (c *ValidationClient) verifyRestartDrain(ctx context.Context, node string, restart restartMaintenance,
	ids []string, allowEvicted bool) error {
	initiator := restart.initiator(node)
	if initiator == "" {
		return fmt.Errorf("missing restart for node %s", node)
	}

	retained := slices.Contains(ids, initiator)
	if !retained && !allowEvicted {
		return fmt.Errorf("cannot establish complete validation history for %s on %s", initiator, node)
	}

	if err := c.verifyCoveredFaults(ctx, node, restart, initiator, ids); err != nil {
		return err
	}

	record, health, err := c.coveredFault(ctx, node, restart, initiator)
	if err != nil {
		return err
	}

	if err := c.requireFullDrain(record, health); err != nil {
		return err
	}

	if !retained {
		return c.verifyEvictedInitiator(ctx, node, initiator, record, restart)
	}

	return nil
}

func (c *ValidationClient) verifyCoveredFaults(ctx context.Context, node string, restart restartMaintenance,
	initiator string, ids []string) error {
	for _, id := range ids {
		if id == initiator {
			continue
		}

		if _, _, err := c.coveredFault(ctx, node, restart, id); err != nil {
			return err
		}
	}

	return nil
}

func (r restartMaintenance) initiator(node string) string {
	id, ok := strings.CutPrefix(r.MaintenanceCR, "maintenance-"+node+"-")
	if !ok {
		return ""
	}

	return id
}

// Each retained fault must predate this repair. Only the CR's exact initiating document proves its drain.
func (c *ValidationClient) coveredFault(ctx context.Context, node string, restart restartMaintenance, id string) (
	*datastore.HealthEventWithStatus, *protos.HealthEvent, error) {
	// Use the datastore ordering timestamp for both the receipt and the history query.
	record, err := c.healthEventStore.FindLatestHealthEventByQuery(ctx, query.New().Build(query.Eq("_id", id)))
	if err != nil {
		return nil, nil, fmt.Errorf("read fault %s for node %s: %w", id, node, err)
	}

	if record == nil {
		return nil, nil, fmt.Errorf("missing fault %s on node %s", id, node)
	}

	parsed, err := eventutil.ParseHealthEventFromEvent(record.RawEvent)
	if err != nil {
		return nil, nil, fmt.Errorf("decode fault %s: %w", id, err)
	}

	health := parsed.HealthEvent
	if id == "" || health.NodeName != node || health.IsHealthy || record.CreatedAt.IsZero() ||
		record.CreatedAt.After(restart.CreatedAt) {
		return nil, nil, fmt.Errorf("restart %s does not cover fault %s on %s", restart.MaintenanceCR, id, node)
	}

	return record, health, nil
}

func (c *ValidationClient) requireFullDrain(record *datastore.HealthEventWithStatus, health *protos.HealthEvent) error {
	partial, err := drain.PartialDrainEntity(health, c.partialDrainEnabled)
	if err != nil {
		return err
	}

	if partial != nil || health.GetDrainOverrides().GetSkip() ||
		record.HealthEventStatus.UserPodsEvictionStatus.Status != datastore.StatusSucceeded {
		return fmt.Errorf("restart requires a successful full drain for its initiating event")
	}

	return nil
}

func (c *ValidationClient) verifyEvictedInitiator(ctx context.Context, node, id string,
	record *datastore.HealthEventWithStatus, restart restartMaintenance) error {
	status := record.HealthEventStatus.NodeQuarantined
	if status == nil || (*status != datastore.Quarantined && *status != datastore.AlreadyQuarantined) {
		return fmt.Errorf("restart initiator %s has no recorded quarantine on %s", id, node)
	}

	statuses := func(values ...any) query.Condition {
		return query.Or(query.In("healtheventstatus.nodequarantined", values),
			query.In("healtheventstatus.nodeQuarantined", values))
	}
	// Reject a later session boundary (including ambiguous timestamps) or an admitted fault after this repair.
	boundary := query.And(query.Gte("createdAt", record.CreatedAt), query.Ne("_id", id),
		statuses(string(model.Quarantined), string(model.UnQuarantined), string(model.Cancelled)))
	laterFault := query.And(query.Gt("createdAt", restart.CreatedAt),
		query.Or(query.Eq("healthevent.ishealthy", false), query.Eq("healthevent.ishealthy", nil)),
		query.Or(query.Eq("healthevent.isHealthy", false), query.Eq("healthevent.isHealthy", nil)),
		statuses(string(model.Quarantined), string(model.AlreadyQuarantined)))
	sameNode := query.Or(query.Eq("healthevent.nodename", node), query.Eq("healthevent.nodeName", node))

	conflict, err := c.healthEventStore.FindLatestHealthEventByQuery(ctx,
		query.New().Build(query.And(sameNode, query.Or(boundary, laterFault))))
	if err != nil {
		return fmt.Errorf("check quarantine history for restart %s: %w", restart.MaintenanceCR, err)
	}

	if conflict != nil {
		return fmt.Errorf("restart %s does not cover the current quarantine on %s", restart.MaintenanceCR, node)
	}

	return nil
}

// The generic provider stores its pre-reboot boot ID in the SignalSent condition.
func (c *ValidationClient) completedRebootUID(ctx context.Context, node *corev1.Node, crName string) (string, error) {
	cr, err := c.k8sClient.DynamicClient.Resource(rebootNodeGVR).Get(ctx, crName, metav1.GetOptions{})
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

	previousBoot := strings.TrimSpace(sent.Message)
	currentBoot = strings.TrimSpace(currentBoot)

	return previousBoot != "" && currentBoot != "" && currentBoot != previousBoot
}

func conditionSucceeded(c *metav1.Condition) bool {
	return c != nil && c.Status == metav1.ConditionTrue && c.Reason == "Succeeded"
}
