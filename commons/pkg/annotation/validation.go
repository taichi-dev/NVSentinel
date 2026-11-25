// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package annotation

import (
	"encoding/json"
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ValidationBootID identifies the boot on which a post-repair request must run and pass.
const ValidationBootID = "nvsentinel.nvidia.com/validation-boot-id"

// QuarantineValidationRequest names the boot-bound request holding this Node's scheduling gate.
const QuarantineValidationRequest = "nvsentinel.nvidia.com/quarantine-validation-request"

const ValidationSession = "nvsentinel.nvidia.com/validation-session"

// QuarantineValidationOwner also recognizes boot-bound sessions started before the handoff annotation existed.
func QuarantineValidationOwner(node *corev1.Node) string {
	if name := node.Annotations[QuarantineValidationRequest]; name != "" {
		return name
	}

	var entries []struct {
		Name   string `json:"name"`
		BootID string `json:"bootID"`
	}
	if raw := node.Annotations[ValidationSession]; raw != "" && json.Unmarshal([]byte(raw), &entries) == nil {
		for _, entry := range entries {
			if entry.BootID != "" && entry.Name != "" {
				return entry.Name
			}
		}
	}

	return ""
}

const QuarantineHealthEvent = "quarantineHealthEvent"

const RebootUID = "nvsentinel.nvidia.com/reboot-uid"
const CordonPreExisting = "nvsentinel.nvidia.com/cordon-pre-existing"

// HasNodeOwner matches the Node incarnation that an API object belongs to.
func HasNodeOwner(object metav1.Object, node *corev1.Node) bool {
	return node.UID != "" && slices.ContainsFunc(object.GetOwnerReferences(), func(ref metav1.OwnerReference) bool {
		return ref.APIVersion == "v1" && ref.Kind == "Node" && ref.Name == node.Name && ref.UID == node.UID
	})
}
