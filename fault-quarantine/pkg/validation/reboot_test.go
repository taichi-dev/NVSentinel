// Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
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
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRebootSucceededRequiresDistinctBootIDs(t *testing.T) {
	for _, tc := range []struct {
		name, previous, current string
		want                    bool
	}{
		{"same boot in padded receipt", " boot-a\n", "boot-a", false},
		{"same boot in padded node status", "boot-a", "\tboot-a ", false},
		{"missing previous boot", " \n", "boot-b", false},
		{"missing current boot", "boot-a", "\t ", false},
		{"distinct boots", " boot-a\n", "\tboot-b ", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conditions := []metav1.Condition{
				{Type: "SignalSent", Status: metav1.ConditionTrue, Reason: "Succeeded", Message: tc.previous},
				{Type: "NodeReady", Status: metav1.ConditionTrue, Reason: "Succeeded"},
			}
			require.Equal(t, tc.want, rebootSucceeded(conditions, tc.current))
		})
	}
}
