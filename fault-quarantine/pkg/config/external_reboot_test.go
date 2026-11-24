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

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExternalReceiptActivation(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		flag, toml, hold, native, ok bool
	}{
		{"default", false, false, false, false, true},
		{"old ConfigMap", true, false, true, false, false},
		{"old Deployment", false, true, true, false, false},
		{"hardware support unheld", true, true, false, false, false},
		{"duplicate validator", true, true, true, true, false},
		{"external", true, true, true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := TomlConfig{RequireExternalRebootReceipt: tc.toml, HoldContactSupport: tc.hold, Validation: ValidationConfig{Enabled: tc.native}}
			err := cfg.ValidateExternalRebootReceipt(tc.flag)
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, tc.toml, cfg.WaitForReboot())
		})
	}
}
