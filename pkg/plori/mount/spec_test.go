//go:build plori
// +build plori

/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mount

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// validSpecJSON is the shape services/control-plane/internal/storagespec
// actually emits (docs/design/per-agent-juicefs/mountspec.md §3), so a change
// on either side that breaks the wire shows up here.
const validSpecJSON = `{
  "storage_volume_id": "550e8400-e29b-41d4-a716-446655440000",
  "format_uuid": "6c1e5f2c-0f0a-4a1c-9f2d-2b4e6a8c0d1e",
  "generation": 1,
  "volume_state": "active",
  "fence_epoch": 3,
  "lease_expires_at": "2026-09-02T12:00:00Z",
  "lease_renew_interval": "20s",
  "write_stop_margin": "45s",
  "data_prefix": "agents/550e8400-e29b-41d4-a716-446655440000/",
  "meta_prefix": "agents-meta/550e8400-e29b-41d4-a716-446655440000/g3/",
  "fence_marker_key": "agents-meta/550e8400-e29b-41d4-a716-446655440000/g3/fence",
  "grant": {"bytes": 10737418240, "inodes": 1000000, "epoch": 2, "acked_epoch": 1},
  "object_store": {
    "endpoint": "https://plorifs.lax1.vultrobjects.com",
    "bucket": "plorifs",
    "region": "lax1",
    "credential_source": "node_secret"
  },
  "mount_options": ["--writeback", "--cache-size=10240"],
  "issued_at": "2026-09-02T11:58:00Z"
}`

func writeSpec(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "spec.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadSpecAcceptsTheControlPlaneWireShape(t *testing.T) {
	spec, err := LoadSpec(writeSpec(t, validSpecJSON))
	if err != nil {
		t.Fatalf("LoadSpec: %v", err)
	}
	if spec.FenceEpoch != 3 {
		t.Errorf("fence epoch = %d, want 3", spec.FenceEpoch)
	}
	if got, want := spec.LeaseRenewInterval.D(), 20*time.Second; got != want {
		t.Errorf("renew interval = %s, want %s", got, want)
	}
	if got, want := spec.WriteStopMargin.D(), 45*time.Second; got != want {
		t.Errorf("write stop margin = %s, want %s", got, want)
	}
	// The whole reason Format.Name is not just the volume id: JuiceFS derives
	// every data key from `<Format.Name>/`, and the S3 backend accepts no
	// prefix of its own, so the data root has to live in the name.
	if got, want := spec.VolumeName(), "agents/550e8400-e29b-41d4-a716-446655440000"; got != want {
		t.Errorf("VolumeName() = %q, want %q", got, want)
	}
}

// A field this worker does not know about means the control-plane is ahead of
// it. Ignoring it is the silent downgrade the closed vocabulary exists to
// prevent, so the spec is refused with exit 64.
func TestUnknownSpecFieldIsRefused(t *testing.T) {
	var raw map[string]any
	if err := json.Unmarshal([]byte(validSpecJSON), &raw); err != nil {
		t.Fatal(err)
	}
	raw["credential_handle"] = "redeem-me"
	body, _ := json.Marshal(raw)
	_, err := LoadSpec(writeSpec(t, string(body)))
	if err == nil {
		t.Fatal("expected a refusal for an unknown field")
	}
	if got := Classify(err).Exit; got != CodeSpecInvalid {
		t.Errorf("exit = %d, want %d", got, CodeSpecInvalid)
	}
}

func TestSpecRefusals(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(m map[string]any)
		want   int
	}{
		{"unknown credential source", func(m map[string]any) {
			m["object_store"].(map[string]any)["credential_source"] = "vault_reference"
		}, CodeSpecInvalid},
		{"zero fence epoch", func(m map[string]any) { m["fence_epoch"] = 0 }, CodeSpecInvalid},
		{"renew not shorter than margin", func(m map[string]any) {
			m["lease_renew_interval"] = "60s"
		}, CodeSpecInvalid},
		{"non-canonical data prefix", func(m map[string]any) {
			m["data_prefix"] = "agents/../agents/v1/"
		}, CodeSpecInvalid},
		{"absolute data prefix", func(m map[string]any) { m["data_prefix"] = "/agents/v1/" }, CodeSpecInvalid},
		{"nested roots", func(m map[string]any) {
			m["data_prefix"] = "agents/v1/"
			m["meta_prefix"] = "agents/v1/meta/g3/"
			m["fence_marker_key"] = "agents/v1/meta/g3/fence"
		}, CodeSpecInvalid},
		{"fence marker outside meta prefix", func(m map[string]any) {
			m["fence_marker_key"] = "agents-meta/other/fence"
		}, CodeSpecInvalid},
		{"unwritable volume state", func(m map[string]any) { m["volume_state"] = "retiring" }, CodeSpecInvalid},
		{"trash days below the floor", func(m map[string]any) {
			m["format_spec"] = map[string]any{"trash_days": 0}
		}, CodeSpecInvalid},
		{"storage outside the profile", func(m map[string]any) {
			m["format_spec"] = map[string]any{"trash_days": 1, "storage": "gs"}
		}, CodeSpecInvalid},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var raw map[string]any
			if err := json.Unmarshal([]byte(validSpecJSON), &raw); err != nil {
				t.Fatal(err)
			}
			tc.mutate(raw)
			body, _ := json.Marshal(raw)
			_, err := LoadSpec(writeSpec(t, string(body)))
			if err == nil {
				t.Fatal("expected a refusal")
			}
			if got := Classify(err).Exit; got != tc.want {
				t.Errorf("exit = %d, want %d (%v)", got, tc.want, err)
			}
		})
	}
}

func TestEffectiveFormatRaisesTrashDaysToTheFloor(t *testing.T) {
	spec := &MountSpec{}
	if got := spec.EffectiveFormat().TrashDays; got != DefaultTrashDays {
		t.Errorf("derived trash days = %d, want %d", got, DefaultTrashDays)
	}
	if got := spec.EffectiveFormat().Storage; got != "s3" {
		t.Errorf("derived storage = %q, want s3", got)
	}
}

// The operator escape hatch replaces the server's list rather than merging
// into it: a merged list would make the kernel mount string a function of two
// authorities and neither would be auditable.
func TestMountOptionsOverrideReplacesRatherThanMerges(t *testing.T) {
	spec := &MountSpec{MountOptions: []string{"--writeback", "--cache-size=10240"}}
	env := func(k string) string {
		if k == "PLORI_MOUNT_OPTIONS" {
			return "--cache-size=512, --heartbeat=60s"
		}
		return ""
	}
	got := spec.EffectiveMountOptions(env)
	want := []string{"--cache-size=512", "--heartbeat=60s"}
	if len(got) != len(want) {
		t.Fatalf("options = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("options = %v, want %v", got, want)
		}
	}
	if plain := spec.EffectiveMountOptions(func(string) string { return "" }); len(plain) != 2 {
		t.Fatalf("without the override the server list must stand, got %v", plain)
	}
}
