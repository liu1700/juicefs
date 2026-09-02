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

package mountspec

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// validSpecJSON is the shape services/control-plane/internal/storagespec
// actually emits (docs/design/per-agent-juicefs/mountspec.md §3), so a change
// on either side that breaks the wire shows up here.
//
// It is a hand-kept copy of that wire and therefore only half a guard: this
// package cannot see the control-plane's source. The other half lives in
// plori-runtime, where services/storage-worker decodes the control-plane's own
// generated golden with LoadSpec (PLO-395) — which is the check that would have
// caught `format` against `format_spec` on the day it was written.
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
  "format": {
    "volume_id": "550e8400-e29b-41d4-a716-446655440000",
    "bucket": "https://plorifs.lax1.vultrobjects.com/plorifs",
    "data_prefix": "agents/550e8400-e29b-41d4-a716-446655440000/",
    "meta_prefix": "agents-meta/550e8400-e29b-41d4-a716-446655440000/",
    "trash_days": 1,
    "capacity_bytes": 10737418240,
    "inodes": 1000000,
    "grant_epoch": 2,
    "expected_uuid": "6c1e5f2c-0f0a-4a1c-9f2d-2b4e6a8c0d1e"
  },
  "may_format": false,
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

func TestLoadAcceptsTheControlPlaneWireShape(t *testing.T) {
	spec, err := Load(writeSpec(t, validSpecJSON))
	if err != nil {
		t.Fatalf("Load: %v", err)
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
	// The format block is what PLO-395 was about: the control-plane sends
	// `format` with nine fields and `may_format`, and this worker decoded
	// `format_spec` with four, so every real spec was refused with exit 64
	// before it reached any of the above.
	if got, want := spec.Format.TrashDays, 1; got != want {
		t.Errorf("format.trash_days = %d, want %d", got, want)
	}
	if got, want := spec.Format.Bucket, "https://plorifs.lax1.vultrobjects.com/plorifs"; got != want {
		t.Errorf("format.bucket = %q, want %q", got, want)
	}
	if got, want := spec.Format.CapacityBytes, int64(10737418240); got != want {
		t.Errorf("format.capacity_bytes = %d, want %d", got, want)
	}
	if spec.MayFormat {
		t.Error("may_format is true for a volume that already carries a Format.UUID")
	}
}

// The bootstrap spec — an `allocating` volume whose lease IS the formatting
// lease (PLO-373). It is the only shape that authorises a format, and the shape
// no round trip covered until PLO-395.
func TestLoadAcceptsTheBootstrapWireShape(t *testing.T) {
	var raw map[string]any
	if err := json.Unmarshal([]byte(validSpecJSON), &raw); err != nil {
		t.Fatal(err)
	}
	raw["volume_state"] = "allocating"
	raw["format_uuid"] = ""
	raw["may_format"] = true
	delete(raw["format"].(map[string]any), "expected_uuid")
	body, _ := json.Marshal(raw)
	spec, err := Load(writeSpec(t, string(body)))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !spec.MayFormat {
		t.Error("may_format did not survive the round trip")
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
	_, err := Load(writeSpec(t, string(body)))
	if err == nil {
		t.Fatal("expected a refusal for an unknown field")
	}
	if !errors.Is(err, ErrSpec) {
		t.Errorf("refusal is not an ErrSpec: %v", err)
	}
}

func TestSpecRefusals(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(m map[string]any)
	}{
		{"unknown credential source", func(m map[string]any) {
			m["object_store"].(map[string]any)["credential_source"] = "vault_reference"
		}},
		{"zero fence epoch", func(m map[string]any) { m["fence_epoch"] = 0 }},
		{"renew not shorter than margin", func(m map[string]any) {
			m["lease_renew_interval"] = "60s"
		}},
		{"non-canonical data prefix", func(m map[string]any) {
			m["data_prefix"] = "agents/../agents/v1/"
		}},
		{"absolute data prefix", func(m map[string]any) { m["data_prefix"] = "/agents/v1/" }},
		{"nested roots", func(m map[string]any) {
			m["data_prefix"] = "agents/v1/"
			m["meta_prefix"] = "agents/v1/meta/g3/"
			m["fence_marker_key"] = "agents/v1/meta/g3/fence"
		}},
		{"fence marker outside meta prefix", func(m map[string]any) {
			m["fence_marker_key"] = "agents-meta/other/fence"
		}},
		{"unwritable volume state", func(m map[string]any) { m["volume_state"] = "retiring" }},
		{"missing format block", func(m map[string]any) { delete(m, "format") }},
		{"trash days below the floor", func(m map[string]any) {
			m["format"].(map[string]any)["trash_days"] = 0
		}},
		{"format names another volume", func(m map[string]any) {
			m["format"].(map[string]any)["volume_id"] = "00000000-0000-0000-0000-000000000000"
		}},
		{"format names another data prefix", func(m map[string]any) {
			m["format"].(map[string]any)["data_prefix"] = "agents/other/"
		}},
		// The format block carries the metadata ROOT; a spec that put this
		// writer's epoch prefix there would restore and replicate one segment
		// deeper than every other generation.
		{"format meta prefix is the epoch prefix", func(m map[string]any) {
			m["format"].(map[string]any)["meta_prefix"] = m["meta_prefix"]
		}},
		{"format bucket is not the spec's object store", func(m map[string]any) {
			m["format"].(map[string]any)["bucket"] = "https://elsewhere.example.com/other"
		}},
		{"expected uuid contradicts format_uuid", func(m map[string]any) {
			m["format"].(map[string]any)["expected_uuid"] = "0d0d0d0d-0000-0000-0000-000000000000"
		}},
		// The one that would licence formatting over a live filesystem.
		{"may_format granted on a formatted volume", func(m map[string]any) {
			m["may_format"] = true
		}},
		{"may_format withheld on an unformatted volume", func(m map[string]any) {
			m["format_uuid"] = ""
			delete(m["format"].(map[string]any), "expected_uuid")
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var raw map[string]any
			if err := json.Unmarshal([]byte(validSpecJSON), &raw); err != nil {
				t.Fatal(err)
			}
			tc.mutate(raw)
			body, _ := json.Marshal(raw)
			_, err := Load(writeSpec(t, string(body)))
			if err == nil {
				t.Fatal("expected a refusal")
			}
			if !errors.Is(err, ErrSpec) {
				t.Errorf("refusal is not an ErrSpec: %v", err)
			}
		})
	}
}
