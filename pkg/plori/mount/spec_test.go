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
	"os"
	"path/filepath"
	"testing"
)

// The wire contract itself — decode, unknown-field refusal, and every
// validation refusal — is tested in pkg/plori/mountspec, where the types live
// and where the tests run on the default build with no tags. What is left here
// is the seam this package owns: a wire refusal has to arrive at exit 64.
//
// validSpecJSON below stays because it is the SUPERVISOR's fixture, not a
// second copy of the contract: supervisor_test.go and restore_source_test.go
// build their MountSpec from it. It is self-policing — testSpec() runs
// Validate() and panics — so it cannot quietly fall behind the wire.
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

// A spec the wire package refuses must reach the plugin as exit 64. The
// refusals themselves are pkg/plori/mountspec's tests; this one pins that the
// error still classifies across the package split, which an alias that lost
// ErrSpec identity would silently break.
func TestSpecRefusalClassifiesAsSpecInvalid(t *testing.T) {
	_, err := LoadSpec(writeSpec(t, `{"credential_handle":"redeem-me"}`))
	if err == nil {
		t.Fatal("expected a refusal for an unknown field")
	}
	if got := Classify(err).Exit; got != CodeSpecInvalid {
		t.Errorf("exit = %d, want %d (%v)", got, CodeSpecInvalid, err)
	}
}
