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
	"testing"
	"time"
)

// The operator escape hatch replaces the server's list rather than merging
// into it: a merged list would make the resulting mount a function of two
// authorities and neither would be auditable.
func TestMountOptionsOverrideReplacesRatherThanMerges(t *testing.T) {
	spec := &MountSpec{MountOptions: []string{"writeback", "buffer_size=256"}}
	env := func(k string) string {
		if k == "PLORI_MOUNT_OPTIONS" {
			return "buffer_size=64, heartbeat=120"
		}
		return ""
	}
	got := spec.Options(env)
	if got.BufferSizeMB != 64 {
		t.Errorf("buffer size = %d, want 64", got.BufferSizeMB)
	}
	if got.Heartbeat != 120*time.Second {
		t.Errorf("heartbeat = %s, want 2m0s", got.Heartbeat)
	}
	if plain := spec.Options(func(string) string { return "" }); plain.BufferSizeMB != 256 {
		t.Errorf("without the override the server list must stand, got buffer size %d", plain.BufferSizeMB)
	}
}

// An unknown OPTION is tuning this worker does not have, so it is reported and
// ignored. An unknown top-level FIELD is authority it cannot honour, so it is
// refused (TestUnknownSpecFieldIsRefused). The two must not converge.
func TestUnknownMountOptionIsIgnoredNotRefused(t *testing.T) {
	got := ParseMountOptions([]string{"writeback", "future_knob=7", "allow_other", "gomemlimit=128"})
	if !got.Writeback || !got.AllowOther {
		t.Errorf("known options were dropped: %+v", got)
	}
	if len(got.Ignored) != 1 || got.Ignored[0] != "future_knob" {
		t.Errorf("ignored = %v, want [future_knob]; gomemlimit belongs to the plugin and must not be warned about", got.Ignored)
	}
	if got.BufferSizeMB != DefaultBufferSizeMB || got.Heartbeat != DefaultHeartbeat {
		t.Errorf("defaults were disturbed: %+v", got)
	}
}

func TestMountOptionDurationsAcceptBothSpellings(t *testing.T) {
	got := ParseMountOptions([]string{"heartbeat=300", "barrier_interval=90s", "litestream_sync=bogus"})
	if got.Heartbeat != 300*time.Second {
		t.Errorf("heartbeat = %s, want 5m0s", got.Heartbeat)
	}
	if got.BarrierInterval != 90*time.Second {
		t.Errorf("barrier interval = %s, want 1m30s", got.BarrierInterval)
	}
	// An unparseable value falls back to the default rather than to zero,
	// which would busy-loop the replicator.
	if got.LitestreamSync != DefaultLitestreamSync {
		t.Errorf("litestream sync = %s, want the default %s", got.LitestreamSync, DefaultLitestreamSync)
	}
}
