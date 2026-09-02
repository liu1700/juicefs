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
	"strconv"
	"strings"
	"time"
)

// MountOptions is the resolved form of the MountSpec's `mount_options`.
//
// The vocabulary is closed and small (CLI contract rev 2): `writeback`,
// `allow_other`, `buffer_size=`, `heartbeat=`, `barrier_interval=`,
// `litestream_sync=`. It is deliberately NOT "a list of juicefs flags" — the
// list is server-built and the two sides version independently, so the worker
// understands a vocabulary rather than a command line.
//
// An unrecognised key is LOGGED AND IGNORED, not refused. That is the opposite
// of the rule for an unknown top-level spec FIELD, and the difference is which
// side owns the meaning: an unknown field means the control-plane is describing
// authority this worker cannot honour, while an unknown option means it is
// tuning something this worker does not have. `gomemlimit=` is in the
// vocabulary but is consumed by the plugin, which exports GOMEMLIMIT into the
// worker's environment; the Go runtime reads it directly, so it appears here
// only to be ignored rather than warned about.
type MountOptions struct {
	Writeback       bool
	AllowOther      bool
	BufferSizeMB    int
	Heartbeat       time.Duration
	BarrierInterval time.Duration
	LitestreamSync  time.Duration
	// Ignored holds the keys this worker did not recognise, for one log line.
	Ignored []string
}

// Defaults are the PLO-316 wave-2 measured settings. Together they take an
// idle mount to 0.018 object ops/s and 32.6 MiB RSS.
const (
	// DefaultHeartbeat replaces JuiceFS's 12 s. The heartbeat writes to the
	// metadata on every tick, and with Litestream replicating that metadata
	// each tick became object writes; it fences nothing here, because the
	// session table is per-Agent local SQLite.
	DefaultHeartbeat = 300 * time.Second
	// DefaultBufferSizeMB is the floor the chunk store enforces anyway
	// (pkg/chunk/cached_store.go:584-586 raises anything smaller to 32).
	DefaultBufferSizeMB = 32
	// DefaultBarrierInterval: 15/60/300 s all measured zero write stall and
	// zero extra PUTs, so the period is chosen for how much an unclean death
	// loses, not for what it costs.
	DefaultBarrierInterval = 60 * time.Second
	// DefaultLitestreamSync stays at 1 s deliberately: raising it does not
	// reduce PUTs (batching is monitor-interval's job) and 10 s multiplies
	// replica lag 7.7x.
	DefaultLitestreamSync = time.Second
	// DefaultUsageReportEvery reports usage every 15th renew: at a 20 s renew
	// interval that is one /usage call per five minutes per mount.
	DefaultUsageReportEvery = 15
	// HealthWriteInterval bounds how stale health.json may be. The plugin
	// reads anything older than 60 s as degraded, so the worker rewrites it
	// well inside that regardless of how long the renew interval is.
	HealthWriteInterval = 10 * time.Second
	// DefaultTrashDays is the crash-consistency floor. A volume formatted with
	// trash-days 0 cannot satisfy the Rank 1 protocol, so the worker never
	// formats one and refuses to mount one.
	DefaultTrashDays = 1
)

// ParseMountOptions resolves the vocabulary over the defaults.
func ParseMountOptions(entries []string) MountOptions {
	opts := MountOptions{
		Writeback:       true,
		BufferSizeMB:    DefaultBufferSizeMB,
		Heartbeat:       DefaultHeartbeat,
		BarrierInterval: DefaultBarrierInterval,
		LitestreamSync:  DefaultLitestreamSync,
	}
	for _, raw := range entries {
		key, value, hasValue := strings.Cut(strings.TrimSpace(raw), "=")
		switch key {
		case "":
		case "writeback":
			opts.Writeback = !hasValue || value != "false"
		case "allow_other":
			// Honoured through the `-o` string rather than the AllowOther
			// default, which upstream ties to uid 0 (pkg/fuse/fuse.go:485)
			// while the explicit option sets it at any uid (:500-501). The
			// worker may not always run as root.
			opts.AllowOther = !hasValue || value != "false"
		case "buffer_size":
			if n, err := strconv.Atoi(value); err == nil && n > 0 {
				opts.BufferSizeMB = n
			}
		case "heartbeat":
			if d, ok := parseSeconds(value); ok {
				opts.Heartbeat = d
			}
		case "barrier_interval":
			if d, ok := parseSeconds(value); ok {
				opts.BarrierInterval = d
			}
		case "litestream_sync":
			if d, ok := parseSeconds(value); ok {
				opts.LitestreamSync = d
			}
		case "gomemlimit":
			// The plugin exports this as GOMEMLIMIT and the Go runtime reads
			// it. Setting it here as well would give one knob two owners.
		default:
			opts.Ignored = append(opts.Ignored, key)
		}
	}
	return opts
}

// parseSeconds accepts both a Go duration ("300s") and a bare number of
// seconds ("300"), because the vocabulary is written by hand in helm values as
// often as it is generated.
func parseSeconds(value string) (time.Duration, bool) {
	if value == "" {
		return 0, false
	}
	if d, err := time.ParseDuration(value); err == nil && d > 0 {
		return d, true
	}
	if n, err := strconv.Atoi(value); err == nil && n > 0 {
		return time.Duration(n) * time.Second, true
	}
	return 0, false
}

// Options resolves this spec's mount options, applying the operator override.
func (s *MountSpec) Options(env func(string) string) MountOptions {
	return ParseMountOptions(s.EffectiveMountOptions(env))
}

// EffectiveMountOptions applies the PLORI_MOUNT_OPTIONS operator escape hatch.
// The override replaces the server's list wholesale rather than merging: a
// merge would make the resulting mount a function of two authorities and
// neither would be auditable.
func (s *MountSpec) EffectiveMountOptions(env func(string) string) []string {
	if raw := env("PLORI_MOUNT_OPTIONS"); raw != "" {
		var out []string
		for _, part := range strings.Split(raw, ",") {
			if part = strings.TrimSpace(part); part != "" {
				out = append(out, part)
			}
		}
		return out
	}
	return append([]string(nil), s.MountOptions...)
}
