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
	// GoMemLimitBytes is the `gomemlimit=` value in bytes, 0 when the option
	// is absent or unparseable. Before PLO-366 this key was consumed only by
	// the plugin, which exports it as GOMEMLIMIT before exec; it is parsed
	// here as well because the FUSE loop now runs in the worker process, so
	// the value bounds THIS heap, and PLORI_MOUNT_OPTIONS — the operator
	// escape hatch — has to be able to move it after the environment is set.
	GoMemLimitBytes int64
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
	// DefaultGoMemLimit is the per-mount soft heap ceiling, in bytes. 96 MiB
	// is the ADR §4 B9 value: measured on the production node shape it buys
	// the eighth concurrent `git clone` at 3.5 % faster wall time and zero
	// OOMs, and the Go runtime caps GC CPU at ~50 % rather than thrashing
	// when a heap will not fit under it (PLO-346, PLO-385). It is a soft
	// bound by construction — it excludes C allocations and mmap, and this
	// binary is CGO for SQLite — which is why the plugin also sets a cgroup
	// memory.max beside it.
	DefaultGoMemLimit = 96 << 20
	// DefaultTrashDays is the crash-consistency floor. A volume formatted with
	// trash-days 0 cannot satisfy the Rank 1 protocol, so the worker never
	// formats one and refuses to mount one.
	DefaultTrashDays = 1
)

// Writeback backlog bounds (PLO-383). The writeback backlog -- blocks staged on
// local disk and not yet uploaded -- is both the loss window if the node dies
// and the work the ordered stop's barrier has to finish inside the writer's
// remaining lease. Neither is bounded unless the backlog is.
const (
	// DefaultMaxStagingBacklog is the ceiling, in blocks. Above it a write is
	// uploaded through rather than staged: the writer waits for the object
	// store and the backlog stops growing. Nothing is dropped.
	//
	// Sizing, from the only measurement on the production node shape
	// (`vhf-1c-2gb`, PLO-346, docs/design/per-agent-juicefs/benchmark-real-node.md
	// §5): the shutdown barrier drained ~345 staged blocks in 10,724 ms with
	// `--max-uploads 1` -- 31 ms per block, serialised, on a core that was also
	// serving FUSE. The 45 s write-stop margin therefore covers ~1,447 blocks
	// in that worst case, and 1,024 leaves a 1.4x margin on it. Production runs
	// `--max-uploads 20`, so the real headroom is larger again.
	//
	// It is NOT sized from that run's headline 595 s / 1,008-block figure. That
	// was the PASSIVE drain, quantised by a once-a-minute re-queue sweep
	// (pkg/chunk/cached_store.go stagingSweepInterval, a flat minute before
	// PLO-383), not by per-block cost; the ordered stop does not use that path,
	// it force-queues through the barrier.
	DefaultMaxStagingBacklog = 1024

	// DefaultDrainPerBlock seeds the supervisor's measured drain model before
	// its first barrier with a non-empty queue. Same measurement as above,
	// rounded up: 10,724 ms / 345 blocks = 31.1 ms.
	DefaultDrainPerBlock = 32 * time.Millisecond

	// MaxProjectedDrain caps the projected drain the worker publishes and the
	// plugin waits for. The plugin gives a stopping worker
	// `write_stop_margin + projected_drain + 10 s` before SIGKILL, and the
	// kubelet's own per-volume CSI operation timeout is 2 minutes, so the whole
	// sum has to fit inside that: 45 + 60 + 10 = 115 s.
	MaxProjectedDrain = 60 * time.Second
)

// ParseMountOptions resolves the vocabulary over the defaults.
func ParseMountOptions(entries []string) MountOptions {
	opts := MountOptions{
		Writeback:       true,
		BufferSizeMB:    DefaultBufferSizeMB,
		Heartbeat:       DefaultHeartbeat,
		BarrierInterval: DefaultBarrierInterval,
		LitestreamSync:  DefaultLitestreamSync,
		GoMemLimitBytes: DefaultGoMemLimit,
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
			// Parsed with the Go runtime's own grammar so the value the
			// worker applies and the value GOMEMLIMIT means are the same
			// number, not two readings of one string. An unparseable value
			// leaves the default rather than removing the bound.
			if n, ok := ParseGoSize(value); ok {
				opts.GoMemLimitBytes = n
			}
		default:
			opts.Ignored = append(opts.Ignored, key)
		}
	}
	return opts
}

// ParseGoSize parses the size grammar GOMEMLIMIT uses: a decimal number with
// an optional B/KiB/MiB/GiB/TiB suffix, powers of two, no decimal point
// (go/src/runtime/extern.go, `godebug` memory limit parsing). It is exported
// because the control-plane issues these strings and the plugin exports them,
// so all three sides can agree on what one means.
func ParseGoSize(value string) (int64, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	units := []struct {
		suffix string
		scale  int64
	}{
		{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"B", 1},
	}
	for _, u := range units {
		if !strings.HasSuffix(value, u.suffix) {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSuffix(value, u.suffix), 10, 64)
		if err != nil || n <= 0 {
			return 0, false
		}
		return n * u.scale, true
	}
	// A bare number is bytes, which is what the runtime does with one.
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
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
