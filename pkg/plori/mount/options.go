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

import "time"

// The mount-option vocabulary itself lives in pkg/plori/mountspec, next to the
// MountSpec that carries it (spec.go re-exports MountOptions and
// ParseMountOptions). What stays here are the timings that belong to the
// supervisor rather than to the wire: nothing outside this process reads them,
// and none of them is a mount option.
const (
	// DefaultUsageReportEvery reports usage every 15th renew: at a 20 s renew
	// interval that is one /usage call per five minutes per mount.
	DefaultUsageReportEvery = 15
	// HealthWriteInterval bounds how stale health.json may be. The plugin
	// reads anything older than 60 s as degraded, so the worker rewrites it
	// well inside that regardless of how long the renew interval is.
	HealthWriteInterval = 10 * time.Second
	// ReplicationRecoveryWindow is how long replication may stay failed
	// before the mount takes the ordered replication-failed stop; the lease
	// stop instant still caps it. It bounds how long metadata writes can go
	// unreplicated. It is not a fence: the lease deadline guard, the metadata
	// write gate and the per-generation replica prefix fence a stale writer.
	//
	// It is separate from the barrier interval (PLO-1172). Until then the
	// window was the 5 s barrier period, which is shorter than one probe
	// timeout plus one restart: in incident B a live Litestream under memory
	// pressure missed one 5 s probe, was killed, and its replacement had no
	// control socket when the window closed 7.7 s after the first failure, so
	// the session was lost with a healthy lease. 30 s covers
	// ReplicationProbeFailuresBeforeRestart timed-out probes (about 10 s of
	// failure, since the first failure is recorded when the first probe
	// returns) and leaves about 20 s for a replacement to open its socket and
	// answer one probe.
	ReplicationRecoveryWindow = 30 * time.Second
	// ReplicationProbeFailuresBeforeRestart is how many consecutive probe
	// failures a replicator that is still running gets before it is
	// restarted. Each timed-out probe blocks for ProbeTimeout (5 s), and while
	// replication is failed the one-second guard runs the next probe
	// immediately, so the third failure lands about 10 s after the first is
	// recorded. One or two slow answers are what memory pressure produces and
	// a restart does not fix; three in a row over 15 s is a child that is not
	// serving, and a restart at 10 s still leaves most of the window for the
	// replacement.
	ReplicationProbeFailuresBeforeRestart = 3
	// AdmissionRenewRounds is how many lease-renew intervals a write refused by
	// the volume ceiling may wait for an answer before it is given the
	// ceiling's own errno. It is counted in renew intervals rather than set as
	// a duration because the renewal IS the grant conversation: Admit opens a
	// request and pokes the renew timer, so the answer costs one round trip,
	// and the interval is the only cadence the worker has.
	//
	// Three: one round trip for the poked renew, one ordinary interval for the
	// case where the poke was dropped (the signal channel holds one and a
	// second send is discarded), and one more for the answer that follows it.
	// An answer that has not arrived by then is not late, it is absent, and a
	// writer that keeps waiting for it is the PLO-873 park — a job that never
	// finishes and a user who is told nothing.
	AdmissionRenewRounds = 3
)
