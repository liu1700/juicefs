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
