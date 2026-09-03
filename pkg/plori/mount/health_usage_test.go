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
	"context"
	"errors"
	"os"
	"syscall"
	"testing"
	"time"
)

// The volume's consumption is published twice — into health.json, which the CSI
// plugin republishes as `plori_mount_used_bytes` / `plori_mount_used_inodes`
// (PLO-406), and to the control-plane's /usage route, which is what the
// allocator and the account's disk page read. Two publications of one fact, so
// the fact is measured once: Supervisor.usage is the only caller of
// Volume.Usage, health.json publishes what it cached and the report sends what
// it returned.
//
// PLO-427 is what it cost to have the caching and the publishing on different
// clocks. The number was read only inside the /usage report — every fifteenth
// renewal, five minutes at the production interval — while health.json was
// rewritten every ten seconds from a field nothing had filled yet. Two staging
// runs therefore read `used_bytes: 0` from the plugin's gauge for their whole
// length while the control-plane's row for the same volume held the true
// figure, because the only report that ever landed was the one the ordered stop
// sends after the last health.json was written.

// runningSup starts a supervisor and stops it when the test ends.
func runningSup(t *testing.T, sup *Supervisor) {
	t.Helper()
	stop := make(chan os.Signal, 1)
	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), stop) }()
	t.Cleanup(func() { stop <- syscall.SIGTERM; <-done })
}

// A mount publishes the volume it actually has from its first health.json, not
// from its first /usage report. This is PLO-427's regression: the figure has to
// be in the file before any report has happened, because on a five-minute
// report cadence "after the first report" is most of a short Agent session.
func TestHealthJSONCarriesTheVolumesUsageBeforeTheFirstReport(t *testing.T) {
	vol := healthyVolume()
	vol.setUsage(Usage{Bytes: 68747263, Inodes: 35}, nil)
	cp := &fakeCP{}
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})
	runningSup(t, sup)

	var first Health
	waitFor(t, 10*time.Second, func() bool {
		h, ok := healthWhenWritten(sup)
		if ok {
			first = h
		}
		return ok
	}, "health.json was never written")

	// The claim is about a mount that has not reported yet, so say so rather
	// than assume it: the first report is fifteen renews away and the first
	// health write is one.
	if n := len(cp.reportedUsages()); n != 0 {
		t.Fatalf("%d usage reports landed before the first health.json; this test no longer proves what it claims", n)
	}
	if first.UsedBytes != 68747263 || first.UsedInodes != 35 {
		t.Errorf("first health.json = %d B / %d inodes, want 68747263 / 35: publishing zero for a volume that has data is PLO-427",
			first.UsedBytes, first.UsedInodes)
	}
}

// The two publications track the same volume. A figure that moves under a
// running mount reaches the control-plane and health.json, and they agree on
// it — one accessor, so there is nothing for them to disagree about.
func TestHealthJSONAndTheUsageReportCarryTheSameFigure(t *testing.T) {
	vol := healthyVolume()
	vol.setUsage(Usage{Bytes: 4 << 20, Inodes: 7}, nil)
	cp := &fakeCP{}
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})
	runningSup(t, sup)

	waitFor(t, 20*time.Second, func() bool { return len(cp.reportedUsages()) > 0 },
		"the worker never reported usage")

	// The Agent writes.
	vol.setUsage(Usage{Bytes: 96 << 20, Inodes: 341}, nil)

	waitFor(t, 20*time.Second, func() bool {
		u := cp.reportedUsages()
		return u[len(u)-1] == Usage{Bytes: 96 << 20, Inodes: 341}
	}, "the control-plane was never told the volume had grown")
	waitFor(t, 20*time.Second, func() bool {
		h, ok := healthWhenWritten(sup)
		return ok && h.UsedBytes == 96<<20 && h.UsedInodes == 341
	}, "health.json never carried the figure the control-plane was told")
}

// A volume nobody can measure keeps its last known figure. Zero is a real
// answer — an Agent that has written nothing — so publishing it for a failed
// reading would tell the plugin's gauge, and the operator reading it beside
// quota_exhausted, that a full volume had emptied itself.
func TestAnUnreadableUsageKeepsTheLastFigureRatherThanPublishingZero(t *testing.T) {
	vol := healthyVolume()
	vol.setUsage(Usage{Bytes: 12 << 20, Inodes: 9}, nil)
	cp := &fakeCP{}
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})
	runningSup(t, sup)

	waitFor(t, 10*time.Second, func() bool {
		h, ok := healthWhenWritten(sup)
		return ok && h.UsedBytes == 12<<20
	}, "health.json never carried the volume's usage")

	vol.setUsage(Usage{}, errors.New("metadata engine is gone"))
	// Long enough to cross a report tick as well as many health writes: the
	// report is every fifteenth renew and this waits for twenty more.
	before := len(cp.renewRequests())
	waitFor(t, 20*time.Second, func() bool { return len(cp.renewRequests()) >= before+20 },
		"the worker stopped renewing")

	if h := readHealth(t, sup); h.UsedBytes != 12<<20 || h.UsedInodes != 9 {
		t.Errorf("health.json = %d B / %d inodes after the readings started failing, want the last good 12582912 / 9",
			h.UsedBytes, h.UsedInodes)
	}
	for _, u := range cp.reportedUsages() {
		if u.Bytes == 0 {
			t.Error("a failed reading was reported to the control-plane as an empty volume")
		}
	}
}
