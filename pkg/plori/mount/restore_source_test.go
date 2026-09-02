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
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// restoreRecorder is a Replicator that remembers BOTH arguments of Restore. The
// package's other fake keeps only the prefix, and the anchor is half of what
// this issue is about.
type restoreRecorder struct {
	mu     sync.Mutex
	prefix string
	anchor time.Time
}

func (r *restoreRecorder) Restore(_ context.Context, sourcePrefix string, ts time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prefix, r.anchor = sourcePrefix, ts
	return nil
}
func (r *restoreRecorder) Start(context.Context) error       { return nil }
func (r *restoreRecorder) SyncAndWait(context.Context) error { return nil }
func (r *restoreRecorder) TxID(context.Context) (string, error) {
	return "0000000000000009", nil
}
func (r *restoreRecorder) Stop(context.Context) error { return nil }

func (r *restoreRecorder) result() (string, time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.prefix, r.anchor
}

// countingFencer records whether the LIST fallback was reached at all. "The
// server told us" and "we listed and got the same answer" are indistinguishable
// by the restored prefix alone, and the difference is one object-store round
// trip per mount start on every Agent (ADR §4 B9's op budget).
type countingFencer struct {
	mu     sync.Mutex
	prior  string
	listed int
}

func (f *countingFencer) Claim(context.Context, string, []byte) error { return nil }
func (f *countingFencer) PriorMetaPrefix(context.Context, string, int64) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listed++
	return f.prior, nil
}
func (f *countingFencer) lists() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listed
}

const testVolumeRoot = "agents-meta/550e8400-e29b-41d4-a716-446655440000/"

// runToCleanStop drives one supervisor through startup and an immediate
// SIGTERM, which is the shortest path that still performs the restore.
func runToCleanStop(t *testing.T, spec *MountSpec, rep Replicator, fencer Fencer) {
	t.Helper()
	sup := newSup(t, spec, &fakeFS{vol: healthyVolume()}, &fakeCP{}, &fakeReplicator{}, fencer)
	sup.Deps.Replicator = rep
	stop := make(chan os.Signal, 1)
	stop <- syscall.SIGTERM
	if got := sup.Run(context.Background(), stop); got.Exit != CodeOK {
		t.Fatalf("exit = %d: %v", got.Exit, got.Err)
	}
}

// TestRestoreSourcePrefersTheSpecOverTheListing is the server-derived half of
// PLO-391. When the control-plane knows which epoch produced the durable point
// it names that epoch's prefix, and the worker uses it verbatim — no LIST, and
// the restore stops at the point the server recorded rather than at whatever
// the replica's newest transaction happens to be.
func TestRestoreSourcePrefersTheSpecOverTheListing(t *testing.T) {
	spec := testSpec()
	spec.FenceEpoch = 7
	spec.MetaPrefix = testVolumeRoot + "g7/"
	spec.FenceMarkerKey = spec.MetaPrefix + "fence"
	// Epochs 5 and 6 came and went; only 4 ever replicated. The server says so.
	spec.RestoreFromPrefix = testVolumeRoot + "g4/"
	durable := time.Date(2026, 9, 2, 11, 30, 0, 0, time.UTC)
	spec.DurablePoint = &DurablePointSpec{
		DurableAt: durable, ReplicaTxID: "0000000000000042", FenceEpoch: 4,
	}
	if err := spec.Validate(); err != nil {
		t.Fatalf("the spec the control-plane emits must validate: %v", err)
	}

	rep := &restoreRecorder{}
	// The fencer would answer with the WRONG prefix if it were consulted, which
	// is what makes this assertion about precedence rather than about luck.
	fencer := &countingFencer{prior: testVolumeRoot + "g6/"}
	runToCleanStop(t, spec, rep, fencer)

	prefix, anchor := rep.result()
	if prefix != spec.RestoreFromPrefix {
		t.Errorf("restored from %q, want the spec's %q", prefix, spec.RestoreFromPrefix)
	}
	if !anchor.Equal(durable) {
		t.Errorf("restore anchor = %s, want the spec's durable point %s", anchor, durable)
	}
	if n := fencer.lists(); n != 0 {
		t.Errorf("listed the metadata root %d times although the spec named the source", n)
	}
}

// TestRestoreSourceFallsBackToTheListing is the other branch, and it is not a
// legacy path: a volume no writer has ever reported a durable point for has no
// server-side answer, and the listing is the only correct one. It must keep
// working unchanged, including its skip of an epoch that claimed a fence marker
// and replicated nothing.
func TestRestoreSourceFallsBackToTheListing(t *testing.T) {
	spec := testSpec()
	spec.FenceEpoch = 7
	spec.MetaPrefix = testVolumeRoot + "g7/"
	spec.FenceMarkerKey = spec.MetaPrefix + "fence"
	if spec.RestoreFromPrefix != "" || spec.DurablePoint != nil {
		t.Fatal("this test needs a spec carrying neither restore field")
	}

	rep := &restoreRecorder{}
	fencer := &countingFencer{prior: testVolumeRoot + "g3/"}
	runToCleanStop(t, spec, rep, fencer)

	prefix, anchor := rep.result()
	if prefix != fencer.prior {
		t.Errorf("restored from %q, want the listed prefix %q", prefix, fencer.prior)
	}
	if !anchor.IsZero() {
		t.Errorf("restore anchor = %s, want the zero time (restore the latest transaction)", anchor)
	}
	if n := fencer.lists(); n != 1 {
		t.Errorf("listed the metadata root %d times, want exactly 1", n)
	}
}

// TestALocalDurablePointTheServerNeverHeardAboutWinsWhole covers the state
// reportDurablePoint deliberately creates: the local file is written BEFORE the
// report is posted, so a generation that barriers and then loses the network
// leaves this node knowing a newer durable point than the control-plane does.
//
// Both halves must then come from the local point. Taking its anchor with the
// server's prefix would restore the OLDER epoch's replica up to an instant the
// newer epoch established, silently dropping everything the newer one wrote —
// worse than either source used alone.
func TestALocalDurablePointTheServerNeverHeardAboutWinsWhole(t *testing.T) {
	spec := testSpec()
	spec.FenceEpoch = 7
	spec.MetaPrefix = testVolumeRoot + "g7/"
	spec.FenceMarkerKey = spec.MetaPrefix + "fence"
	// The server's last word is epoch 4.
	spec.RestoreFromPrefix = testVolumeRoot + "g4/"
	spec.DurablePoint = &DurablePointSpec{
		DurableAt: time.Date(2026, 9, 2, 11, 0, 0, 0, time.UTC), FenceEpoch: 4,
	}
	if err := spec.Validate(); err != nil {
		t.Fatalf("spec: %v", err)
	}

	rep := &restoreRecorder{}
	fencer := &countingFencer{}
	sup := newSup(t, spec, &fakeFS{vol: healthyVolume()}, &fakeCP{}, &fakeReplicator{}, fencer)
	sup.Deps.Replicator = rep

	// Epoch 6 ran here, barriered, and never got its report through.
	local := time.Date(2026, 9, 2, 11, 45, 0, 0, time.UTC)
	if err := os.MkdirAll(sup.Paths.StateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := writeJSONAtomic(sup.Paths.DurablePointPath(), DurablePoint{
		Volume: spec.StorageVolumeID, FenceEpoch: 6, DurableAt: local,
		BarrierAt: local.Add(time.Second), ReplicaTxID: "0000000000000077",
	}); err != nil {
		t.Fatal(err)
	}

	stop := make(chan os.Signal, 1)
	stop <- syscall.SIGTERM
	if got := sup.Run(context.Background(), stop); got.Exit != CodeOK {
		t.Fatalf("exit = %d: %v", got.Exit, got.Err)
	}

	prefix, anchor := rep.result()
	if want := testVolumeRoot + "g6/"; prefix != want {
		t.Errorf("restored from %q, want the local point's %q — the server's g4/ would drop epoch 6's writes",
			prefix, want)
	}
	if !anchor.Equal(local) {
		t.Errorf("restore anchor = %s, want the local point's %s", anchor, local)
	}
	if n := fencer.lists(); n != 0 {
		t.Errorf("listed the metadata root %d times although a durable point was known", n)
	}
}

// TestHalfARestoreInstructionIsRefused pins the fail-closed rule the two fields
// exist under. PLO-373 declined to ship `durable_point` without
// `restore_from_prefix` because half the instruction is worse than none; the
// worker enforces the same thing on the wire it is handed.
func TestHalfARestoreInstructionIsRefused(t *testing.T) {
	root := testVolumeRoot
	point := func(epoch int64) *DurablePointSpec {
		return &DurablePointSpec{DurableAt: time.Now().UTC(), FenceEpoch: epoch}
	}
	cases := []struct {
		name   string
		mutate func(*MountSpec)
		want   string
	}{
		{"prefix without a point", func(s *MountSpec) {
			s.RestoreFromPrefix = root + "g2/"
		}, "must be sent together"},
		{"point without a prefix", func(s *MountSpec) {
			s.DurablePoint = point(2)
		}, "must be sent together"},
		{"they disagree about the epoch", func(s *MountSpec) {
			s.RestoreFromPrefix, s.DurablePoint = root+"g2/", point(1)
		}, "does not name durable_point.fence_epoch"},
		{"a point from an epoch ahead of this writer", func(s *MountSpec) {
			s.RestoreFromPrefix, s.DurablePoint = root+"g9/", point(9)
		}, "ahead of this writer's epoch"},
		{"a prefix outside this volume", func(s *MountSpec) {
			s.RestoreFromPrefix = "agents-meta/other-volume/g2/"
			s.DurablePoint = point(2)
		}, "outside this volume's metadata root"},
		{"epoch zero", func(s *MountSpec) {
			s.RestoreFromPrefix, s.DurablePoint = root+"g0/", point(0)
		}, "fence_epoch must be positive"},
		{"a point with no instant", func(s *MountSpec) {
			s.RestoreFromPrefix = root + "g2/"
			s.DurablePoint = &DurablePointSpec{FenceEpoch: 2}
		}, "durable_at is unset"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := testSpec()
			spec.FenceEpoch = 3
			spec.MetaPrefix = root + "g3/"
			spec.FenceMarkerKey = spec.MetaPrefix + "fence"
			tc.mutate(spec)
			err := spec.Validate()
			if err == nil {
				t.Fatalf("Validate accepted %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// TestSpecDurablePointSurvivesTheWire covers the decode: LoadSpec refuses
// unknown fields, so a control-plane that starts sending these two would break
// every worker that does not know them. This is the assertion that says this
// one does.
func TestSpecDurablePointSurvivesTheWire(t *testing.T) {
	body := strings.Replace(validSpecJSON,
		`  "mount_options"`,
		`  "restore_from_prefix": "`+testVolumeRoot+`g2/",
  "durable_point": {"durable_at": "2026-09-02T11:00:00Z", "replica_txid": "0000000000000042", "fence_epoch": 2},
  "mount_options"`, 1)
	if body == validSpecJSON {
		t.Fatal("the fixture changed shape; this test patched nothing")
	}
	spec, err := LoadSpec(writeSpec(t, body))
	if err != nil {
		t.Fatalf("LoadSpec: %v", err)
	}
	if spec.RestoreFromPrefix != testVolumeRoot+"g2/" {
		t.Errorf("restore_from_prefix = %q", spec.RestoreFromPrefix)
	}
	if spec.DurablePoint == nil {
		t.Fatal("durable_point did not decode")
	}
	if spec.DurablePoint.FenceEpoch != 2 || spec.DurablePoint.ReplicaTxID != "0000000000000042" {
		t.Errorf("durable_point = %+v", *spec.DurablePoint)
	}
	if want := time.Date(2026, 9, 2, 11, 0, 0, 0, time.UTC); !spec.DurablePoint.DurableAt.Equal(want) {
		t.Errorf("durable_point.durable_at = %s, want %s", spec.DurablePoint.DurableAt, want)
	}
}
