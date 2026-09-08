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
	"testing"
	"time"
)

// PLO-637. A mount that lived nine seconds and stopped cleanly posted
// used_bytes=0 used_inodes=0 for a volume holding 1572 files and 81 MB, and the
// control plane accepted the zero over the previous holder's 81465344 / 1572.
// The ordered stop read its usage after it had closed the metadata engine, and
// a mount younger than the engine's first counter refresh has no in-memory
// totals to answer from, so StatFS fell back to the closed engine and returned
// zero. The read now happens in step 4, between the seal and the close.

// closingVolume answers Usage with real numbers while the metadata session is
// open and with whatever a closed engine produces afterwards. Both tests below
// fail against the pre-fix ordering and pass against the fixed one.
type closingVolume struct {
	*fakeVolume
	closed bool
	// afterClose and afterErr are what Usage answers once Close has run. A zero
	// Usage is the production symptom; an error is the other shape a read
	// against a closed engine takes.
	afterClose Usage
	afterErr   error
}

func newClosingVolume() *closingVolume {
	return &closingVolume{fakeVolume: healthyVolume()}
}

func (v *closingVolume) Close() error {
	v.mu.Lock()
	v.closed = true
	v.mu.Unlock()
	return v.fakeVolume.Close()
}

func (v *closingVolume) Usage(ctx context.Context, withTrash bool) (Usage, error) {
	v.mu.Lock()
	closed, after, afterErr := v.closed, v.afterClose, v.afterErr
	v.mu.Unlock()
	if closed {
		return after, afterErr
	}
	return v.fakeVolume.Usage(ctx, withTrash)
}

// TestTheFinalUsageIsReadBeforeTheMetadataEngineCloses is the regression. The
// volume reports what the incident's volume held while it is open and the
// zeros a closed engine produces afterwards, so the figure the control plane
// receives says which side of Close the reading came from.
func TestTheFinalUsageIsReadBeforeTheMetadataEngineCloses(t *testing.T) {
	live := Usage{Bytes: 81465344, Inodes: 1572}
	vol := newClosingVolume()
	vol.setUsage(live, nil)
	cp := &fakeCP{}
	sup := newCloseoutSup(t, testSpec(), vol, cp, &fakeReplicator{}, newSharedFencer())

	f := sup.Run(context.Background(), stopAfter(120*time.Millisecond))
	if f.Exit != CodeOK {
		t.Fatalf("exit = %d (%v), want a clean stop", f.Exit, f.Err)
	}
	reported := cp.reportedUsages()
	if len(reported) == 0 {
		t.Fatal("a clean stop posted no final usage report")
	}
	final := reported[len(reported)-1]
	if final.Bytes != live.Bytes || final.Inodes != live.Inodes {
		t.Errorf("final usage = %d bytes / %d inodes, want %d / %d: the snapshot was taken after Close",
			final.Bytes, final.Inodes, live.Bytes, live.Inodes)
	}
	// Where the report is posted has not moved: still step 6, after the durable
	// point and before the lease release.
	order := cp.order()
	if n := len(order); n < 2 || order[n-2] != "usage" || order[n-1] != "release" {
		t.Errorf("control-plane calls = %v, want the final usage report immediately before the release", order)
	}
}

// TestAFailedPreCloseUsageReadPostsNoFinalReport is the other half of the
// contract. A reading that fails is not retried after the close and not
// replaced by a guess: nothing is posted, the usage_read_failed line carries
// the reason, and the stop finishes exactly as it did before.
func TestAFailedPreCloseUsageReadPostsNoFinalReport(t *testing.T) {
	vol := newClosingVolume()
	vol.setUsage(Usage{}, errors.New("metadata counters unavailable"))
	// Real numbers after the close, so a stop that still read late would post
	// something and this test would catch it.
	vol.afterClose = Usage{Bytes: 81465344, Inodes: 1572}
	cp := &fakeCP{}
	sup := newCloseoutSup(t, testSpec(), vol, cp, &fakeReplicator{}, newSharedFencer())

	f := sup.Run(context.Background(), stopAfter(120*time.Millisecond))
	if f.Exit != CodeOK {
		t.Fatalf("exit = %d (%v), want a clean stop", f.Exit, f.Err)
	}
	if got := cp.reportedUsages(); len(got) != 0 {
		t.Errorf("usage reports = %+v, want none: a reading that failed must not be posted", got)
	}
	if cp.released != ReasonShutdown {
		t.Errorf("released with reason %q, want %q", cp.released, ReasonShutdown)
	}
	if !exists(t, sup.Paths.CleanStopPath()) {
		t.Error("a failed usage reading cost the stop its clean marker")
	}
}

// PLO-498. The final usage figure went to the control-plane and health.json
// kept whatever the last periodic loop iteration had read, so the file an
// operator (and the CSI plugin) reads after a stop disagreed with the report
// the volume's usage was billed from. The stop now republishes health.json from
// the same snapshot it posted.

// unmountingVolume answers Usage with one figure while the mount is serving and
// another once Unmount has run. The ordered stop takes its final snapshot
// between the unmount and the close, so the two figures separate the final
// reading from every reading the periodic loop took.
type unmountingVolume struct {
	*fakeVolume
	unmounted bool
	after     Usage
}

func newUnmountingVolume() *unmountingVolume {
	return &unmountingVolume{fakeVolume: healthyVolume()}
}

func (v *unmountingVolume) Unmount(ctx context.Context) error {
	v.mu.Lock()
	v.unmounted = true
	v.mu.Unlock()
	return v.fakeVolume.Unmount(ctx)
}

func (v *unmountingVolume) Usage(ctx context.Context, withTrash bool) (Usage, error) {
	v.mu.Lock()
	unmounted, after := v.unmounted, v.after
	v.mu.Unlock()
	if unmounted {
		return after, nil
	}
	return v.fakeVolume.Usage(ctx, withTrash)
}

func TestHealthJSONEndsTheStopWithTheReportedUsage(t *testing.T) {
	serving := Usage{Bytes: 4096, Inodes: 3}
	final := Usage{Bytes: 81465344, Inodes: 1572}
	vol := newUnmountingVolume()
	vol.setUsage(serving, nil)
	vol.after = final
	cp := &fakeCP{}
	sup := newCloseoutSup(t, testSpec(), vol, cp, &fakeReplicator{}, newSharedFencer())

	f := sup.Run(context.Background(), stopAfter(120*time.Millisecond))
	if f.Exit != CodeOK {
		t.Fatalf("exit = %d (%v), want a clean stop", f.Exit, f.Err)
	}
	reported := cp.reportedUsages()
	if len(reported) == 0 {
		t.Fatal("a clean stop posted no final usage report")
	}
	last := reported[len(reported)-1]
	if last.Bytes != final.Bytes || last.Inodes != final.Inodes {
		t.Fatalf("final report = %d bytes / %d inodes, want %d / %d",
			last.Bytes, last.Inodes, final.Bytes, final.Inodes)
	}
	h := readHealth(t, sup)
	if h.UsedBytes != last.Bytes || h.UsedInodes != last.Inodes {
		t.Errorf("health.json = %d bytes / %d inodes, final usage report = %d / %d: the file was not rewritten from the posted snapshot",
			h.UsedBytes, h.UsedInodes, last.Bytes, last.Inodes)
	}
}

// A stop whose final usage reading failed posts nothing, so there is no posted
// snapshot to republish and health.json keeps the last figure the loop read.
// The stop is still clean.
func TestAFailedFinalUsageReadLeavesHealthAlone(t *testing.T) {
	vol := newClosingVolume()
	vol.setUsage(Usage{}, errors.New("metadata counters unavailable"))
	cp := &fakeCP{}
	sup := newCloseoutSup(t, testSpec(), vol, cp, &fakeReplicator{}, newSharedFencer())

	f := sup.Run(context.Background(), stopAfter(120*time.Millisecond))
	if f.Exit != CodeOK {
		t.Fatalf("exit = %d (%v), want a clean stop", f.Exit, f.Err)
	}
	if got := cp.reportedUsages(); len(got) != 0 {
		t.Fatalf("usage reports = %+v, want none", got)
	}
	if h := readHealth(t, sup); h.UsedBytes != 0 || h.UsedInodes != 0 {
		t.Errorf("health.json = %d bytes / %d inodes, want the unchanged 0 / 0", h.UsedBytes, h.UsedInodes)
	}
}
