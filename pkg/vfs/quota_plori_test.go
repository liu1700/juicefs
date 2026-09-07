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

package vfs

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
)

// The metadata half of the live-grant mechanism is proved in pkg/meta. This is
// the half that only the real VFS can show: an Agent's write(2) does not reach
// meta.Write, it lands in a buffer whose slices are committed later
// (writer.go:208), so the ceiling refuses the COMMIT and the errno surfaces on
// the flush. That is the signal the supervisor turns into a Grow request, and
// it is what "EDQUOT triggers a Grow" actually looks like in this codebase —
// an ENOSPC out of a flush, not an EDQUOT out of a write.
//
// It is also where PLO-346 §8's "live quota-update cost" is measurable end to
// end: the object storage is right there, so the claim "applying a grant costs
// zero object requests" can be checked instead of argued.

const quotaTestCeiling = 8 << 20

type heldQuotaWriteMeta struct {
	meta.Meta
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (m *heldQuotaWriteMeta) Write(ctx meta.Context, inode meta.Ino, indx uint32, off uint32, slice meta.Slice, mtime time.Time) syscall.Errno {
	m.once.Do(func() { close(m.entered) })
	<-m.release
	return syscall.ENOSPC
}

// writeAndFlush writes one buffer and forces its slices to commit, returning
// the errno the commit produced.
func writeAndFlush(t *testing.T, v *VFS, ctx LogContext, ino Ino, fh uint64, off uint64, buf []byte) syscall.Errno {
	t.Helper()
	if e := v.Write(ctx, ino, buf, off, fh); e != 0 {
		return e
	}
	return v.Fsync(ctx, ino, 1, fh)
}

// fillToTheCeiling writes until the volume ceiling refuses a commit, and
// returns the errno it refused with.
func fillToTheCeiling(t *testing.T, v *VFS, ctx LogContext, name string) syscall.Errno {
	t.Helper()
	fe, fh, e := v.Create(ctx, 1, name, 0644, 0, syscall.O_RDWR)
	if e != 0 {
		t.Fatalf("create %s: %s", name, e)
	}
	defer v.Release(ctx, fe.Inode, fh)

	buf := make([]byte, 1<<20)
	for i := range 32 {
		if e := writeAndFlush(t, v, ctx, fe.Inode, fh, uint64(i)*uint64(len(buf)), buf); e != 0 {
			return e
		}
	}
	t.Fatalf("wrote 32 MiB against a %d-byte ceiling without a refusal", quotaTestCeiling)
	return 0
}

// TestPloriGrantAppliesLiveThroughTheVFS is PLO-324's core claim, end to end:
// a mount that is refusing writes accepts them again after a grant is applied
// in-process, with no remount, no reopen and no second metadata client.
func TestPloriGrantAppliesLiveThroughTheVFS(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ctx := NewLogContext(meta.Background())

	if err := meta.PloriApplyGrant(v.Meta, quotaTestCeiling, 16384); err != nil {
		t.Fatalf("set the starting ceiling: %s", err)
	}

	trips := meta.PloriVolumeQuotaTrips()
	if st := fillToTheCeiling(t, v, ctx, "fills-the-grant"); st != syscall.ENOSPC {
		t.Fatalf("the full volume refused with %v, want ENOSPC", st)
	}
	if got := meta.PloriVolumeQuotaTrips(); got <= trips {
		t.Fatalf("the volume ceiling refused a commit but the trip counter did not move (%d -> %d)", trips, got)
	}

	// The grant lands. Nothing is remounted, no handle is reopened, and the
	// same *VFS and the same metadata session carry on.
	if err := meta.PloriApplyGrant(v.Meta, 256<<20, 65536); err != nil {
		t.Fatalf("apply grant: %s", err)
	}

	fe, fh, e := v.Create(ctx, 1, "written-after-the-grant", 0644, 0, syscall.O_RDWR)
	if e != 0 {
		t.Fatalf("create after the grant: %s", e)
	}
	defer v.Release(ctx, fe.Inode, fh)
	if e := writeAndFlush(t, v, ctx, fe.Inode, fh, 0, make([]byte, 1<<20)); e != 0 {
		t.Errorf("write after the grant = %s, want it to succeed", e)
	}
}

// TestPloriDurabilityBarrierSkipsAPreExistingQuotaRefusal verifies that a
// failed file remains failed to its caller without stopping a later volume-wide
// durability point. The first writer intentionally stays open across both the
// refusal and the grant.
func TestPloriDurabilityBarrierSkipsAPreExistingQuotaRefusal(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ctx := NewLogContext(meta.Background())

	if err := meta.PloriApplyGrant(v.Meta, quotaTestCeiling, 16384); err != nil {
		t.Fatalf("set the starting ceiling: %s", err)
	}
	failed, failedFH, e := v.Create(ctx, 1, "refused-but-open", 0644, 0, syscall.O_RDWR)
	if e != 0 {
		t.Fatalf("create refused writer: %s", e)
	}

	buf := make([]byte, 1<<20)
	var refused syscall.Errno
	accepted := 0
	for i := range 32 {
		refused = writeAndFlush(t, v, ctx, failed.Inode, failedFH, uint64(i)*uint64(len(buf)), buf)
		if refused != 0 {
			break
		}
		accepted++
	}
	if refused != syscall.ENOSPC {
		t.Fatalf("quota refusal = %s, want ENOSPC", refused)
	}

	// A permanent refusal retains a per-file error and its refused slice is
	// gone. It must not prevent a later volume-wide drain.
	if err := v.FlushAll(""); err != nil {
		t.Fatalf("barrier after a pre-existing ENOSPC = %v", err)
	}
	if e := v.Fsync(ctx, failed.Inode, 1, failedFH); e != syscall.ENOSPC {
		t.Fatalf("fsync on the refused open writer = %s, want ENOSPC", e)
	}

	if err := meta.PloriApplyGrant(v.Meta, 256<<20, 65536); err != nil {
		t.Fatalf("apply growth after the refusal: %s", err)
	}
	healthy, healthyFH, e := v.Create(ctx, 1, "durable-after-growth", 0644, 0, syscall.O_RDWR)
	if e != 0 {
		t.Fatalf("create healthy writer: %s", e)
	}
	data := []byte("the healthy writer completes after growth")
	if e := writeAndFlush(t, v, ctx, healthy.Inode, healthyFH, 0, data); e != 0 {
		t.Fatalf("healthy writer after growth: %s", e)
	}
	for i := 0; i < 3; i++ {
		if err := v.FlushAll(""); err != nil {
			t.Fatalf("barrier %d after growth = %v", i+1, err)
		}
	}
	if e := v.Fsync(ctx, failed.Inode, 1, failedFH); e != syscall.ENOSPC {
		t.Fatalf("growth cleared refused writer error: %s", e)
	}

	v.Release(ctx, healthy.Inode, healthyFH)
	_, reopenedFH, e := v.Open(ctx, healthy.Inode, syscall.O_RDONLY)
	if e != 0 {
		t.Fatalf("reopen healthy writer: %s", e)
	}
	defer v.Release(ctx, healthy.Inode, reopenedFH)
	got := make([]byte, len(data))
	if n, e := v.Read(ctx, healthy.Inode, got, 0, reopenedFH); e != 0 || n != len(data) || string(got) != string(data) {
		t.Fatalf("read healthy durable data = (%d, %s, %q), want (%d, 0, %q)", n, e, got, len(data), data)
	}

	// Closing releases the failed writer's private error. Reopening checks that
	// the refused suffix never entered the file's committed contents.
	v.Release(ctx, failed.Inode, failedFH)
	writer := v.writer.(*dataWriter)
	deadline := time.Now().Add(time.Second)
	for writer.find(failed.Inode) != nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if writer.find(failed.Inode) != nil {
		t.Fatal("the closed refused writer was not released")
	}
	entry, e := v.GetAttr(ctx, failed.Inode, 0)
	if e != 0 {
		t.Fatalf("stat refused writer after close: %s", e)
	}
	if want := uint64(accepted * len(buf)); entry.Attr.Length != want {
		t.Fatalf("refused file length = %d, want committed prefix %d", entry.Attr.Length, want)
	}
	_, failedReadFH, e := v.Open(ctx, failed.Inode, syscall.O_RDONLY)
	if e != 0 {
		t.Fatalf("reopen refused writer: %s", e)
	}
	if n, e := v.Read(ctx, failed.Inode, make([]byte, 1), uint64(accepted*len(buf)), failedReadFH); e != 0 || n != 0 {
		t.Fatalf("read refused suffix = (%d, %s), want (0, 0)", n, e)
	}
	v.Release(ctx, failed.Inode, failedReadFH)
	_, failedWriteFH, e := v.Open(ctx, failed.Inode, syscall.O_RDWR)
	if e != 0 {
		t.Fatalf("reopen refused writer for a new write: %s", e)
	}
	reopened := []byte("committed after reopening")
	off := uint64(accepted * len(buf))
	if e := writeAndFlush(t, v, ctx, failed.Inode, failedWriteFH, off, reopened); e != 0 {
		t.Fatalf("write through reopened writer: %s", e)
	}
	v.Release(ctx, failed.Inode, failedWriteFH)
	_, verifyFH, e := v.Open(ctx, failed.Inode, syscall.O_RDONLY)
	if e != 0 {
		t.Fatalf("reopen committed replacement data: %s", e)
	}
	defer v.Release(ctx, failed.Inode, verifyFH)
	got = make([]byte, len(reopened))
	if n, e := v.Read(ctx, failed.Inode, got, off, verifyFH); e != 0 || n != len(reopened) || string(got) != string(reopened) {
		t.Fatalf("read reopened writer data = (%d, %s, %q), want (%d, 0, %q)", n, e, got, len(reopened), reopened)
	}
}

// TestPloriDurabilityBarrierReportsAQuotaRefusalItDrains holds a commit until
// FlushAll has captured its error generation. The barrier that caused the
// ENOSPC must return it; after the rejected slice drains, the next barrier may
// proceed while the file's fsync still returns ENOSPC.
func TestPloriDurabilityBarrierReportsAQuotaRefusalItDrains(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ctx := NewLogContext(meta.Background())
	held := &heldQuotaWriteMeta{
		Meta:    v.Meta,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	v.writer.(*dataWriter).m = held
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(held.release) }) }
	t.Cleanup(release)

	fe, fh, e := v.Create(ctx, 1, "held-quota-refusal", 0644, 0, syscall.O_RDWR)
	if e != 0 {
		t.Fatalf("create: %s", e)
	}
	defer v.Release(ctx, fe.Inode, fh)
	defer release()
	if e := v.Write(ctx, fe.Inode, []byte("buffered until the barrier"), 0, fh); e != 0 {
		t.Fatalf("write: %s", e)
	}

	barrier := make(chan error, 1)
	go func() { barrier <- v.FlushAll("") }()
	select {
	case <-held.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("FlushAll did not start the held commit")
	}
	writer := v.writer.(*dataWriter).find(fe.Inode)
	flushDeadline := time.Now().Add(5 * time.Second)
	for {
		writer.Lock()
		flushing := writer.flushwaiting > 0
		writer.Unlock()
		if flushing {
			break
		}
		if time.Now().After(flushDeadline) {
			t.Fatal("the held commit did not run under FlushAll")
		}
		time.Sleep(time.Millisecond)
	}
	release()
	select {
	case err := <-barrier:
		if err != syscall.ENOSPC {
			t.Fatalf("barrier that drained the held commit = %v, want ENOSPC", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("FlushAll did not finish after the held commit was released")
	}
	if err := v.FlushAll(""); err != nil {
		t.Fatalf("barrier after the held refusal drained: %v", err)
	}
	if e := v.Fsync(ctx, fe.Inode, 1, fh); e != syscall.ENOSPC {
		t.Fatalf("fsync after the held refusal = %s, want ENOSPC", e)
	}
}

// TestPloriGrantCostsNoObjectRequests is the PLO-346 §8 gate's object-store
// half, and the number quota-allocator.md §8 was waiting for.
//
// It matters because the increment is sized against it: a grant application
// that cost object requests would argue for larger, rarer grants, and the
// 64 MiB increment would have to be defended rather than chosen. The
// measurement says the cost is zero — the ceiling lives in the metadata engine
// and the object store is not on the path at all.
//
// The check is a full listing rather than a count: an object rewritten in place
// keeps the count and changes the content, and either would be a cost.
func TestPloriGrantCostsNoObjectRequests(t *testing.T) {
	v, blob := createTestVFS(nil, "")
	ctx := NewLogContext(meta.Background())

	if err := meta.PloriApplyGrant(v.Meta, quotaTestCeiling, 16384); err != nil {
		t.Fatalf("set the starting ceiling: %s", err)
	}
	if st := fillToTheCeiling(t, v, ctx, "fills-the-grant"); st != syscall.ENOSPC {
		t.Fatalf("the full volume refused with %v, want ENOSPC", st)
	}

	// A refused commit uploads its block first and removes it afterwards, in a
	// goroutine (writer.go:214-217). Snapshotting before that DELETE lands
	// would charge the grant for the ENOSPC's own cleanup, so wait for the
	// store to stop moving first.
	before := settledObjects(t, blob)
	if len(before) == 0 {
		t.Fatal("the workload wrote no objects; the measurement below would be vacuous")
	}
	if err := meta.PloriApplyGrant(v.Meta, 256<<20, 65536); err != nil {
		t.Fatalf("apply grant: %s", err)
	}
	after := settledObjects(t, blob)

	if len(before) != len(after) {
		t.Fatalf("applying a grant changed the object count from %d to %d", len(before), len(after))
	}
	for i := range before {
		if before[i] != after[i] {
			t.Errorf("applying a grant touched object %d: %q -> %q", i, before[i], after[i])
		}
	}
	t.Logf("live grant application: 0 object requests over %d objects on the file backend", len(after))
}

// settledObjects returns the object listing once it has stopped changing. The
// data plane finishes work asynchronously — the writeback upload of a slice and
// the removal of a block whose commit was refused both outlive the syscall that
// started them — so a snapshot taken the instant a flush returns is a snapshot
// of a store still in motion.
func settledObjects(t *testing.T, blob object.ObjectStorage) []string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	prev := listObjects(t, blob)
	for {
		time.Sleep(50 * time.Millisecond)
		cur := listObjects(t, blob)
		if slices.Equal(prev, cur) {
			return cur
		}
		if time.Now().After(deadline) {
			t.Fatalf("the object store never settled: %d objects, still changing", len(cur))
		}
		prev = cur
	}
}

// listObjects returns every object in the `file` backend as "key:size", sorted.
// The Plori profile carries exactly two object backends and this is the one the
// test harness uses (testing_backends_plori_test.go), so reaching into its root
// directory is reading the store, not bypassing it.
func listObjects(t *testing.T, blob object.ObjectStorage) []string {
	t.Helper()
	root := strings.TrimPrefix(blob.String(), "file://")
	var out []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// The store is being written to while it is walked, so an entry
			// that vanished between the readdir and this callback is the
			// asynchronous data plane, not a broken store. settledObjects
			// below is what turns a moving listing into a stable one.
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		out = append(out, fmt.Sprintf("%s:%d", rel, fi.Size()))
		return nil
	})
	if err != nil {
		t.Fatalf("walk the object store at %s: %s", root, err)
	}
	sort.Strings(out)
	return out
}
