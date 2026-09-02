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
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
)

// The write gate PLO-323 F-2 and F-5 install lives in pkg/meta, but what it has
// to be correct about is a VFS property: an Agent's write(2) never reaches
// meta.Write directly. It lands in a buffer, and meta.Write is the COMMIT of a
// slice the writer has already uploaded (pkg/vfs/writer.go:208). So the gate is
// what stops an already-open handle from committing — and it is also what the
// ordered shutdown's bounded flush depends on being open.
//
// Both facts are asserted here, through the real VFS, because neither is
// visible from the metadata engine alone and getting the second one wrong turns
// every clean stop with a dirty buffer into reported data loss.
//
// The tests drive the LEASE EXPIRY rather than PloriFenceWrites: the fence is
// deliberately one-way (nothing un-fences a writer, because the epoch it lost
// is never reissued), so tripping it here would leave every later test in this
// package running against a read-only filesystem. The expiry is the same gate,
// reached through the same predicate, and it can be moved back.
func TestPloriWriteGateStopsAnAlreadyOpenHandle(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ctx := NewLogContext(meta.Background())
	t.Cleanup(func() { meta.PloriSetWriteExpiry(time.Now().Add(time.Hour)) })

	meta.PloriSetWriteExpiry(time.Now().Add(time.Hour))
	fe, fh, e := v.Create(ctx, 1, "open-across-the-fence", 0755, 0, syscall.O_RDWR)
	if e != 0 {
		t.Fatalf("create: %s", e)
	}
	if e := v.Write(ctx, fe.Inode, []byte("written before the fence"), 0, fh); e != 0 {
		t.Fatalf("write: %s", e)
	}

	// The lease is gone. The handle is still open and the data is still
	// buffered; the commit must not happen.
	meta.PloriSetWriteExpiry(time.Now().Add(-time.Millisecond))
	if err := v.FlushAll(""); err == nil {
		t.Error("a handle opened before the write gate closed still committed its slices")
	}
	if e := v.Truncate(ctx, fe.Inode, 4096, fh, &meta.Attr{}); e != syscall.EROFS {
		t.Errorf("Truncate through an open handle = %s, want EROFS", e)
	}
}

// TestPloriOrderedStopCanStillDrain is the regression guard for the shutdown
// ordering that fix forced.
//
// The ordered stop reserves the write-stop margin for a bounded flush INSIDE
// the lease (threat-model.md §7.5): "flush did not complete inside the window"
// is data loss to be reported, never licence to write on. That flush commits
// slices through exactly the Write the fence now seals, so the seal cannot be
// step 1 of the stop — measured, sealing first makes FlushAll answer EIO and
// every clean SIGTERM with a dirty buffer exits 69.
//
// The supervisor therefore drains first and seals once the mount is detached
// (pkg/plori/mount/supervisor.go shutdown). This asserts the property that
// ordering relies on: inside the lease, with the write-stop margin reached but
// the lease not yet expired, the drain still completes.
func TestPloriOrderedStopCanStillDrain(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ctx := NewLogContext(meta.Background())
	t.Cleanup(func() { meta.PloriSetWriteExpiry(time.Now().Add(time.Hour)) })

	// A lease with its margin behind it but its expiry ahead: the exact window
	// the drain is funded from.
	meta.PloriSetWriteExpiry(time.Now().Add(30 * time.Second))
	fe, fh, e := v.Create(ctx, 1, "drained-inside-the-margin", 0755, 0, syscall.O_RDWR)
	if e != 0 {
		t.Fatalf("create: %s", e)
	}
	if e := v.Write(ctx, fe.Inode, []byte("staged, and owed to the store"), 0, fh); e != 0 {
		t.Fatalf("write: %s", e)
	}
	if err := v.FlushAll(""); err != nil {
		t.Fatalf("the bounded flush inside the lease failed: %v; "+
			"the ordered stop cannot seal before its barrier", err)
	}
}
