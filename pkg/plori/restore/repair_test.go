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

package restore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/utils"
)

func testCtx() meta.Context {
	return meta.NewContext(uint32(os.Getpid()), 0, []uint32{0})
}

func lookup(t *testing.T, m meta.Meta, name string) meta.Ino {
	t.Helper()
	var ino meta.Ino
	var attr meta.Attr
	if st := m.Lookup(testCtx(), meta.RootInode, name, &ino, &attr, false); st != 0 {
		t.Fatalf("lookup %s: %s", name, st)
	}
	return ino
}

// blockCovering returns the block object that supplies the byte at file offset
// `off`. It walks the chunk the way pkg/meta/slice.go:144-153 lays it out: an
// ordered, gap-free cover whose entries carry their offset inside the slice
// object.
func blockCovering(t *testing.T, m meta.Meta, ino meta.Ino, off uint64, blockSize int, hashPrefix bool) BlockRef {
	t.Helper()

	indx := uint32(off / meta.ChunkSize)
	var slices []meta.Slice
	if st := m.Read(testCtx(), ino, indx, &slices); st != 0 {
		t.Fatalf("read chunk %d: %s", indx, st)
	}
	want := off % meta.ChunkSize
	var pos uint64
	for _, s := range slices {
		if want < pos+uint64(s.Len) {
			if s.Id == 0 {
				t.Fatalf("offset %d falls in a hole", off)
			}
			inSlice := uint64(s.Off) + (want - pos)
			index := uint32(inSlice / uint64(blockSize))
			size := blockSize
			if n := (s.Size - 1) / uint32(blockSize); index == n {
				size = int(s.Size) - int(index)*blockSize
			}
			return BlockRef{
				Inode:  ino,
				Slice:  s.Id,
				Chunk:  index,
				Key:    blockKey(s.Id, index, size, hashPrefix),
				Size:   size,
				Offset: uint64(index) * uint64(blockSize),
			}
		}
		pos += uint64(s.Len)
	}
	t.Fatalf("offset %d is past the end of chunk %d", off, indx)
	return BlockRef{}
}

// TestRepairMissingBlockTruncates is crash-consistency.md 7 d3 end to end:
// a restore lands on metadata that references a block the object store never
// received, and the file must stop being a stat-ok/read-EIO trap.
func TestRepairMissingBlockTruncates(t *testing.T) {
	const blockSize = 1 << 20
	v := newVolume(t, volumeOptions{
		trashDays:    1,
		blockSizeKiB: blockSize >> 10,
		files:        map[string]int{"/big.bin": 3 * blockSize},
	})

	m, _, closeFn := v.openMeta(t, v.metaPath)
	ino := lookup(t, m, "big.bin")
	target := blockCovering(t, m, ino, blockSize, blockSize, v.format.HashPrefix)

	// A clean volume must report nothing. Running the scan before and after
	// the damage is what makes the "exactly it" assertion meaningful.
	clean, err := ScanMissingBlocks(t.Context(), m, v.blocks(t), ScanOptions{Format: v.format})
	if err != nil {
		t.Fatalf("scan a clean volume: %v", err)
	}
	if len(clean.Missing) != 0 {
		t.Fatalf("clean volume reported %d missing blocks: %+v", len(clean.Missing), clean.Missing)
	}
	if clean.BlocksChecked < 3 {
		t.Fatalf("only %d blocks were checked; the fixture should have at least 3", clean.BlocksChecked)
	}

	// Delete one block behind JuiceFS's back, exactly the shape a
	// kill-before-upload leaves after a Litestream restore.
	if err := os.Remove(v.blockPath(target.Key)); err != nil {
		t.Fatalf("remove block %s: %v", target.Key, err)
	}

	report, err := ScanMissingBlocks(t.Context(), m, v.blocks(t), ScanOptions{Format: v.format})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(report.Missing) != 1 {
		t.Fatalf("scan found %d missing blocks, want exactly 1: %+v", len(report.Missing), report.Missing)
	}
	got := report.Missing[0]
	if got.Key != target.Key || got.Inode != ino || got.Slice != target.Slice || got.Chunk != target.Chunk {
		t.Fatalf("scan found %+v, want %+v", got, target)
	}
	if got.Path != "/big.bin" {
		t.Fatalf("missing block path = %q, want /big.bin", got.Path)
	}
	if report.InodesAffected != 1 {
		t.Fatalf("InodesAffected = %d, want 1", report.InodesAffected)
	}

	qr, err := Quarantine(t.Context(), m, report.Missing, ModeTruncate, v.format)
	if err != nil {
		t.Fatalf("quarantine: %v", err)
	}
	if len(qr.Entries) != 1 {
		t.Fatalf("quarantine produced %d entries, want 1", len(qr.Entries))
	}
	entry := qr.Entries[0]
	if entry.Code != CodeBlockMissingAfterRestore {
		t.Fatalf("entry code = %q, want %s", entry.Code, CodeBlockMissingAfterRestore)
	}
	if !entry.Marked {
		t.Fatal("the inode was not marked")
	}
	if entry.TruncatedTo == nil {
		t.Fatalf("the file was not truncated: %+v", entry)
	}
	if *entry.TruncatedTo != blockSize {
		t.Fatalf("truncated to %d, want %d", *entry.TruncatedTo, blockSize)
	}
	if entry.OriginalLength != 3*blockSize {
		t.Fatalf("original length = %d, want %d", entry.OriginalLength, 3*blockSize)
	}

	// The marker must survive as an xattr the tenant cannot forge.
	var raw []byte
	if st := m.GetXattr(testCtx(), ino, QuarantineXattr, &raw); st != 0 {
		t.Fatalf("get %s: %s", QuarantineXattr, st)
	}
	var mk marker
	if err := json.Unmarshal(raw, &mk); err != nil {
		t.Fatalf("decode marker %q: %v", raw, err)
	}
	if mk.Code != CodeBlockMissingAfterRestore || len(mk.Blocks) != 1 {
		t.Fatalf("marker = %+v", mk)
	}
	closeFn()

	// Reopen so the read goes through a cold reader, then prove the file is
	// readable to the boundary and no further.
	_, jfs, closeFn2 := v.openMeta(t, v.metaPath)
	defer closeFn2()
	data, err := readAll(t, jfs, "/big.bin")
	if err != nil {
		t.Fatalf("read the quarantined file: %v", err)
	}
	if len(data) != blockSize {
		t.Fatalf("read %d bytes, want %d", len(data), blockSize)
	}
	if !bytes.Equal(data, v.files["/big.bin"][:blockSize]) {
		t.Fatal("the bytes before the boundary changed")
	}
}

func TestQuarantineMarkOnlyLeavesTheFileAlone(t *testing.T) {
	const blockSize = 1 << 20
	v := newVolume(t, volumeOptions{
		trashDays:    1,
		blockSizeKiB: blockSize >> 10,
		files:        map[string]int{"/big.bin": 2 * blockSize},
	})

	m, _, closeFn := v.openMeta(t, v.metaPath)
	defer closeFn()
	ino := lookup(t, m, "big.bin")
	target := blockCovering(t, m, ino, blockSize, blockSize, v.format.HashPrefix)
	if err := os.Remove(v.blockPath(target.Key)); err != nil {
		t.Fatal(err)
	}

	report, err := ScanMissingBlocks(t.Context(), m, v.blocks(t), ScanOptions{Format: v.format})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	qr, err := Quarantine(t.Context(), m, report.Missing, ModeMarkOnly, nil)
	if err != nil {
		t.Fatalf("quarantine: %v", err)
	}
	if len(qr.Entries) != 1 || qr.Entries[0].TruncatedTo != nil {
		t.Fatalf("mark-only must not truncate: %+v", qr.Entries)
	}
	if !qr.Entries[0].Marked {
		t.Fatal("mark-only must still mark")
	}

	var attr meta.Attr
	if st := m.GetAttr(testCtx(), ino, &attr); st != 0 {
		t.Fatalf("getattr: %s", st)
	}
	if attr.Length != 2*blockSize {
		t.Fatalf("length = %d, want %d", attr.Length, 2*blockSize)
	}
}

// TestScanRespectsWatermark proves MinSliceID filters, so a supervisor that
// trusts a durable watermark scans less.
func TestScanRespectsWatermark(t *testing.T) {
	const blockSize = 1 << 20
	v := newVolume(t, volumeOptions{
		trashDays:    1,
		blockSizeKiB: blockSize >> 10,
		files:        map[string]int{"/big.bin": 2 * blockSize},
	})

	m, _, closeFn := v.openMeta(t, v.metaPath)
	defer closeFn()
	ino := lookup(t, m, "big.bin")
	target := blockCovering(t, m, ino, blockSize, blockSize, v.format.HashPrefix)
	if err := os.Remove(v.blockPath(target.Key)); err != nil {
		t.Fatal(err)
	}

	above, err := ScanMissingBlocks(t.Context(), m, v.blocks(t),
		ScanOptions{Format: v.format, MinSliceID: target.Slice + 1})
	if err != nil {
		t.Fatalf("scan above the watermark: %v", err)
	}
	if len(above.Missing) != 0 {
		t.Fatalf("a watermark above the damage should skip it, got %+v", above.Missing)
	}

	at, err := ScanMissingBlocks(t.Context(), m, v.blocks(t),
		ScanOptions{Format: v.format, MinSliceID: target.Slice})
	if err != nil {
		t.Fatalf("scan at the watermark: %v", err)
	}
	if len(at.Missing) != 1 {
		t.Fatalf("a watermark at the damage should find it, got %+v", at.Missing)
	}
}

// inventoryStore wraps a real object store so tests can control page responses
// and prove the repair never falls back to a per-block Head call.
type inventoryStore struct {
	object.ObjectStorage
	list      func(context.Context, string, string, string, string, int64, bool) ([]object.Object, bool, string, error)
	heads     int
	lists     int
	headDelay time.Duration
}

func (s *inventoryStore) List(ctx context.Context, prefix, marker, token, delimiter string, limit int64, followLink bool) ([]object.Object, bool, string, error) {
	s.lists++
	if s.list != nil {
		return s.list(ctx, prefix, marker, token, delimiter, limit, followLink)
	}
	return s.ObjectStorage.List(ctx, prefix, marker, token, delimiter, limit, followLink)
}

func (s *inventoryStore) Head(ctx context.Context, _ string) (object.Object, error) {
	s.heads++
	if s.headDelay > 0 {
		select {
		case <-time.After(s.headDelay):
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, errors.New("the inventory scan must not call Head")
}

// TestScanUsesACompleteInventoryAndNeverHeads proves present objects come from
// the one complete listing, not a request per referenced block.
func TestScanUsesACompleteInventoryAndNeverHeads(t *testing.T) {
	const blockSize = 1 << 20
	v := newVolume(t, volumeOptions{
		trashDays:    1,
		blockSizeKiB: blockSize >> 10,
		files:        map[string]int{"/big.bin": 3 * blockSize},
	})

	m, _, closeFn := v.openMeta(t, v.metaPath)
	defer closeFn()

	store := &inventoryStore{ObjectStorage: v.blocks(t)}
	report, err := ScanMissingBlocks(t.Context(), m, store, ScanOptions{Format: v.format})
	if err != nil {
		t.Fatalf("scan complete inventory: %v", err)
	}
	if len(report.Missing) != 0 {
		t.Fatalf("missing = %+v, want none", report.Missing)
	}
	if store.lists < 2 || store.heads != 0 {
		t.Fatalf("list/head calls = %d/%d, want file fallback listings and 0 Heads", store.lists, store.heads)
	}
}

func TestScanFailsClosedWhenInventoryCannotStart(t *testing.T) {
	v := newVolume(t, volumeOptions{trashDays: 1, files: map[string]int{"/f": 1}})
	m, _, closeFn := v.openMeta(t, v.metaPath)
	defer closeFn()
	want := errors.New("listing unavailable")
	store := &inventoryStore{ObjectStorage: v.blocks(t), list: func(context.Context, string, string, string, string, int64, bool) ([]object.Object, bool, string, error) {
		return nil, false, "", want
	}}
	_, err := ScanMissingBlocks(t.Context(), m, store, ScanOptions{Format: v.format})
	if Code(err) != CodeBlockScanFailed || !errors.Is(err, want) || !Retryable(err) {
		t.Fatalf("error = %v, want retryable inventory failure wrapping %v", err, want)
	}
}

func TestScanFailsClosedOnIncompleteInventoryPage(t *testing.T) {
	v := newVolume(t, volumeOptions{trashDays: 1, files: map[string]int{"/f": 1}})
	m, _, closeFn := v.openMeta(t, v.metaPath)
	defer closeFn()
	store := &inventoryStore{ObjectStorage: v.blocks(t), list: func(context.Context, string, string, string, string, int64, bool) ([]object.Object, bool, string, error) {
		return []object.Object{nil}, false, "", nil
	}}
	_, err := ScanMissingBlocks(t.Context(), m, store, ScanOptions{Format: v.format})
	if Code(err) != CodeBlockScanFailed || !Retryable(err) {
		t.Fatalf("error = %v, want retryable incomplete-inventory failure", err)
	}
}

type syntheticSliceScanner struct{ count int }

func (s syntheticSliceScanner) ScanSlices(_ meta.Context, _ *meta.ScanSlicesOption, fn func(meta.Ino, meta.Slice) error) syscall.Errno {
	for i := 0; i < s.count; i++ {
		if err := fn(meta.Ino(i+2), meta.Slice{Id: uint64(i + 1), Size: 1024}); err != nil {
			return syscall.ECANCELED
		}
	}
	return 0
}

func (syntheticSliceScanner) GetPaths(meta.Context, meta.Ino) []string { return nil }

type trackingSliceScanner struct{ calls int }

func (s *trackingSliceScanner) ScanSlices(meta.Context, *meta.ScanSlicesOption, func(meta.Ino, meta.Slice) error) syscall.Errno {
	s.calls++
	return 0
}

func (*trackingSliceScanner) GetPaths(meta.Context, meta.Ino) []string { return nil }

type cancelingSliceScanner struct{ cancel context.CancelFunc }

func (s cancelingSliceScanner) ScanSlices(meta.Context, *meta.ScanSlicesOption, func(meta.Ino, meta.Slice) error) syscall.Errno {
	s.cancel()
	return 0
}

func (cancelingSliceScanner) GetPaths(meta.Context, meta.Ino) []string { return nil }

func TestCancelledSliceScanCannotReturnAnEmptySuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &inventoryStore{list: func(context.Context, string, string, string, string, int64, bool) ([]object.Object, bool, string, error) {
		return nil, false, "", nil
	}}
	_, err := ScanMissingBlocks(ctx, cancelingSliceScanner{cancel: cancel}, store,
		ScanOptions{Format: &meta.Format{BlockSize: 1}})
	if Code(err) != CodeBlockScanFailed || !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want cancelled slice scan failure", err)
	}
}

type syntheticObject struct {
	key string
	dir bool
}

func (o syntheticObject) Key() string        { return o.key }
func (syntheticObject) Size() int64          { return 1024 }
func (syntheticObject) Mtime() time.Time     { return time.Time{} }
func (o syntheticObject) IsDir() bool        { return o.dir }
func (syntheticObject) IsSymlink() bool      { return false }
func (syntheticObject) StorageClass() string { return "" }
func (syntheticObject) Status() string       { return "" }

func TestScanLargeInventoryUsesPaginatedListingAndNoHeads(t *testing.T) {
	const count = 13083
	store := &inventoryStore{list: func(_ context.Context, prefix, marker, token, delimiter string, limit int64, followLink bool) ([]object.Object, bool, string, error) {
		if prefix != "" || delimiter != "" || limit != inventoryPageSize || !followLink {
			t.Fatalf("list args prefix=%q delimiter=%q limit=%d follow=%v", prefix, delimiter, limit, followLink)
		}
		start := 1
		switch token {
		case "":
		case "page-2":
			if marker != blockKey(10000, 0, 1024, false) {
				t.Fatalf("page-two marker = %q", marker)
			}
			start = 10001
		default:
			t.Fatalf("unexpected continuation token %q", token)
		}
		end := start + 10000
		if end > count+1 {
			end = count + 1
		}
		objects := make([]object.Object, 0, end-start)
		for i := start; i < end; i++ {
			objects = append(objects, syntheticObject{key: blockKey(uint64(i), 0, 1024, false)})
		}
		if end <= count {
			return objects, true, "page-2", nil
		}
		return objects, false, "", nil
	}, headDelay: 20 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	report, err := ScanMissingBlocks(ctx, syntheticSliceScanner{count: count}, store,
		ScanOptions{Format: &meta.Format{BlockSize: 1}})
	if err != nil {
		t.Fatalf("large scan: %v", err)
	}
	if report.BlocksChecked != count || len(report.Missing) != 0 {
		t.Fatalf("checked/missing = %d/%d, want %d/0", report.BlocksChecked, len(report.Missing), count)
	}
	if store.lists != 2 || store.heads != 0 {
		t.Fatalf("list/head calls = %d/%d, want 2/0", store.lists, store.heads)
	}
}

func TestScanFailsClosedOnLaterInventoryPage(t *testing.T) {
	for _, directory := range []bool{false, true} {
		for _, cause := range []error{errors.New("page unavailable"), os.ErrPermission, utils.ErrNotSUP, context.Canceled} {
			t.Run(fmt.Sprintf("directory=%t/%v", directory, cause), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				calls := 0
				store := &inventoryStore{list: func(_ context.Context, prefix, marker, token, delimiter string, _ int64, _ bool) ([]object.Object, bool, string, error) {
					calls++
					if directory && delimiter == "" {
						return nil, false, "", utils.ErrNotSUP
					}
					if directory && prefix == "" {
						return []object.Object{syntheticObject{key: "1/", dir: true}}, false, "", nil
					}
					if marker == "" {
						return []object.Object{syntheticObject{key: "1/0001_0_1024"}}, true, "page-2", nil
					}
					if token != "page-2" || marker != "1/0001_0_1024" {
						t.Fatalf("unexpected continuation marker=%q token=%q", marker, token)
					}
					if errors.Is(cause, context.Canceled) {
						cancel()
					}
					return nil, false, "", cause
				}}
				scanner := &trackingSliceScanner{}
				report, err := ScanMissingBlocks(ctx, scanner, store, ScanOptions{Format: &meta.Format{BlockSize: 1}})
				if Code(err) != CodeBlockScanFailed || !errors.Is(err, cause) || !Retryable(err) || errors.Is(err, ErrBlockMissing) {
					t.Fatalf("error = %v, want retryable page-two failure wrapping %v, not confirmed damage", err, cause)
				}
				wantCalls := 2
				if directory {
					wantCalls = 4
				}
				if report != nil || scanner.calls != 0 || calls != wantCalls {
					t.Fatalf("report/scans/list calls = %v/%d/%d, want nil/0/%d", report, scanner.calls, calls, wantCalls)
				}
			})
		}
	}
}

func TestQuarantineRejectsUnknownMode(t *testing.T) {
	_, err := Quarantine(t.Context(), nil, []BlockRef{{Inode: 2}}, QuarantineMode("delete"), nil)
	if err == nil {
		t.Fatal("an unknown mode must be refused")
	}
}

func TestQuarantineWithNoRecordsIsANoop(t *testing.T) {
	report, err := Quarantine(t.Context(), nil, nil, ModeTruncate, &meta.Format{BlockSize: 1024})
	if err != nil {
		t.Fatalf("quarantine: %v", err)
	}
	if len(report.Entries) != 0 {
		t.Fatalf("entries = %+v", report.Entries)
	}
}

func TestBlockKeyMatchesFsck(t *testing.T) {
	// The two shapes cmd/fsck.go:230-234 builds.
	if got := blockKey(1234567, 2, 4194304, false); got != "1/1234/1234567_2_4194304" {
		t.Fatalf("flat key = %q", got)
	}
	if got := blockKey(1234567, 2, 4194304, true); got != "87/1/1234567_2_4194304" {
		t.Fatalf("hashed key = %q", got)
	}
	_ = syscall.Errno(0)
}

func TestMissingBlocksRequiresTheCompleteKey(t *testing.T) {
	slice := meta.Slice{Id: 1234567, Size: 1024}
	key := blockKey(slice.Id, 0, 1024, true)
	wrongPrefix := map[string]struct{}{"00/1/1234567_0_1024": {}}
	if refs, _ := missingBlocks(2, slice, 1024, true, wrongPrefix); len(refs) != 1 {
		t.Fatalf("wrong-prefix inventory satisfied %q: %+v", key, refs)
	}
	if refs, _ := missingBlocks(2, slice, 1024, true, map[string]struct{}{key: {}}); len(refs) != 0 {
		t.Fatalf("complete-key inventory reported missing: %+v", refs)
	}
}
