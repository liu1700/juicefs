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
	"os"
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
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

// erroringHeader models a store that answers something other than "not found".
type erroringHeader struct {
	inner object.ObjectStorage
	fail  error
	n     int
}

func (e *erroringHeader) Head(ctx context.Context, key string) (object.Object, error) {
	e.n++
	if e.n > 1 {
		return nil, e.fail
	}
	return e.inner.Head(ctx, key)
}

// TestScanFailsClosedOnHeadError is the difference from `juicefs fsck`, which
// logs a non-"not found" HEAD failure and carries on (cmd/fsck.go:245). A
// repair decision taken from a scan with holes in it would truncate healthy
// files.
func TestScanFailsClosedOnHeadError(t *testing.T) {
	const blockSize = 1 << 20
	v := newVolume(t, volumeOptions{
		trashDays:    1,
		blockSizeKiB: blockSize >> 10,
		files:        map[string]int{"/big.bin": 3 * blockSize},
	})

	m, _, closeFn := v.openMeta(t, v.metaPath)
	defer closeFn()

	store := &erroringHeader{inner: v.blocks(t), fail: errors.New("503 slow down")}
	_, err := ScanMissingBlocks(t.Context(), m, store,
		ScanOptions{Format: v.format, Concurrency: 1})
	if err == nil {
		t.Fatal("a HEAD failure that is not ErrNotExist must end the scan")
	}
	if Code(err) != CodeBlockMissingAfterRestore {
		t.Fatalf("got code %q (%v)", Code(err), err)
	}
	if !Retryable(err) {
		t.Fatal("a store-side failure should be retryable")
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
