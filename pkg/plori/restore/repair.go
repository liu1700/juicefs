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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
)

// QuarantineXattr is the extended attribute a quarantined inode carries.
//
// The `trusted.` namespace is deliberate. The supervisor sets it through the
// metadata engine, which performs no namespace check, while an Agent reading
// or clearing it goes through FUSE, where the kernel restricts `trusted.*` to
// CAP_SYS_ADMIN (fs/xattr.c). So the marker is writable by us and read-only to
// the tenant, without a second enforcement point.
const QuarantineXattr = "trusted.plori.quarantine"

// Quarantine modes.
const (
	// ModeMarkOnly records the damage and changes no file content. Use it when
	// an operator will decide, or when the run is a drill.
	ModeMarkOnly QuarantineMode = "mark-only"
	// ModeTruncate additionally truncates each damaged file at the first byte
	// that a missing block would have supplied, so every byte the file still
	// reports is a byte that can actually be read.
	ModeTruncate QuarantineMode = "truncate"
)

// QuarantineMode selects what Quarantine does about the damage it is given.
type QuarantineMode string

// BlockRef identifies one block that metadata references and the object store
// does not hold. Chunk is the block's index inside the slice object, matching
// the `<slice-id>_<index>_<size>` key JuiceFS writes.
type BlockRef struct {
	Inode  meta.Ino `json:"inode"`
	Path   string   `json:"path,omitempty"`
	Slice  uint64   `json:"slice"`
	Chunk  uint32   `json:"chunk"`
	Key    string   `json:"key"`
	Size   int      `json:"size"`
	Offset uint64   `json:"offset"`
}

// SliceScanner is the slice of meta.Meta that ScanMissingBlocks needs.
// *meta.baseMeta (every engine) satisfies it.
type SliceScanner interface {
	ScanSlices(ctx meta.Context, opt *meta.ScanSlicesOption, fn func(meta.Ino, meta.Slice) error) syscall.Errno
	GetPaths(ctx meta.Context, inode meta.Ino) []string
}

// BlockHeader is the slice of object.ObjectStorage that ScanMissingBlocks
// needs. Pass object.WithPrefix(blob, "chunks/"), the same handle
// cmd/fsck.go:135 builds, so that Key is relative to the data prefix.
type BlockHeader interface {
	Head(ctx context.Context, key string) (object.Object, error)
}

// ScanOptions configures ScanMissingBlocks.
type ScanOptions struct {
	// Format supplies BlockSize and HashPrefix. Required.
	Format *meta.Format

	// MinSliceID is the repair watermark. Zero, the default, scans every
	// slice, which is the right choice: a full fsck over a realistic volume
	// costs 870 ms and 12 LIST operations on 11k objects
	// (benchmark-wave2-footprint.md Q3), and it is the only variant that
	// catches damage older than the generation being restored.
	//
	// A non-zero value scans only slices with Id >= MinSliceID. JuiceFS has no
	// per-slice transaction id, so this is the closest available proxy: slice
	// ids come from a monotonic counter in the metadata engine, therefore
	// "allocated after the durable point" implies "id at or above the id in
	// use at that point". It is an optimisation, never a correctness argument:
	// a lower id whose block upload was still in flight is missed.
	MinSliceID uint64

	// SkipInos are inodes whose missing blocks are expected: files already
	// queued for deletion. Build it with DeletedInos.
	SkipInos map[meta.Ino]bool

	// ScanPending includes slices from pending (uncommitted) chunks.
	ScanPending bool

	// Concurrency bounds parallel HEAD requests. Zero means 8.
	Concurrency int

	// Progress, when set, is called once per scanned slice.
	Progress func()
}

// ScanReport is the outcome of a scan.
type ScanReport struct {
	// Missing is sorted by (inode, slice, chunk) so two runs over the same
	// damage produce the same report.
	Missing []BlockRef `json:"missing"`
	// SlicesScanned counts slices considered after the watermark filter.
	SlicesScanned int `json:"slices_scanned"`
	// BlocksChecked counts HEAD requests issued.
	BlocksChecked int `json:"blocks_checked"`
	// InodesAffected counts distinct inodes in Missing.
	InodesAffected int `json:"inodes_affected"`
	// Duration is the wall time of the scan.
	Duration time.Duration `json:"duration"`
}

// ScanMissingBlocks enumerates every block the metadata references and asks
// the object store whether it exists.
//
// It mirrors cmd/fsck.go:172-245 rather than shelling out to `juicefs fsck`,
// with two deliberate differences: it never lists the whole data prefix (a
// LIST of every block is what makes path-scoped fsck 15x more expensive), and
// it fails closed. `juicefs fsck` logs a HEAD failure that is not
// "not found" and moves on; a repair decision taken from a scan that silently
// skipped blocks would truncate files that are fine, so any such failure ends
// the scan with an error and the supervisor retries.
func ScanMissingBlocks(ctx context.Context, m SliceScanner, store BlockHeader, opt ScanOptions) (*ScanReport, error) {
	if opt.Format == nil {
		return nil, newError(CodeBlockMissingAfterRestore, "format required for a block scan", false, nil)
	}
	blockSize := opt.Format.BlockSize << 10 // Format.BlockSize is in KiB
	if blockSize <= 0 {
		return nil, newError(CodeBlockMissingAfterRestore,
			fmt.Sprintf("format has an unusable block size: %d", opt.Format.BlockSize), false, nil)
	}
	concurrency := opt.Concurrency
	if concurrency <= 0 {
		concurrency = 8
	}

	started := time.Now()
	report := &ScanReport{}

	type candidate struct {
		ino   meta.Ino
		slice meta.Slice
	}
	var candidates []candidate
	mctx := meta.Background()
	st := m.ScanSlices(mctx, &meta.ScanSlicesOption{
		ScanPending: opt.ScanPending,
		Progress:    opt.Progress,
	}, func(ino meta.Ino, s meta.Slice) error {
		if s.Id == 0 || s.Size == 0 {
			return nil
		}
		if s.Id < opt.MinSliceID {
			return nil
		}
		if opt.SkipInos[ino] {
			return nil
		}
		candidates = append(candidates, candidate{ino: ino, slice: s})
		return nil
	})
	if st != 0 {
		return nil, newError(CodeBlockMissingAfterRestore, "scan slices", true, st)
	}
	report.SlicesScanned = len(candidates)

	var (
		mu      sync.Mutex
		missing []BlockRef
		checked int
		scanErr error
		wg      sync.WaitGroup
	)
	work := make(chan candidate)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range work {
				refs, n, err := headSlice(ctx, store, c.ino, c.slice, blockSize, opt.Format.HashPrefix)
				mu.Lock()
				checked += n
				missing = append(missing, refs...)
				if err != nil && scanErr == nil {
					scanErr = err
				}
				mu.Unlock()
			}
		}()
	}
	for _, c := range candidates {
		select {
		case <-ctx.Done():
			close(work)
			wg.Wait()
			return nil, newError(CodeBlockMissingAfterRestore, "scan cancelled", true, ctx.Err())
		case work <- c:
		}
	}
	close(work)
	wg.Wait()

	if scanErr != nil {
		return nil, scanErr
	}

	sort.Slice(missing, func(i, j int) bool {
		if missing[i].Inode != missing[j].Inode {
			return missing[i].Inode < missing[j].Inode
		}
		if missing[i].Slice != missing[j].Slice {
			return missing[i].Slice < missing[j].Slice
		}
		return missing[i].Chunk < missing[j].Chunk
	})

	inodes := make(map[meta.Ino]bool, len(missing))
	for i := range missing {
		if !inodes[missing[i].Inode] {
			inodes[missing[i].Inode] = true
			if paths := m.GetPaths(mctx, missing[i].Inode); len(paths) > 0 {
				missing[i].Path = paths[0]
			}
		}
	}
	// Fill the path in for the remaining refs of an inode we already resolved.
	paths := make(map[meta.Ino]string, len(inodes))
	for i := range missing {
		if missing[i].Path != "" {
			paths[missing[i].Inode] = missing[i].Path
		}
	}
	for i := range missing {
		if missing[i].Path == "" {
			missing[i].Path = paths[missing[i].Inode]
		}
	}

	report.Missing = missing
	report.BlocksChecked = checked
	report.InodesAffected = len(inodes)
	report.Duration = time.Since(started)
	return report, nil
}

// headSlice HEADs every block of one slice and returns the refs that are gone.
func headSlice(ctx context.Context, store BlockHeader, ino meta.Ino, s meta.Slice, blockSize int, hashPrefix bool) ([]BlockRef, int, error) {
	var (
		refs    []BlockRef
		checked int
	)
	n := (s.Size - 1) / uint32(blockSize)
	for i := uint32(0); i <= n; i++ {
		size := blockSize
		if i == n {
			size = int(s.Size) - int(i)*blockSize
		}
		key := blockKey(s.Id, i, size, hashPrefix)
		checked++
		if _, err := store.Head(ctx, key); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return refs, checked, newError(CodeBlockMissingAfterRestore,
					"HEAD block "+key, true, err)
			}
			refs = append(refs, BlockRef{
				Inode:  ino,
				Slice:  s.Id,
				Chunk:  i,
				Key:    key,
				Size:   size,
				Offset: uint64(i) * uint64(blockSize),
			})
		}
	}
	return refs, checked, nil
}

// blockKey mirrors cmd/fsck.go:229-235. The returned key is relative to the
// "chunks/" prefix.
func blockKey(id uint64, index uint32, size int, hashPrefix bool) string {
	name := fmt.Sprintf("%d_%d_%d", id, index, size)
	if hashPrefix {
		return fmt.Sprintf("%02X/%v/%s", id%256, id/1000/1000, name)
	}
	return fmt.Sprintf("%v/%v/%s", id/1000/1000, id/1000, name)
}

// DeletedInos collects the inodes JuiceFS already queued for deletion, whose
// blocks are allowed to be gone. It takes the full meta.Meta because
// ScanDeletedObject's callback types are unexported and cannot appear in an
// interface declared outside pkg/meta; the body is the same call
// cmd/fsck.go:164-170 makes.
func DeletedInos(ctx meta.Context, m meta.Meta) (map[meta.Ino]bool, error) {
	inos := make(map[meta.Ino]bool)
	err := m.ScanDeletedObject(ctx, nil, nil, nil,
		func(ino meta.Ino, size uint64, ts int64) (clean bool, err error) {
			inos[ino] = true
			return false, nil
		})
	if err != nil {
		return nil, err
	}
	return inos, nil
}

// Quarantiner is the slice of meta.Meta that Quarantine needs.
type Quarantiner interface {
	GetAttr(ctx meta.Context, inode meta.Ino, attr *meta.Attr) syscall.Errno
	Read(ctx meta.Context, inode meta.Ino, indx uint32, slices *[]meta.Slice) syscall.Errno
	Truncate(ctx meta.Context, inode meta.Ino, flags uint8, attrlength uint64, attr *meta.Attr, skipPermCheck bool) syscall.Errno
	SetXattr(ctx meta.Context, inode meta.Ino, name string, value []byte, flags uint32) syscall.Errno
	GetPaths(ctx meta.Context, inode meta.Ino) []string
}

// QuarantineEntry is what happened to one damaged inode.
type QuarantineEntry struct {
	Inode          meta.Ino   `json:"inode"`
	Path           string     `json:"path,omitempty"`
	Code           string     `json:"code"`
	Blocks         []BlockRef `json:"blocks"`
	OriginalLength uint64     `json:"original_length"`
	TruncatedTo    *uint64    `json:"truncated_to,omitempty"`
	Marked         bool       `json:"marked"`
	// Reason explains a missing truncation: the inode was already shorter than
	// the damage, the offset could not be located, or the mode was mark-only.
	Reason string `json:"reason,omitempty"`
}

// QuarantineReport is the durable record of one repair pass. Quarantine
// returns it rather than writing it anywhere: the supervisor persists it next
// to the mount state and reports it to the control plane. Writing a manifest
// into the volume itself would need data-plane writes during exactly the
// window where the data plane is known to be damaged, would land in the
// tenant's namespace, and would be deletable by the tenant.
type QuarantineReport struct {
	Mode    QuarantineMode    `json:"mode"`
	At      time.Time         `json:"at"`
	Entries []QuarantineEntry `json:"entries"`
}

// marker is the JSON stored in the xattr.
type marker struct {
	Code        string     `json:"code"`
	At          time.Time  `json:"at"`
	TruncatedTo *uint64    `json:"truncated_to,omitempty"`
	Blocks      []BlockRef `json:"blocks"`
}

// Quarantine records, and optionally bounds, the damage ScanMissingBlocks
// found. It never deletes a file and never removes metadata.
//
// In ModeTruncate a damaged file is cut at the first byte a missing block
// would have supplied, so a read that used to return EIO in the middle of an
// apparently healthy file becomes a short file whose every byte is readable.
// If that offset cannot be located the entry degrades to mark-only rather than
// guessing: an over-eager truncation destroys data that is still there.
func Quarantine(ctx context.Context, m Quarantiner, records []BlockRef, mode QuarantineMode, format *meta.Format) (*QuarantineReport, error) {
	switch mode {
	case ModeMarkOnly, ModeTruncate:
	default:
		return nil, newError(CodeBlockMissingAfterRestore,
			"unknown quarantine mode "+string(mode), false, nil)
	}
	if mode == ModeTruncate && format == nil {
		return nil, newError(CodeBlockMissingAfterRestore,
			"format required to compute a truncation offset", false, nil)
	}

	report := &QuarantineReport{Mode: mode, At: time.Now().UTC()}
	if len(records) == 0 {
		return report, nil
	}

	byInode := make(map[meta.Ino][]BlockRef)
	order := make([]meta.Ino, 0, len(records))
	for _, r := range records {
		if _, ok := byInode[r.Inode]; !ok {
			order = append(order, r.Inode)
		}
		byInode[r.Inode] = append(byInode[r.Inode], r)
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })

	mctx := meta.Background()
	for _, ino := range order {
		blocks := byInode[ino]
		entry := QuarantineEntry{
			Inode:  ino,
			Code:   CodeBlockMissingAfterRestore,
			Blocks: blocks,
			Path:   blocks[0].Path,
		}
		if entry.Path == "" {
			if paths := m.GetPaths(mctx, ino); len(paths) > 0 {
				entry.Path = paths[0]
			}
		}

		var attr meta.Attr
		if st := m.GetAttr(mctx, ino, &attr); st != 0 {
			if errors.Is(st, syscall.ENOENT) {
				entry.Reason = "inode no longer exists"
				report.Entries = append(report.Entries, entry)
				continue
			}
			return nil, newError(CodeBlockMissingAfterRestore,
				fmt.Sprintf("getattr inode %d", ino), true, st)
		}
		entry.OriginalLength = attr.Length

		if mode == ModeTruncate {
			off, ok := firstMissingOffset(mctx, m, ino, attr.Length, format, blocks)
			switch {
			case !ok:
				entry.Reason = "first missing byte could not be located; left intact"
			case off >= attr.Length:
				entry.Reason = "damage lies past the end of the file"
			default:
				var out meta.Attr
				if st := m.Truncate(mctx, ino, 0, off, &out, true); st != 0 {
					return nil, newError(CodeBlockMissingAfterRestore,
						fmt.Sprintf("truncate inode %d to %d", ino, off), true, st)
				}
				truncated := off
				entry.TruncatedTo = &truncated
			}
		} else {
			entry.Reason = "mark-only"
		}

		payload, err := json.Marshal(marker{
			Code:        CodeBlockMissingAfterRestore,
			At:          report.At,
			TruncatedTo: entry.TruncatedTo,
			Blocks:      blocks,
		})
		if err != nil {
			return nil, newError(CodeBlockMissingAfterRestore, "encode quarantine marker", false, err)
		}
		if st := m.SetXattr(mctx, ino, QuarantineXattr, payload, meta.XattrCreateOrReplace); st != 0 {
			return nil, newError(CodeBlockMissingAfterRestore,
				fmt.Sprintf("mark inode %d", ino), true, st)
		}
		entry.Marked = true
		report.Entries = append(report.Entries, entry)
	}
	return report, nil
}

// firstMissingOffset locates the file offset of the first byte that a missing
// block would have supplied.
//
// meta.Read returns each chunk as an ordered, gap-free cover
// (pkg/meta/slice.go:144-153), so a running position over Len gives the file
// offset of every slice entry, and Off is the entry's start inside the slice
// object. The first byte of block b that this entry actually references is
// therefore max(b*BlockSize, Off), at file offset
// indx*ChunkSize + pos + max(b*BlockSize, Off) - Off.
func firstMissingOffset(ctx meta.Context, m Quarantiner, ino meta.Ino, length uint64, format *meta.Format, blocks []BlockRef) (uint64, bool) {
	blockSize := uint64(format.BlockSize) << 10
	if blockSize == 0 {
		return 0, false
	}
	gone := make(map[uint64]map[uint32]bool)
	for _, b := range blocks {
		if gone[b.Slice] == nil {
			gone[b.Slice] = make(map[uint32]bool)
		}
		gone[b.Slice][b.Chunk] = true
	}

	for indx := uint64(0); indx*meta.ChunkSize < length; indx++ {
		var slices []meta.Slice
		if st := m.Read(ctx, ino, uint32(indx), &slices); st != 0 {
			return 0, false
		}
		var pos uint64
		for _, s := range slices {
			if s.Id != 0 {
				if idx, ok := gone[s.Id]; ok {
					start := uint64(s.Off)
					end := start + uint64(s.Len)
					best := uint64(0)
					found := false
					for b := range idx {
						bs := uint64(b) * blockSize
						be := bs + blockSize
						if be <= start || bs >= end {
							continue // this block is outside the referenced range
						}
						at := bs
						if at < start {
							at = start
						}
						if !found || at < best {
							best, found = at, true
						}
					}
					if found {
						return indx*meta.ChunkSize + pos + (best - start), true
					}
				}
			}
			pos += uint64(s.Len)
		}
	}
	return 0, false
}
