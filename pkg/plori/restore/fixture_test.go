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
	crand "crypto/rand"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/fs"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/version"
	"github.com/juicedata/juicefs/pkg/vfs"
)

// A Mac cannot FUSE-mount a JuiceFS volume, so these tests build one through
// the SDK write path instead. That is not a workaround for the thing under
// test: the SDK path writes the same SQLite metadata and the same block
// objects a mount would, which is exactly what restore and repair operate on.
// The technique follows bench/storage/offline-reader/fixture.go on
// project/per-agent-juicefs.

type volume struct {
	dir       string
	metaPath  string
	metaURL   string
	bucketDir string // "file" storage root
	format    *meta.Format
	files     map[string][]byte
	blockSize int // bytes
}

type volumeOptions struct {
	name         string
	trashDays    int
	blockSizeKiB int
	files        map[string]int // path -> size in bytes
}

// newVolume formats a fresh SQLite-backed volume on a file object store and
// writes the requested files. The metadata database is fully checkpointed on
// return, so the caller can copy or replicate it.
func newVolume(t *testing.T, opt volumeOptions) *volume {
	t.Helper()

	if opt.name == "" {
		opt.name = "plo320"
	}
	if opt.blockSizeKiB == 0 {
		opt.blockSizeKiB = 1024 // 1 MiB blocks keep the fixtures small
	}

	dir := t.TempDir()
	v := &volume{
		dir:       dir,
		metaPath:  filepath.Join(dir, "meta.db"),
		bucketDir: filepath.Join(dir, "data") + string(os.PathSeparator),
		files:     map[string][]byte{},
		blockSize: opt.blockSizeKiB << 10,
	}
	v.metaURL = "sqlite3://" + v.metaPath

	format := &meta.Format{
		Name:        opt.name,
		UUID:        newTestUUID(t),
		Storage:     "file",
		Bucket:      v.bucketDir,
		BlockSize:   opt.blockSizeKiB,
		MetaVersion: meta.MaxVersion,
		TrashDays:   opt.trashDays,
		DirStats:    true,
	}

	mc := meta.DefaultConf()
	mc.NoBGJob = true
	mc.Heartbeat = 0
	mc.AtimeMode = meta.NoAtime
	m := meta.NewClient(v.metaURL, mc)
	if err := m.Init(format, false); err != nil {
		t.Fatalf("init format: %v", err)
	}
	loaded, err := m.Load(true)
	if err != nil {
		t.Fatalf("load format: %v", err)
	}
	v.format = loaded

	blob := v.storage(t)
	if err := blob.Create(t.Context()); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	// `juicefs format` writes this object into the data prefix
	// (cmd/format.go:603) and m.Init does not, so the fixture writes it: the
	// supervisor's third identity leg reads it back.
	if err := object.WithPrefix(blob, loaded.Name+"/").
		Put(t.Context(), "juicefs_uuid", strings.NewReader(loaded.UUID)); err != nil {
		t.Fatalf("put juicefs_uuid: %v", err)
	}

	cc := chunk.Config{
		BlockSize:      loaded.BlockSize * 1024,
		CacheDir:       filepath.Join(dir, "cache"),
		CacheMode:      0600,
		CacheSize:      64 << 20,
		CacheChecksum:  "full",
		CacheEviction:  "2-random",
		AutoCreate:     true,
		MaxUpload:      4,
		MaxDownload:    8,
		MaxRetries:     3,
		CacheFullBlock: true,
		BufferSize:     32 << 20,
		GetTimeout:     30 * time.Second,
		PutTimeout:     30 * time.Second,
		Prefetch:       1,
	}
	cc.SelfCheck(loaded.UUID)
	// cmd/object.go createStorage() prefixes the bucket with the volume name;
	// the reader and the fsck-style block scan both assume that layout.
	store := chunk.NewCachedStore(object.WithPrefix(blob, loaded.Name+"/"), cc, nil)

	if err := m.NewSession(true); err != nil {
		t.Fatalf("new session: %v", err)
	}
	vc := &vfs.Config{
		Meta: mc, Format: *loaded, Chunk: &cc,
		Version:      version.Version(),
		HideInternal: true,
		Mountpoint:   "plo320-fixture",
		Pid:          os.Getpid(),
	}
	jfs, err := fs.NewFileSystem(vc, m, store, nil)
	if err != nil {
		t.Fatalf("filesystem: %v", err)
	}

	ctx := meta.NewContext(uint32(os.Getpid()), 0, []uint32{0})
	rng := rand.New(rand.NewSource(20260902))
	for p, size := range opt.files {
		if d := filepath.Dir(p); d != "/" && d != "." {
			if errno := jfs.MkdirAll(ctx, d, 0755, 0); errno != 0 && errno != 17 /* EEXIST */ {
				t.Fatalf("mkdir %s: %s", d, errno)
			}
		}
		buf := make([]byte, size)
		rng.Read(buf)
		f, errno := jfs.Create(ctx, p, 0644, 0)
		if errno != 0 {
			t.Fatalf("create %s: %s", p, errno)
		}
		if size > 0 {
			if _, errno := f.Write(ctx, buf); errno != 0 {
				t.Fatalf("write %s: %s", p, errno)
			}
		}
		if errno := f.Close(ctx); errno != 0 {
			t.Fatalf("close %s: %s", p, errno)
		}
		v.files[p] = buf
	}
	if err := jfs.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// Close() checkpoints and removes the WAL. Without it the database on disk
	// is a 4 KiB stub with the real content in a sidecar, and every later
	// assertion about "the metadata database" would be about the stub.
	if err := jfs.Close(); err != nil {
		t.Fatalf("close fs: %v", err)
	}
	m.CloseSession()
	if err := m.Shutdown(); err != nil {
		t.Fatalf("shutdown meta: %v", err)
	}
	return v
}

// storage returns the raw bucket handle (no volume prefix).
func (v *volume) storage(t *testing.T) object.ObjectStorage {
	t.Helper()
	blob, err := object.CreateStorage("file", v.bucketDir, "", "", "")
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	return blob
}

// blocks returns the handle ScanMissingBlocks expects: the volume prefix plus
// "chunks/", the same composition cmd/fsck.go:135 builds.
func (v *volume) blocks(t *testing.T) object.ObjectStorage {
	t.Helper()
	return object.WithPrefix(object.WithPrefix(v.storage(t), v.format.Name+"/"), "chunks/")
}

// blockPath is the on-disk path of one block object under the file store.
func (v *volume) blockPath(key string) string {
	return filepath.Join(v.bucketDir, v.format.Name, "chunks", key)
}

// openMeta opens a metadata database for reading and writing and returns a
// client plus a filesystem over the volume's data. The caller must call the
// returned close function.
func (v *volume) openMeta(t *testing.T, metaPath string) (meta.Meta, *fs.FileSystem, func()) {
	t.Helper()

	mc := meta.DefaultConf()
	mc.NoBGJob = true
	mc.Heartbeat = 0
	mc.AtimeMode = meta.NoAtime
	m := meta.NewClient("sqlite3://"+metaPath, mc)
	format, err := m.Load(true)
	if err != nil {
		t.Fatalf("load format from %s: %v", metaPath, err)
	}

	cc := chunk.Config{
		BlockSize:      format.BlockSize * 1024,
		CacheDir:       filepath.Join(t.TempDir(), "cache"),
		CacheMode:      0600,
		CacheSize:      64 << 20,
		CacheChecksum:  "full",
		CacheEviction:  "2-random",
		AutoCreate:     true,
		MaxUpload:      4,
		MaxDownload:    8,
		MaxRetries:     1,
		CacheFullBlock: false,
		BufferSize:     32 << 20,
		GetTimeout:     10 * time.Second,
		PutTimeout:     10 * time.Second,
	}
	cc.SelfCheck(format.UUID)
	store := chunk.NewCachedStore(object.WithPrefix(v.storage(t), format.Name+"/"), cc, nil)

	if err := m.NewSession(true); err != nil {
		t.Fatalf("new session: %v", err)
	}
	vc := &vfs.Config{
		Meta: mc, Format: *format, Chunk: &cc,
		Version:      version.Version(),
		HideInternal: true,
		Mountpoint:   "plo320-verify",
		Pid:          os.Getpid(),
	}
	jfs, err := fs.NewFileSystem(vc, m, store, nil)
	if err != nil {
		t.Fatalf("filesystem: %v", err)
	}
	return m, jfs, func() {
		_ = jfs.Close()
		m.CloseSession()
		_ = m.Shutdown()
	}
}

// readAll reads a whole file through the JuiceFS read path.
func readAll(t *testing.T, jfs *fs.FileSystem, p string) ([]byte, error) {
	t.Helper()
	ctx := meta.NewContext(uint32(os.Getpid()), 0, []uint32{0})
	f, errno := jfs.Open(ctx, p, 0)
	if errno != 0 {
		return nil, fmt.Errorf("open %s: %s", p, errno)
	}
	defer func() { _ = f.Close(ctx) }()

	var out []byte
	buf := make([]byte, 128<<10)
	for {
		n, err := f.Read(ctx, buf)
		if n > 0 {
			out = append(out, buf[:n]...)
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return out, err
		}
		if n == 0 {
			return out, nil
		}
	}
}

func newTestUUID(t *testing.T) string {
	t.Helper()
	b := make([]byte, 16)
	if _, err := crand.Read(b); err != nil {
		t.Fatalf("uuid: %v", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
