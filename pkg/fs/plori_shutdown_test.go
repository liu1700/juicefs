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

package fs

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/vfs"
)

// openOneVolume builds the same object graph a short-lived embedded client
// builds -- a SQLite metadata engine, a `file://` bucket, a cached store with a
// real cache directory and a FileSystem on top -- and returns it.
func openOneVolume(t *testing.T, dir string, seq int) *FileSystem {
	t.Helper()

	metaPath := filepath.Join(dir, fmt.Sprintf("meta-%d.db", seq))
	// A short heartbeat only shortens how long the metadata engine's own
	// session refresh takes to notice CloseSession: that goroutine already
	// stops on its own, and this test is about the ones that did not.
	mc := meta.DefaultConf()
	mc.Heartbeat = 200 * time.Millisecond
	m := meta.NewClient("sqlite3://"+metaPath, mc)
	format := &meta.Format{
		Name:      "test",
		BlockSize: 4096,
		Capacity:  1 << 30,
		DirStats:  true,
	}
	if err := m.Init(format, true); err != nil {
		t.Fatalf("init metadata: %s", err)
	}
	cacheDir := filepath.Join(dir, fmt.Sprintf("cache-%d", seq))
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		t.Fatalf("create cache dir: %s", err)
	}
	cc := chunk.Config{
		BlockSize:   format.BlockSize << 10,
		CacheDir:    cacheDir,
		CacheSize:   10 << 20,
		AutoCreate:  true,
		MaxUpload:   1,
		MaxDownload: 4,
		MaxRetries:  1,
		BufferSize:  32 << 20,
		Prefetch:    1,
		GetTimeout:  time.Second,
		PutTimeout:  time.Second,
	}
	conf := vfs.Config{
		Meta:            mc,
		Format:          *format,
		Chunk:           &cc,
		DirEntryTimeout: time.Millisecond * 100,
		EntryTimeout:    time.Millisecond * 100,
		AttrTimeout:     time.Millisecond * 100,
	}
	blob, err := object.CreateStorage("file", filepath.Join(dir, fmt.Sprintf("blob-%d", seq))+"/", "", "", "")
	if err != nil {
		t.Fatalf("create object storage: %s", err)
	}
	store := chunk.NewCachedStore(blob, cc, nil)
	if err := m.NewSession(true); err != nil {
		t.Fatalf("new session: %s", err)
	}
	jfs, err := NewFileSystem(&conf, m, store, nil)
	if err != nil {
		t.Fatalf("new filesystem: %s", err)
	}
	return jfs
}

func closeOneVolume(t *testing.T, jfs *FileSystem) {
	t.Helper()
	if err := jfs.Close(); err != nil {
		t.Fatalf("close filesystem: %s", err)
	}
	if err := jfs.Meta().Shutdown(); err != nil {
		t.Fatalf("shutdown metadata: %s", err)
	}
}

// TestPloriCloseReleasesTheBackgroundGoroutines is PLO-572.
//
// A process that opens a volume, does one thing to it and closes it again --
// the storage worker's shape, thousands of times a day -- used to keep every
// background loop the open had started: FileSystem.cleanupCache, the VFS
// writer's flushAll, the VFS reader's checkReadBuffer, the cached store's cache
// watcher, its prefetchers and the block cache's own maintenance. Close flushed
// and closed the metadata session and left all of them running, so goroutines
// and the buffers they hold grew for the life of the process.
func TestPloriCloseReleasesTheBackgroundGoroutines(t *testing.T) {
	dir := t.TempDir()

	// One open/close pair first, so every package-level singleton (the object
	// storage's HTTP transport, the SQLite driver's pools, the metrics
	// registries) exists before the baseline is taken.
	closeOneVolume(t, openOneVolume(t, dir, 0))

	base := settledGoroutines()

	const rounds = 20
	for i := 1; i <= rounds; i++ {
		closeOneVolume(t, openOneVolume(t, dir, i))
	}

	after := settledGoroutines()
	// Anything that scales with `rounds` is a leak. A handful of goroutines
	// that do not is the runtime's own churn, so the bound is a constant and
	// not a per-round allowance.
	if after > base+8 {
		t.Fatalf("goroutines grew from %d to %d over %d open/close rounds (%.1f per round)\n%s",
			base, after, rounds, float64(after-base)/rounds, goroutineDump())
	}
	t.Logf("goroutines: baseline %d, after %d rounds %d", base, rounds, after)
}

// settledGoroutines waits for the count to hold still, so a goroutine that is
// on its way out -- the metadata engine's session refresh, which notices
// CloseSession only after its heartbeat -- is not counted as one that stayed.
func settledGoroutines() int {
	const (
		step   = 200 * time.Millisecond
		stable = 10 // 2 s unchanged
		limit  = 150
	)
	last, same := -1, 0
	for i := 0; i < limit; i++ {
		runtime.GC()
		time.Sleep(step)
		n := runtime.NumGoroutine()
		if n == last {
			if same++; same >= stable {
				return n
			}
		} else {
			last, same = n, 0
		}
	}
	return last
}

func goroutineDump() string {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	dump := string(buf[:n])
	// Keep the report to the leading line of each stack: the count per site is
	// what names the leak, and the full stacks are megabytes.
	var heads []string
	for _, g := range strings.Split(dump, "\n\n") {
		// The top frames are the park itself (time.Sleep, chan receive), which
		// is the same for every leak. Name the first frame that belongs to this
		// module instead: that is the loop that did not stop.
		for _, line := range strings.Split(g, "\n") {
			if strings.HasPrefix(line, "github.com/juicedata/juicefs/") {
				// Drop the receiver and argument words: they are pointer
				// values, and they would make one site look like many.
				if i := strings.LastIndex(line, "("); i > 0 {
					line = line[:i]
				}
				heads = append(heads, strings.TrimSpace(line))
				break
			}
		}
	}
	counts := map[string]int{}
	for _, h := range heads {
		counts[h]++
	}
	var out []string
	for h, c := range counts {
		if c > 1 {
			out = append(out, fmt.Sprintf("%4d  %s", c, h))
		}
	}
	return strings.Join(out, "\n")
}
