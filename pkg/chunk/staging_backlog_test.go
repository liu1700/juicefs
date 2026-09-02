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

//nolint:errcheck
package chunk

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// backlogTestConf is a writeback store whose staging area is not a function of
// how full the developer's disk happens to be.
//
// FreeSpace is pinned low on purpose: diskCache refuses to stage once the
// filesystem's free ratio drops under it (disk_cache.go isFull/stageFull), and
// a test about a backlog cap that silently stops staging for an unrelated
// reason proves nothing. Everything else mirrors newDurabilityTestStore.
func backlogTestConf(t *testing.T) Config {
	t.Helper()
	config := defaultConf
	config.CacheDir = backlogCacheDir(t)
	config.CacheSize = 1 << 30
	config.FreeSpace = 0.0001
	config.Writeback = true
	config.WritebackThresholdSize = config.BlockSize + 1
	config.PutTimeout = 30 * time.Second
	return config
}

// backlogCacheDir is t.TempDir() without the cleanup assertion.
//
// NewCachedStore has no Close: its uploader, its delayed-staging sweep and its
// cache scanner run for the life of the process. t.TempDir() fails the test if
// its RemoveAll races one of them writing a block back ("directory not empty"),
// which says nothing about the code under test.
func backlogCacheDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "backlog-cache-")
	if err != nil {
		t.Fatalf("cache dir: %s", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func stagedBlocks(store ChunkStore) uint64 {
	return store.(RemoteDurabilityStore).RemoteDurabilityStatus().PendingBlocks
}

// TestTheStagingBacklogCapUploadsThroughInsteadOfDropping is the cap's whole
// contract in one test: at the cap the writer stops staging and goes to the
// object store instead, so the backlog stops growing, the data is still
// written, and nothing answers an error.
//
// The alternative back-pressure knobs in this package do not have that shape.
// --upload-limit throttles bandwidth (it makes the backlog worse), --max-uploads
// sets upload concurrency, and --max-stage-write caps CONCURRENT staging writes
// (stagingBlocks is incremented for the duration of one flushPage,
// disk_cache.go stage), none of which bounds how many blocks are staged and
// waiting. That number is the writeback loss window and the work a shutdown
// barrier has to drain, so it is the one that needed a cap (PLO-383).
func TestTheStagingBacklogCapUploadsThroughInsteadOfDropping(t *testing.T) {
	blob := newTestStorage(t)
	config := backlogTestConf(t)
	config.MaxStagingBacklog = 4
	// Nothing drains on its own, so every pending block is one the cap put
	// there and the count is not a race against the uploader.
	config.UploadDelay = time.Hour
	store := NewCachedStore(blob, config, nil)
	limiter := store.(StagingBacklogLimiter)
	require.EqualValues(t, 4, limiter.StagingBacklogCap())

	data := []byte("staged")
	for id := uint64(1); id <= 4; id++ {
		writeDurabilityTestSlice(t, store, id, data)
	}
	require.EqualValues(t, 4, stagedBlocks(store), "the first four writes stage")

	for id := uint64(5); id <= 8; id++ {
		writeDurabilityTestSlice(t, store, id, data)
	}
	require.EqualValues(t, 4, stagedBlocks(store), "the backlog does not grow past the cap")
	require.EqualValues(t, 4, store.(*cachedStore).StagingBacklogTrips(), "four writes were sent through the store")

	// Everything above the cap is IN the object store already: the cap creates
	// back-pressure, it never drops a block.
	for id := uint64(5); id <= 8; id++ {
		p := NewPage(make([]byte, len(data)))
		n, err := store.NewReader(id, len(data)).ReadAt(context.Background(), p, 0)
		require.NoError(t, err, "slice %d must be readable", id)
		require.Equal(t, len(data), n)
		require.Equal(t, data, p.Data)
		p.Release()
		require.NoError(t, store.EvictCache(id, uint32(len(data)), nil))
		p = NewPage(make([]byte, len(data)))
		_, err = store.NewReader(id, len(data)).ReadAt(context.Background(), p, 0)
		require.NoError(t, err, "slice %d must be durable in the object store, not just cached", id)
		p.Release()
	}
}

// TestTheStagingBacklogCapBlocksTheWriter proves the back-pressure is real: at
// the cap a write does not return until the object store has taken it. A cap
// that let the writer run ahead would move the unbounded queue somewhere else
// rather than bound it.
func TestTheStagingBacklogCapBlocksTheWriter(t *testing.T) {
	blob := newTestStorage(t)
	release := make(chan struct{})
	controlled := &controlledPutStore{ObjectStorage: blob, release: release}
	config := backlogTestConf(t)
	config.MaxStagingBacklog = 1
	config.UploadDelay = time.Hour
	store := NewCachedStore(controlled, config, nil)

	writeDurabilityTestSlice(t, store, 201, []byte("staged"))
	require.EqualValues(t, 1, stagedBlocks(store))

	done := make(chan struct{})
	go func() {
		defer close(done)
		w := store.NewWriter(202, 0)
		w.WriteAt([]byte("through"), 0)
		w.Finish(len("through"))
	}()
	select {
	case <-done:
		t.Fatal("the write above the cap returned before the object store took it")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the write above the cap never completed after the store was released")
	}
	require.EqualValues(t, 1, stagedBlocks(store), "the blocked write never joined the backlog")
}

// TestTheStagingBacklogCapMovesLive proves the cap can be re-sized without a
// remount. The supervisor derives it from the drain rate it measures, and a
// rate it can only apply at mount time is a constant again.
func TestTheStagingBacklogCapMovesLive(t *testing.T) {
	blob := newTestStorage(t)
	config := backlogTestConf(t)
	config.UploadDelay = time.Hour
	store := NewCachedStore(blob, config, nil)
	limiter := store.(StagingBacklogLimiter)
	require.Zero(t, limiter.StagingBacklogCap(), "unlimited by default, which is upstream's behaviour")

	for id := uint64(301); id <= 303; id++ {
		writeDurabilityTestSlice(t, store, id, []byte("x"))
	}
	require.EqualValues(t, 3, stagedBlocks(store))

	limiter.SetStagingBacklogCap(3)
	writeDurabilityTestSlice(t, store, 304, []byte("x"))
	require.EqualValues(t, 3, stagedBlocks(store), "a cap set below the live backlog holds it where it is")

	limiter.SetStagingBacklogCap(5)
	writeDurabilityTestSlice(t, store, 305, []byte("x"))
	require.EqualValues(t, 4, stagedBlocks(store), "a larger cap lets the backlog grow again")

	limiter.SetStagingBacklogCap(-1)
	require.Zero(t, limiter.StagingBacklogCap(), "negative is unlimited, not a negative cap")
}

// TestADeepBacklogDrainsWithoutWaitingForTheSweep is the PLO-346 arithmetic,
// flipped.
//
// That run measured 1,008 staged blocks taking 595 s to reach the store on a
// 1-vCPU node and read it as ~590 ms of per-block local work. It is not: the
// only thing that re-queues a staged block which missed pendingCh (capacity
// 100*MaxUpload) was a sweep on a flat one-minute timer, so a backlog deeper
// than that channel could only leave one channel-full per minute. 1,008 blocks
// / ~100 per sweep is ~10 sweeps is ~600 s, which is the measurement. The same
// run's barrier drained ~345 blocks in 10.7 s (31 ms per block) precisely
// because RemoteDurability force-queues past the channel.
//
// This test stages more blocks than the channel holds, never runs a barrier,
// and requires the passive drain to finish in seconds. On the one-minute sweep
// it cannot: the last blocks are not even queued until 60 s have passed.
func TestADeepBacklogDrainsWithoutWaitingForTheSweep(t *testing.T) {
	blob := newTestStorage(t)
	release := make(chan struct{})
	controlled := &controlledPutStore{ObjectStorage: blob, release: release}
	config := backlogTestConf(t)
	// MaxUpload is 1 in defaultConf, so pendingCh holds 100. Stage half again
	// as many blocks as that, with every upload blocked, so ~50 of them can
	// only reach an uploader through the sweep.
	const blocks = 150
	require.Equal(t, 1, config.MaxUpload, "the channel capacity this test reasons about is 100*MaxUpload")
	store := NewCachedStore(controlled, config, nil)
	data := bytes.Repeat([]byte("q"), 64)
	for id := uint64(1); id <= blocks; id++ {
		writeDurabilityTestSlice(t, store, id, data)
	}
	require.EqualValues(t, blocks, stagedBlocks(store), "every write staged; none went through the blocked store")

	close(release)
	deadline := time.Now().Add(30 * time.Second)
	for stagedBlocks(store) > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d blocks were still staged after 30 s; the passive drain is still quantised by the sweep",
				stagedBlocks(store))
		}
		time.Sleep(20 * time.Millisecond)
	}
}
