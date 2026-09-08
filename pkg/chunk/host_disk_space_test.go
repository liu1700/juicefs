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

package chunk

import (
	"os"
	"path/filepath"
	"testing"
)

// defaultCacheFreeRatio is newDiskCache's own default for an unset
// Config.FreeSpace (disk_cache.go:112).
const defaultCacheFreeRatio = 0.1

// existingAncestor is the nearest path that exists at or above dir. statfs on a
// missing path fails, and getDiskUsage answers 1/1/1/1 for a failure, which
// reads as a completely free filesystem. A test's cache directory normally does
// not exist yet, so the reading has to be taken on a parent.
func existingAncestor(dir string) string {
	p := dir
	for {
		if _, err := os.Stat(p); err == nil {
			return p
		}
		parent := filepath.Dir(p)
		if parent == p {
			return p
		}
		p = parent
	}
}

// skipIfHostDiskIsTooFull skips a test whose disk cache cannot hold anything on
// this host.
//
// A disk cache built with the default FreeSpace keeps 10% of the filesystem
// free, and on a host already below that the cache starts full: it accepts no
// block and stages nothing, so a test that writes and then asserts on what the
// cache holds fails for a reason that is not about the code under test. This
// was reported from macOS, where the container filesystem measured about 4%
// free (PLO-412).
//
// The reading uses getDiskUsage, the function curFreeRatio itself calls, so the
// number printed here is the number the cache decided on.
//
// It belongs only on tests whose result depends on the cache accepting data.
// Running the package with the default FreeSpace raised to 99%, which is the
// same predicate the cache applies to a nearly full host, fails TestMetrics and
// TestRemoteDurabilityFence on the default build and TestMetrics,
// TestRemoteDurabilityFence and TestRemoteDurabilityReportsUploadFailure under
// the plori tag set. Two calls cover those: one in TestMetrics and one in
// newDurabilityTestStore, which every durability-fence test shares. Every other test
// that builds a real disk cache passes on a full host and is left alone,
// because a skip there would drop coverage rather than remove a false failure.
func skipIfHostDiskIsTooFull(t *testing.T, dir string) {
	t.Helper()
	total, free, files, ffree := getDiskUsage(existingAncestor(dir))
	var spaceRatio, inodeRatio float64
	if total > 0 {
		spaceRatio = float64(free) / float64(total)
	}
	if files > 0 {
		inodeRatio = float64(ffree) / float64(files)
	}
	if spaceRatio < defaultCacheFreeRatio {
		t.Skipf("host free ratio %.2f%% < %.0f%%: a disk cache with the default FreeSpace holds nothing here",
			spaceRatio*100, defaultCacheFreeRatio*100)
	}
	// inodeCap of zero means the filesystem does not report inodes, and the
	// cache's own full() check ignores the inode ratio in that case
	// (disk_cache.go:380).
	if files > 0 && inodeRatio < defaultCacheFreeRatio {
		t.Skipf("host free inode ratio %.2f%% < %.0f%%: a disk cache with the default FreeSpace holds nothing here",
			inodeRatio*100, defaultCacheFreeRatio*100)
	}
}
