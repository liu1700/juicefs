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
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

var (
	testTempRootOnce sync.Once
	testTempRoot     string
	testTempSeq      atomic.Uint64
)

// testTempDir returns a fresh directory under one process-scoped root. The
// harness entry points (createTestVFS and friends) take no *testing.T, so
// t.TempDir is not available here; TestMain removes the root instead.
func testTempDir(name string) string {
	testTempRootOnce.Do(func() {
		root, err := os.MkdirTemp("", "juicefs-vfs-test-")
		if err != nil {
			panic(fmt.Sprintf("create test root: %s", err))
		}
		testTempRoot = root
	})
	dir := filepath.Join(testTempRoot, fmt.Sprintf("%s-%d", name, testTempSeq.Add(1)))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		panic(fmt.Sprintf("create test dir %s: %s", dir, err))
	}
	return dir
}

// testSQLiteURI is a metadata URI backed by its own database file.
//
// A bare "sqlite3://" leaves the address empty, and the SQL engine then hands
// the driver the encoded DSN options as the filename, so the test writes a file
// literally named "?_journal=WAL&_synchronous=NORMAL&_timeout=5000&cache=shared"
// into whatever directory the test binary runs in — the package source
// directory. Naming the file keeps it in TMPDIR and gives each caller a
// database of its own.
func testSQLiteURI() string {
	return "sqlite3://" + filepath.Join(testTempDir("meta"), "meta.db")
}

func TestMain(m *testing.M) {
	code := m.Run()
	if testTempRoot != "" {
		_ = os.RemoveAll(testTempRoot)
	}
	os.Exit(code)
}
