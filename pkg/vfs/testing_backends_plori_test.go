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

// The Plori release profile drops memkv (pkg/meta/tkv_mem.go) and every object
// backend except `s3` and `file` (pkg/object/register_plori.go). Reaching for
// either from a test is not a compile error: meta.NewClient calls
// logger.Fatalf on an unknown driver, which exits the whole test binary
// (pkg/meta/interface.go:666), and object.CreateStorage returns a nil
// ObjectStorage, which panics on the first upload
// (pkg/chunk/cached_store.go:317). This file binds the harness to the two
// backends the profile does carry — sqlite3 for metadata, `file` for objects —
// so ./pkg/vfs runs under the exact tag set the release binary is built with
// (PLO-368).

package vfs

import (
	"fmt"

	"github.com/juicedata/juicefs/pkg/object"
)

// defaultTestMetaURI is the metadata engine the harness uses when a test does
// not pin one. Each call gets its own database file: a shared-cache
// `sqlite3://:memory:` is one database for the whole process, so state from an
// earlier test would leak into the next one.
func defaultTestMetaURI() string {
	return testSQLiteURI()
}

// testMetaEngines is the engine matrix the readdir tests walk. The keys are
// meta.DirBatchNum keys, not URI schemes. "kv" (memkv) is absent because the
// profile excludes it; sqlite3 covers the same batching code paths through the
// SQL engine.
func testMetaEngines() map[string]string {
	return map[string]string{
		"db":    "sqlite3://:memory:",
		"redis": "redis://127.0.0.1:6379/2",
	}
}

// newTestStorage is the object storage the harness writes chunks through. The
// trailing "/" marks the root as a directory, so object keys become paths under
// it instead of a shared filename prefix (pkg/object/file.go:98).
func newTestStorage() object.ObjectStorage {
	blob, err := object.CreateStorage("file", testTempDir("blob")+"/", "", "", "")
	if err != nil {
		panic(fmt.Sprintf("create file storage: %s", err))
	}
	return blob
}
