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

package chunk

import (
	"testing"

	"github.com/juicedata/juicefs/pkg/object"
)

// newTestStorage returns the object storage backend the tests in this package
// write through.
//
// The Plori release profile registers only `s3` and `file`
// (register_plori.go), so CreateStorage("mem", ...) returns a nil
// ObjectStorage and every store built on it panics on the first upload
// (cached_store.go:317, PLO-368). `file` under a per-test directory is already
// in the profile, needs no network, and satisfies the same ObjectStorage
// contract, so the writeback path and the durability barrier are exercised
// under the exact tag set the release binary is built with.
func newTestStorage(tb testing.TB) object.ObjectStorage {
	tb.Helper()
	// The trailing "/" marks the root as a directory, so object keys become
	// paths under it instead of a shared filename prefix (file.go:98).
	blob, err := object.CreateStorage("file", tb.TempDir()+"/", "", "", "")
	if err != nil {
		tb.Fatalf("create file storage: %s", err)
	}
	return blob
}
