//go:build !plori
// +build !plori

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

	"github.com/juicedata/juicefs/pkg/object"
)

// defaultTestMetaURI is the metadata engine the harness uses when a test does
// not pin one. The default build carries memkv (pkg/meta/tkv_mem.go): an
// in-process KV that needs neither a file nor a server.
func defaultTestMetaURI() string {
	return "memkv://"
}

// testMetaEngines is the engine matrix the readdir tests walk. The keys are
// meta.DirBatchNum keys, not URI schemes.
func testMetaEngines() map[string]string {
	return map[string]string{
		"kv":    "memkv://",
		"db":    "sqlite3://:memory:",
		"redis": "redis://127.0.0.1:6379/2",
	}
}

// newTestStorage is the object storage the harness writes chunks through. The
// default build registers `mem` (pkg/object/register_default.go).
func newTestStorage() object.ObjectStorage {
	blob, err := object.CreateStorage("mem", "", "", "", "")
	if err != nil {
		panic(fmt.Sprintf("create mem storage: %s", err))
	}
	return blob
}
