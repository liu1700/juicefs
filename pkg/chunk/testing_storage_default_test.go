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

package chunk

import (
	"testing"

	"github.com/juicedata/juicefs/pkg/object"
)

// newTestStorage returns the object storage backend the tests in this package
// write through. The default build registers `mem` (register_default.go), the
// cheapest double for a store that only has to honour the ObjectStorage
// contract.
func newTestStorage(tb testing.TB) object.ObjectStorage {
	tb.Helper()
	blob, err := object.CreateStorage("mem", "", "", "", "")
	if err != nil {
		tb.Fatalf("create mem storage: %s", err)
	}
	return blob
}
