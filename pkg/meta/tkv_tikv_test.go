//go:build !notikv
// +build !notikv

/*
 * JuiceFS, Copyright 2021 Juicedata, Inc.
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

//mutate:disable
//nolint:errcheck
package meta

import (
	"testing"
)

func TestTiKVClient(t *testing.T) { //skip mutate
	m, err := newKVMeta("tikv", "127.0.0.1:2379/jfs-unit-test", testConfig())
	if err != nil || m.Name() != "tikv" {
		t.Fatalf("create meta: %s", err)
	}
	testMeta(t, m)
}
