// The memkv metadata engine is excluded from the Plori release profile
// (see tkv_mem.go), and the tests below are built on it.
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

package meta

import (
	"testing"
)

func TestMemKVDumpMetaNoGoroutineLeakOnFailure(t *testing.T) {
	testDumpMetaNoGoroutineLeakOnFailure(t,
		func(t *testing.T) Meta {
			t.Helper()
			m, err := newKVMeta("memkv", "jfs-dump-leak", testConfig())
			if err != nil {
				t.Fatalf("create meta: %s", err)
			}
			if err := m.Reset(); err != nil {
				t.Fatalf("reset meta: %s", err)
			}
			if err := m.Init(testFormat(), true); err != nil {
				t.Fatalf("init meta: %s", err)
			}
			return m
		},
		func(t *testing.T, m Meta) {
			t.Helper()
			km := m.(*kvMeta)
			km.client = &failFastDumpScan{tkvClient: km.client}
		},
	)
}
