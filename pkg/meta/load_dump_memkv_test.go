// The memkv metadata engine is excluded from the Plori release profile
// (see tkv_mem.go), and the tests below are built on it.
//go:build !plori
// +build !plori

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

//nolint:errcheck
package meta

import (
	"os"
	"testing"
)

func TestLoadDump_MemKV(t *testing.T) {
	t.Run("Metadata Engine: memkv", func(t *testing.T) {
		_ = os.Remove(settingPath)
		m := testLoad(t, "memkv://test/jfs", sampleFile, false)
		testDump(t, m, 1, sampleFile, "test.dump")
	})
	t.Run("Metadata Engine: memkv; --SubDir d1 ", func(t *testing.T) {
		_ = os.Remove(settingPath)
		m := testLoad(t, "memkv://user:pass@test/jfs", sampleFile, false)
		if kvm, ok := m.(*kvMeta); ok { // memkv will be empty if created again
			if st := kvm.Chroot(Background(), "d1"); st != 0 {
				t.Fatalf("Chroot to subdir d1: %s", st)
			}
		}
		testDump(t, m, 1, subSampleFile, "test_subdir.dump")
		testDump(t, m, 0, sampleFile, "test.dump")
		_ = os.Remove(settingPath)
		testLoadSub(t, "memkv://user:pass@test/jfs", subSampleFile)
	})
}
