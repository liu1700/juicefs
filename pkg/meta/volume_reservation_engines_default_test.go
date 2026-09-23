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

import "testing"

// Keep every upstream engine required; opening an unavailable engine fails the test.
var volresEngines = []volresEngine{
	volresSQLiteEngine,
	{name: "memkv", reset: true, open: func(t *testing.T) (Meta, error) {
		return newKVMeta("memkv", "jfs-volres", testConfig())
	}},
	volresRedisEngine,
}
