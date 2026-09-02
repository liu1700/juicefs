//go:build !nobadger
// +build !nobadger

/*
 * JuiceFS, Copyright 2025 Juicedata, Inc.
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
	"path/filepath"
	"syscall"
	"testing"
)

func TestBadgerKVTxnReturnsEINTRWhenContextAlreadyCanceled(t *testing.T) {
	metaURL := "badger://" + filepath.Join(t.TempDir(), "jfs-cancel-tkv-badger")
	m := NewClient(metaURL, testConfig())
	if err := m.Reset(); err != nil {
		t.Fatalf("reset meta: %v", err)
	}
	if err := m.Init(testFormat(), true); err != nil {
		t.Fatalf("init format: %v", err)
	}
	km, ok := m.(*kvMeta)
	if !ok {
		t.Fatalf("unexpected meta type: %T", m)
	}

	ctx := NewContext(1, 0, []uint32{0})
	ctx.Cancel()
	err := km.txn(ctx, func(tx *kvTxn) error { return nil })
	if err != syscall.EINTR {
		t.Fatalf("expected EINTR, got %v", err)
	}
}
