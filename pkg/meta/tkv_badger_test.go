//go:build !nobadger
// +build !nobadger

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
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func TestBadgerClient(t *testing.T) {
	m, err := newKVMeta("badger", t.TempDir(), testConfig())
	if err != nil || m.Name() != "badger" {
		t.Fatalf("create meta: %s", err)
	}
	testMeta(t, m)
}

func TestBadgerKV(t *testing.T) {
	c, err := newBadgerClient("test_badger")
	if err != nil {
		t.Fatal(err)
	}
	testTKV(t, c)
}

func TestBadgerScanKeysOnlyNilValues(t *testing.T) {
	c, err := newBadgerClient(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()

	if err := c.txn(Background(), func(kt *kvTxn) error {
		kt.set([]byte("key1"), []byte("value1"))
		kt.set([]byte("key2"), []byte("value2"))
		return nil
	}, 0); err != nil {
		t.Fatal(err)
	}

	var scanned int
	if err := c.txn(Background(), func(kt *kvTxn) error {
		kt.scan([]byte("key"), nextKey([]byte("key")), true, func(k, v []byte) bool {
			if v != nil {
				t.Errorf("keysOnly=true: expected nil value for key %q, got %q", k, v)
			}
			scanned++
			return true
		})
		return nil
	}, 0); err != nil {
		t.Fatal(err)
	}
	if scanned != 2 {
		t.Fatalf("expected 2 keys scanned, got %d", scanned)
	}
}

func TestBadgerSimpleTxnReadOnly(t *testing.T) {
	c, err := newBadgerClient(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()

	if err := c.txn(Background(), func(kt *kvTxn) error {
		kt.set([]byte("ro_key"), []byte("ro_value"))
		return nil
	}, 0); err != nil {
		t.Fatal(err)
	}

	var got []byte
	var scanned int
	if err := c.simpleTxn(Background(), func(kt *kvTxn) error {
		got = kt.get([]byte("ro_key"))
		kt.scan([]byte("ro_"), nextKey([]byte("ro_")), false, func(k, v []byte) bool {
			scanned++
			return true
		})
		return nil
	}, 0); err != nil {
		t.Fatal(err)
	}
	if string(got) != "ro_value" || scanned != 1 {
		t.Fatalf("simpleTxn read: got %q, scanned %d", got, scanned)
	}

	err = c.simpleTxn(Background(), func(kt *kvTxn) error {
		kt.set([]byte("ro_key2"), []byte("v"))
		return nil
	}, 0)
	if err != badger.ErrReadOnlyTxn {
		t.Fatalf("expected ErrReadOnlyTxn, got %v", err)
	}
	var leaked []byte
	if err := c.simpleTxn(Background(), func(kt *kvTxn) error {
		leaked = kt.get([]byte("ro_key2"))
		return nil
	}, 0); err != nil {
		t.Fatal(err)
	}
	if leaked != nil {
		t.Fatalf("write in read-only txn must not be committed, got %q", leaked)
	}
}

func TestBadgerSyncOption(t *testing.T) {
	root := t.TempDir()
	tests := []struct {
		name     string
		addr     string
		expected bool
	}{
		{name: "default false", addr: root + "/default", expected: false},
		{name: "sync true", addr: root + "/true?sync=true", expected: true},
		{name: "sync false", addr: root + "/false?sync=false", expected: false},
		{name: "sync invalid 1", addr: root + "/invalid1?sync=1", expected: false},
		{name: "sync invalid 0", addr: root + "/invalid0?sync=0", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := newBadgerClient(tt.addr)
			if err != nil {
				t.Fatal(err)
			}
			defer client.close()

			bc, ok := client.(*badgerClient)
			if !ok {
				t.Fatalf("unexpected client type %T", client)
			}
			if got := bc.client.Opts().SyncWrites; got != tt.expected {
				t.Fatalf("sync option mismatch: got %v, expected %v", got, tt.expected)
			}
		})
	}
}

func TestBadgerDeleteTxnTooBig(t *testing.T) {
	dir := t.TempDir()

	opt := badger.DefaultOptions(dir)
	opt.Logger = nil
	opt.MetricsEnabled = false
	opt.MemTableSize = 1 << 20
	opt.ValueThreshold = 1 << 10
	db, err := badger.Open(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const numKeys = 5000
	wb := db.NewWriteBatch()
	for i := 0; i < numKeys; i++ {
		if err := wb.Set([]byte(fmt.Sprintf("txbig_%05d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := wb.Flush(); err != nil {
		t.Fatal(err)
	}

	var keys [][]byte
	rtx := db.NewTransaction(false)
	it := rtx.NewIterator(badger.IteratorOptions{Prefix: []byte("txbig_"), PrefetchValues: false})
	for it.Rewind(); it.Valid(); it.Next() {
		keys = append(keys, it.Item().KeyCopy(nil))
	}
	it.Close()
	rtx.Discard()

	client := &badgerClient{client: db, done: make(chan struct{})}

	err = client.txn(Background(), func(kt *kvTxn) error {
		for _, key := range keys {
			kt.delete(key)
		}
		return nil
	}, 0)

	if err != badger.ErrTxnTooBig {
		t.Fatalf("expected ErrTxnTooBig, got %v", err)
	}
}
