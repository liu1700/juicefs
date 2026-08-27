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

package vfs

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
)

// TestBackupPloriProfile runs a full metadata-backup cycle with only the plori
// profile's backends: Redis metadata and S3 object storage. TestBackup cannot
// stand in for it — it uses memkv meta and mem object storage, both excluded
// under the plori tag, so it only ever runs in untagged builds where
// register_default.go registers the `file` backend and hides a profile
// regression. The plori profile shipped exactly that regression once:
// excluding `file` made every CreateStorage("file", ...) staging call inside
// backup() fail with "invalid storage: file" before any upload, silently
// disabling --backup-meta in production for 14 days (issue #27). This test
// pins the whole path under the real build tags.
//
// Requires live services and skips without them:
//
//	PLORI_TEST_META_URL  e.g. redis://127.0.0.1:6379/2
//	PLORI_TEST_BLOB_URL  e.g. http://127.0.0.1:9000/plori-ci-backup (S3/MinIO)
//	AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY for the blob endpoint
func TestBackupPloriProfile(t *testing.T) {
	metaURL := os.Getenv("PLORI_TEST_META_URL")
	blobURL := os.Getenv("PLORI_TEST_BLOB_URL")
	if metaURL == "" || blobURL == "" {
		t.Skip("PLORI_TEST_META_URL / PLORI_TEST_BLOB_URL not set")
	}

	blob, err := object.CreateStorage("s3", blobURL,
		os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), "")
	if err != nil {
		t.Fatalf("create s3 storage %s: %s", blobURL, err)
	}
	if err = blob.Create(context.Background()); err != nil {
		t.Fatalf("create bucket %s: %s", blobURL, err)
	}

	metaConf := meta.DefaultConf()
	metaConf.MountPoint = "/jfs-plori-backup-test"
	m := meta.NewClient(metaURL, metaConf)
	format := &meta.Format{
		Name:      "plori-backup-ci",
		UUID:      uuid.New().String(),
		Storage:   "s3",
		BlockSize: 4096,
	}
	if err = m.Init(format, true); err != nil {
		t.Fatalf("init meta %s: %s", metaURL, err)
	}
	if err = m.NewSession(false); err != nil {
		t.Fatalf("new session: %s", err)
	}
	defer m.CloseSession() //nolint:errcheck

	// 100ms interval: the first cycle is due immediately regardless of any
	// lastBackup xattr a previous run left behind.
	go Backup(m, blob, time.Millisecond*100, false)

	deadline := time.Now().Add(time.Minute)
	scoped := object.WithPrefix(blob, "meta/")
	for {
		kc, err := object.ListAll(context.Background(), scoped, "", "", true, false)
		if err == nil {
			n := 0
			for obj := range kc {
				if obj != nil && obj.Key() != "" {
					n++
				}
			}
			if n >= 1 {
				return // a dump landed in object storage through the file staging path
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("no metadata backup appeared under meta/ within 1m — " +
				"the plori profile likely lost the `file` staging backend again (issue #27)")
		}
		time.Sleep(200 * time.Millisecond)
	}
}
