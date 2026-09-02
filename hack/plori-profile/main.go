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

package main

import (
	"fmt"
	"os"

	_ "github.com/juicedata/juicefs/cmd"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
)

func main() {
	failed := false
	require := func(kind, name string, supported, want bool) {
		if supported != want {
			fmt.Fprintf(os.Stderr, "%s %q support = %v, want %v\n", kind, name, supported, want)
			failed = true
		}
	}

	require("metadata", "redis", meta.IsSupported("redis"), true)
	// A per-Agent volume keeps its metadata in a local SQLite file (PLO-319);
	// the shared volume keeps using Redis. Both are supported; every other
	// SQL and KV engine stays compiled out.
	require("metadata", "sqlite3", meta.IsSupported("sqlite3"), true)
	for _, name := range []string{"mysql", "postgres", "tikv", "etcd", "badger", "memkv"} {
		require("metadata", name, meta.IsSupported(name), false)
	}
	require("object storage", "s3", object.IsSupported("s3"), true)
	// `file` is not a remote backend: vfs.Backup stages every metadata dump
	// through CreateStorage("file", ...) before uploading it, so the profile
	// must keep it registered (issue #27 — excluding it silently disabled
	// --backup-meta in production).
	require("object storage", "file", object.IsSupported("file"), true)
	for _, name := range []string{
		"azure", "b2", "bos", "cifs", "cos", "dragonfly", "eos", "etcd", "gs", "hdfs",
		"ibmcos", "jfs", "ks3", "mem", "minio", "mysql", "nfs", "obs", "oos", "oss", "postgres",
		"qingstor", "qiniu", "redis", "scw", "sftp", "space", "sqlite3", "storj", "swift", "tikv",
		"tos", "ufile", "wasabi", "webdav",
	} {
		require("object storage", name, object.IsSupported(name), false)
	}
	if failed {
		os.Exit(1)
	}
	fmt.Println("Plori build profile exposes only Redis and SQLite metadata and S3 remote object storage (plus the local file backend that vfs.Backup and sync stage through)")
}
