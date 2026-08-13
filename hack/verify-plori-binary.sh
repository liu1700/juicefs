#!/bin/sh
set -eu

binary=${1:-./juicefs.plori}
if [ ! -x "$binary" ]; then
    echo "Plori binary is missing or not executable: $binary" >&2
    exit 1
fi

build_info=$(go version -m "$binary")
printf '%s\n' "$build_info" | grep -q -- '-tags=plori,'

denied_modules='google.golang.org/grpc|github.com/tikv/|go.etcd.io/etcd|github.com/google/btree|github.com/go-sql-driver/mysql|github.com/jackc/pgx|github.com/mattn/go-sqlite3|modernc.org/sqlite|github.com/pkg/sftp|golang.org/x/net/webdav|github.com/minio/minio-go|github.com/coredns/coredns|github.com/prometheus/prometheus|cloud.google.com/go/storage|github.com/Azure/azure-sdk-for-go'
if printf '%s\n' "$build_info" | grep -E "$denied_modules"; then
    echo "Plori binary contains a forbidden backend or service dependency" >&2
    exit 1
fi

"$binary" version
echo "Plori binary dependency profile verified"
