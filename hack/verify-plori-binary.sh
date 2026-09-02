#!/bin/sh
set -eu

binary=${1:-./juicefs.plori}
if [ ! -x "$binary" ]; then
    echo "Plori binary is missing or not executable: $binary" >&2
    exit 1
fi

build_info=$(go version -m "$binary")
printf '%s\n' "$build_info" | grep -q -- '-tags=plori,'

denied_modules='google.golang.org/grpc|github.com/tikv/|go.etcd.io/etcd|github.com/google/btree|github.com/go-sql-driver/mysql|github.com/jackc/pgx|modernc.org/sqlite|github.com/pkg/sftp|golang.org/x/net/webdav|github.com/minio/minio-go|github.com/coredns/coredns|github.com/prometheus/prometheus|cloud.google.com/go/storage|github.com/Azure/azure-sdk-for-go'
if printf '%s\n' "$build_info" | grep -E "$denied_modules"; then
    echo "Plori binary contains a forbidden backend or service dependency" >&2
    exit 1
fi

# The support policy promises a `sqlite3` metadata engine. A build that dropped
# it (by re-adding `nosqlite`, or by losing cgo) would still report a valid
# Plori version, so assert the driver and its hardening tag are actually linked.
if ! printf '%s\n' "$build_info" | grep -q 'github.com/mattn/go-sqlite3'; then
    echo "Plori binary is missing the SQLite metadata driver" >&2
    exit 1
fi
if ! printf '%s\n' "$build_info" | grep -q -- '-tags=.*sqlite_omit_load_extension'; then
    echo "Plori binary was not built with sqlite_omit_load_extension" >&2
    exit 1
fi
if printf '%s\n' "$build_info" | grep -q -- '-tags=.*nosqlite'; then
    echo "Plori binary was built with nosqlite but the profile supports sqlite3" >&2
    exit 1
fi
if ! printf '%s\n' "$build_info" | grep -q -- 'CGO_ENABLED=1'; then
    echo "Plori binary was not built with cgo, so it cannot carry SQLite" >&2
    exit 1
fi

"$binary" version
echo "Plori binary dependency profile verified"
