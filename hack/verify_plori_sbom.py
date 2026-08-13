#!/usr/bin/env python3
import json
import pathlib
import sys


REQUIRED = {
    "github.com/aws/aws-sdk-go-v2/service/s3",
    "github.com/juicedata/go-fuse/v2",
    "github.com/redis/go-redis/v9",
}
DENIED_PREFIXES = (
    "cloud.google.com/go/storage",
    "github.com/Azure/azure-sdk-for-go",
    "github.com/coredns/coredns",
    "github.com/go-sql-driver/mysql",
    "github.com/google/btree",
    "github.com/jackc/pgx",
    "github.com/mattn/go-sqlite3",
    "github.com/minio/minio-go",
    "github.com/pkg/sftp",
    "github.com/prometheus/prometheus",
    "github.com/tikv/",
    "go.etcd.io/etcd",
    "golang.org/x/net/webdav",
    "google.golang.org/grpc",
    "modernc.org/sqlite",
)


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {pathlib.Path(sys.argv[0]).name} SPDX_JSON", file=sys.stderr)
        return 2
    document = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
    packages = {package.get("name", "") for package in document.get("packages", [])}
    missing = sorted(REQUIRED - packages)
    denied = sorted(name for name in packages if name.startswith(DENIED_PREFIXES))
    if missing or denied:
        if missing:
            print(f"Plori SBOM is missing required modules: {', '.join(missing)}", file=sys.stderr)
        if denied:
            print(f"Plori SBOM contains excluded modules: {', '.join(denied)}", file=sys.stderr)
        return 1
    print(f"Plori SBOM dependency profile verified ({len(packages)} packages)")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"Plori SBOM verification failed: {error}", file=sys.stderr)
        raise SystemExit(2)
