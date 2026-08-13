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
DENIED_NAME_FRAGMENTS = (
    "hadoop",
    "juicefs-hadoop",
    "org.apache.ranger",
    "ranger-authorization",
)
DENIED_FILE_FRAGMENTS = (
    "sdk/java/",
    ".jar",
    ".class",
    "io/juicefs/",
    "hadoop",
    "ranger",
)


def verify(document: dict) -> tuple[list[str], list[str], list[str]]:
    packages = {package.get("name", "") for package in document.get("packages", [])}
    missing = sorted(REQUIRED - packages)
    denied_packages = sorted(
        name
        for name in packages
        if name.startswith(DENIED_PREFIXES)
        or any(fragment in name.casefold() for fragment in DENIED_NAME_FRAGMENTS)
    )
    denied_files = sorted(
        file.get("fileName", "")
        for file in document.get("files", [])
        if any(fragment in file.get("fileName", "").casefold() for fragment in DENIED_FILE_FRAGMENTS)
    )
    return missing, denied_packages, denied_files


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {pathlib.Path(sys.argv[0]).name} SPDX_JSON", file=sys.stderr)
        return 2
    document = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
    missing, denied_packages, denied_files = verify(document)
    if missing or denied_packages or denied_files:
        if missing:
            print(f"Plori SBOM is missing required modules: {', '.join(missing)}", file=sys.stderr)
        if denied_packages:
            print(f"Plori SBOM contains excluded modules: {', '.join(denied_packages)}", file=sys.stderr)
        if denied_files:
            print(f"Plori SBOM contains excluded files: {', '.join(denied_files)}", file=sys.stderr)
        return 1
    print(f"Plori SBOM dependency profile verified ({len(document.get('packages', []))} packages)")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"Plori SBOM verification failed: {error}", file=sys.stderr)
        raise SystemExit(2)
