---
title: Plori build profile
---

The `plori` profile is the supported JuiceFS client for the Plori runtime and
Orlop. It intentionally supports only the deployment contract used there:

- Redis-compatible metadata through the `redis` driver;
- S3 and S3-compatible object storage through the `s3` driver;
- the FUSE client and the operational commands needed to format, mount,
  unmount, inspect, fence writes with `durability`, and manage directory quotas.

SQL and KV metadata engines, the S3 gateway, WebDAV, local and in-memory object
stores, and non-S3 object storage providers are excluded. Do not use this
artifact as a general-purpose replacement for a Community Edition release.

## Build and verify

Use the Go version in `.go-version` and run:

```shell
make -B juicefs.plori VERSION=dev
make test.plori.profile
hack/verify-plori-binary.sh ./juicefs.plori
```

`make test.plori.profile` fails if any metadata engine other than Redis or any
object storage driver other than S3 is registered. The binary verifier also
rejects dependencies belonging to excluded backend families.

The container build uses pinned multi-architecture base images and package
versions:

```shell
docker build -f Dockerfile.plori -t juicefs-plori:dev .
```

The runtime image contains the static client, CA certificates, FUSE 3, a POSIX
shell, and `tini`. This is the minimum image contract required by the Plori CSI
mounter.

## Release and security contract

Tags matching `vX.Y.Z-plori.N` run `.github/workflows/plori.yml`. A successful
release publishes:

- Linux AMD64 and ARM64 static archives and checksums;
- SPDX JSON SBOMs and raw `govulncheck` evidence;
- a multi-architecture `ghcr.io/liu1700/juicefs-plori` image with provenance
  and an SBOM;
- `build-info.json`, which records the source revision, Go version, build tags,
  image name, and immutable image digest.

The workflow tests Redis + S3 format and mount, FUSE I/O, and the remote
durability barrier. It rejects reachable Go vulnerabilities and fixed HIGH or
CRITICAL image vulnerabilities.

Temporary Go vulnerability exceptions live in
`.github/security/plori-vuln-waivers.json`. Every exception must match one
artifact and vulnerability ID exactly and include an expiration date and a
reason. Expired, duplicate, unused, or overly broad exceptions fail the build.

Production manifests must use the image digest from `build-info.json`, not a
mutable tag. Keep the existing Redis metadata URL, S3 bucket URL, credentials,
mount flags, and mount path when replacing the Community Edition mount image.
