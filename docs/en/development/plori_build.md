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

The Hadoop/Java SDK and Ranger authorization plugin are also outside this
fork's supported security boundary. They are not copied into the container
build context, built, scanned, or published. Do not build or deploy
`sdk/java` from this fork: its legacy synthesized UID/GID path can map distinct
names to the same POSIX owner. Supporting Hadoop later requires an
authoritative persistent name-to-ID mapping that rejects unknown names, plus a
collision audit and an explicit ownership migration. Existing metadata must
never be silently reassigned.

## Build and verify

Use the Go version in `.go-version` and run:

```shell
make -B juicefs.plori VERSION=dev
make test.plori.profile
make test.plori.security
hack/verify-plori-binary.sh ./juicefs.plori
```

`make test.plori.profile` fails if any metadata engine other than Redis or any
object storage driver other than S3 is registered. The binary verifier also
rejects dependencies belonging to excluded backend families. The security
test verifies the restricted Docker build context, `nohdfs` build tag,
workflow commands, SBOM denylist, and release-file allowlist.

The container build uses pinned multi-architecture base images and package
versions:

```shell
docker build -f Dockerfile.plori -t juicefs-plori:dev .
```

The runtime image contains the static client, CA certificates, FUSE 3, a POSIX
shell, and `tini`. JuiceFS CSI v0.32 CE mount pods execute
`/bin/mount.juicefs`, while its binary-upgrade job copies
`/usr/local/bin/juicefs`. Both paths resolve to the same supported static
client. Verify that exact image contract with:

```shell
hack/verify-plori-csi-image.sh juicefs-plori:dev
```

The verifier also checks the shell and core utilities used by the CSI mount-pod
lifecycle. A successful check is required for pull-request and release images.

## Release and security contract

Tags matching `vX.Y.Z-plori.N` run `.github/workflows/plori.yml`. A successful
release publishes:

- Linux AMD64 and ARM64 static archives and checksums;
- SPDX JSON SBOMs and raw `govulncheck` evidence;
- a multi-architecture `ghcr.io/liu1700/juicefs-plori` image with provenance
  and an SBOM;
- `build-info.json`, which records the source revision, Go version, build tags,
  image name, immutable image digest, and the machine-readable support policy.

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

Use the [Plori immutable-chunk profile](./plori_tuning.md) for new volumes and
canary validation. It makes the block-size decision explicit; any candidate
change applies only to a newly formatted volume.
