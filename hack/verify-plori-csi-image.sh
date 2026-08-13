#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $(basename "$0") IMAGE [PLATFORM]" >&2
  exit 2
fi

image=$1
platform=${2:-}
docker_args=(run --rm)
if [[ -n "$platform" ]]; then
  docker_args+=(--platform "$platform")
fi

docker "${docker_args[@]}" --entrypoint /bin/sh "$image" -ec '
  require_executable() {
    if ! test -x "$1"; then
      echo "required executable is missing: $1" >&2
      exit 1
    fi
  }

  require_link() {
    if ! test "$(readlink "$1")" = /usr/local/bin/juicefs; then
      echo "required link does not target /usr/local/bin/juicefs: $1" >&2
      exit 1
    fi
  }

  require_executable /usr/local/bin/juicefs
  require_executable /bin/juicefs
  require_executable /bin/mount.juicefs
  require_link /bin/juicefs
  require_link /bin/mount.juicefs
  /usr/local/bin/juicefs version >/dev/null

  for command in sh cp ln rm mkdir rmdir sleep stat timeout cat grep mount umount; do
    if ! command -v "$command" >/dev/null; then
      echo "required CSI lifecycle command is missing: $command" >&2
      exit 1
    fi
  done
'

echo "Plori CSI image contract verified for $image${platform:+ ($platform)}"
