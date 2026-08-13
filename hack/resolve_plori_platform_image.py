#!/usr/bin/env python3
import json
import re
import sys


DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
PLATFORM = re.compile(r"^(linux)/(amd64|arm64)$")


def resolve(document: dict, image: str, platform: str) -> str:
    match = PLATFORM.fullmatch(platform)
    if not match:
        raise ValueError(f"unsupported Plori image platform: {platform}")
    if "@" not in image:
        raise ValueError("published Plori image must be referenced by index digest")

    repository, index_digest = image.rsplit("@", 1)
    if not repository or not DIGEST.fullmatch(index_digest):
        raise ValueError("published Plori image has an invalid index digest")

    operating_system, architecture = match.groups()
    candidates = []
    for manifest in document.get("manifests", []):
        target = manifest.get("platform", {})
        if (
            target.get("os") == operating_system
            and target.get("architecture") == architecture
        ):
            digest = manifest.get("digest", "")
            if not DIGEST.fullmatch(digest):
                raise ValueError(f"{platform} manifest has an invalid digest")
            candidates.append(digest)

    if len(candidates) != 1:
        raise ValueError(
            f"expected exactly one {platform} manifest, found {len(candidates)}"
        )
    return f"{repository}@{candidates[0]}"


def main() -> int:
    if len(sys.argv) != 3:
        print(
            f"usage: {sys.argv[0]} IMAGE@sha256:<index> linux/ARCH < index.json",
            file=sys.stderr,
        )
        return 2
    try:
        document = json.load(sys.stdin)
        print(resolve(document, sys.argv[1], sys.argv[2]))
    except (json.JSONDecodeError, OSError, TypeError, ValueError) as error:
        print(f"Plori platform image resolution failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
