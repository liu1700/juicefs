#!/usr/bin/env python3
import json
import pathlib
import re
import sys
import tarfile


ARCHITECTURES = ("amd64", "arm64")


def expected_files(version: str) -> set[str]:
    files = {"build-info.json"}
    for arch in ARCHITECTURES:
        binary = f"juicefs-{version}-linux-{arch}"
        files.update(
            {
                binary,
                f"{binary}.tar.gz",
                f"juicefs-linux-{arch}.spdx.json",
                f"juicefs-plori-container-{arch}.spdx.json",
                f"govulncheck-linux-{arch}.json",
                f"trivy-image-{arch}.json",
            }
        )
    return files


def verify(directory: pathlib.Path, version: str, policy_path: pathlib.Path) -> list[str]:
    errors = []
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+-plori\.[0-9]+", version):
        return [f"invalid Plori version: {version}"]
    actual = {path.name for path in directory.iterdir()}
    expected = expected_files(version)
    if actual != expected:
        unexpected = sorted(actual - expected)
        missing = sorted(expected - actual)
        if unexpected:
            errors.append(f"unexpected release files: {', '.join(unexpected)}")
        if missing:
            errors.append(f"missing release files: {', '.join(missing)}")

    for arch in ARCHITECTURES:
        binary = f"juicefs-{version}-linux-{arch}"
        archive = directory / f"{binary}.tar.gz"
        if not archive.is_file():
            continue
        try:
            with tarfile.open(archive, mode="r:gz") as bundle:
                members = bundle.getmembers()
                if len(members) != 1 or members[0].name != binary or not members[0].isfile():
                    errors.append(f"{archive.name} must contain only the regular file {binary}")
        except tarfile.TarError as error:
            errors.append(f"cannot read {archive.name}: {error}")

    metadata_path = directory / "build-info.json"
    if metadata_path.is_file():
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        policy = json.loads(policy_path.read_text(encoding="utf-8"))
        if metadata.get("version") != version:
            errors.append("build-info.json version does not match the release")
        if metadata.get("supportPolicy") != policy:
            errors.append("build-info.json does not embed the audited support policy")
    return errors


def main() -> int:
    if len(sys.argv) != 4:
        print(
            f"usage: {pathlib.Path(sys.argv[0]).name} DIST VERSION POLICY_JSON",
            file=sys.stderr,
        )
        return 2
    errors = verify(
        pathlib.Path(sys.argv[1]),
        sys.argv[2],
        pathlib.Path(sys.argv[3]),
    )
    if errors:
        for error in errors:
            print(f"Plori release verification failed: {error}", file=sys.stderr)
        return 1
    print("Plori release directory and support policy verified")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"Plori release verification failed: {error}", file=sys.stderr)
        raise SystemExit(2)
