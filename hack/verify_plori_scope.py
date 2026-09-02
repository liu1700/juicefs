#!/usr/bin/env python3
import json
import pathlib
import re
import shlex
import sys


EXPECTED_CONTEXT = {
    "Makefile",
    "go.mod",
    "go.sum",
    "main.go",
    "cmd",
    "pkg",
}
EXPECTED_DOCKERIGNORE = {
    "**",
    "!Makefile",
    "!go.mod",
    "!go.sum",
    "!main.go",
    "!cmd/",
    "!cmd/**",
    "!pkg/",
    "!pkg/**",
}
EXPECTED_POLICY = {
    "schemaVersion": 1,
    "profile": "redis-sqlite3-s3-fuse",
    "supportedInterfaces": ["fuse"],
    "supportedMetadataEngines": ["redis", "sqlite3"],
    "supportedObjectStores": ["s3"],
    "excludedSecurityDomains": ["hadoop-java-sdk", "ranger-authorization"],
}
FORBIDDEN_WORKFLOW = re.compile(
    r"(?:sdk/java|juicefs-hadoop|org\.apache\.(?:hadoop|ranger)|setup-java|"
    r"\.jar(?:[\s'\"]|$)|(?:^|[\s/])(?:java|javac|mvn|mvnw|maven|gradle|"
    r"gradlew|jar|openjdk)(?:[\s/:@]|$))",
    re.IGNORECASE | re.MULTILINE,
)
# `sqlite_omit_load_extension` compiles out `sqlite3_enable_load_extension` and
# the `load_extension()` SQL function (mattn/go-sqlite3
# `sqlite3_load_extension_omit.go:6,12` -> `-DSQLITE_OMIT_LOAD_EXTENSION`), so a
# per-Agent metadata DB cannot be turned into a code-loading primitive.
REQUIRED_TAGS = {"plori", "nohdfs", "sqlite_omit_load_extension"}
# Build tags that would compile out an engine the support policy promises.
FORBIDDEN_TAGS = {"nosqlite"}
# Metadata engine -> the build tag that removes it.
ENGINE_EXCLUSION_TAG = {
    "sqlite3": "nosqlite",
    "mysql": "nomysql",
    "postgres": "nopg",
    "tikv": "notikv",
    "etcd": "noetcd",
    "badger": "nobadger",
}
EXPECTED_STAGE_COPIES = {
    ("build", "/src/juicefs.plori", "/juicefs"),
    ("scan-build", "/src/juicefs.plori.scan", "/juicefs.scan"),
    ("build", "/src/juicefs.plori", "/usr/local/bin/juicefs"),
}
EXPECTED_CSI_LINK = "ln -s /usr/local/bin/juicefs /bin/mount.juicefs"


def docker_instructions(path: pathlib.Path) -> list[str]:
    instructions = []
    current = ""
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        current = f"{current} {line}".strip()
        if current.endswith("\\"):
            current = current[:-1].rstrip()
            continue
        instructions.append(current)
        current = ""
    if current:
        instructions.append(current)
    return instructions


def copied_sources(
    path: pathlib.Path,
) -> tuple[set[str], set[tuple[str, str, str]], list[str]]:
    sources = set()
    stage_copies = set()
    errors = []
    for instruction in docker_instructions(path):
        if not instruction.upper().startswith("COPY "):
            continue
        try:
            fields = shlex.split(instruction)
        except ValueError as error:
            errors.append(f"cannot parse Dockerfile instruction {instruction!r}: {error}")
            continue
        from_fields = [
            field.removeprefix("--from=")
            for field in fields[1:]
            if field.startswith("--from=")
        ]
        operands = [field for field in fields[1:] if not field.startswith("--")]
        if len(operands) < 2:
            errors.append(f"invalid Dockerfile COPY instruction: {instruction}")
            continue
        if from_fields:
            if len(from_fields) != 1 or len(operands) != 2:
                errors.append(f"invalid stage COPY instruction: {instruction}")
            else:
                stage_copies.add((from_fields[0], operands[0], operands[1]))
            continue
        for source in operands[:-1]:
            normalized = source.removeprefix("./").rstrip("/")
            sources.add(normalized)
            if any(marker in source for marker in ("*", "?", "[")):
                errors.append(f"Dockerfile COPY source must not use a wildcard: {source}")
    return sources, stage_copies, errors


def verify(root: pathlib.Path) -> list[str]:
    errors = []
    csi_verifier = root / "hack/verify-plori-csi-image.sh"
    if not csi_verifier.is_file() or not csi_verifier.stat().st_mode & 0o111:
        errors.append("Plori CSI image verifier must exist and be executable")

    policy_path = root / ".github/security/plori-support-policy.json"
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    if policy != EXPECTED_POLICY:
        errors.append("Plori support policy differs from the audited Redis + SQLite + S3 + FUSE contract")

    dockerfile_path = root / "Dockerfile.plori"
    context_sources, stage_copies, docker_errors = copied_sources(dockerfile_path)
    errors.extend(docker_errors)
    if context_sources != EXPECTED_CONTEXT:
        errors.append(
            "Dockerfile.plori context sources must be exactly: "
            + ", ".join(sorted(EXPECTED_CONTEXT))
        )
    if stage_copies != EXPECTED_STAGE_COPIES:
        errors.append("Dockerfile.plori stage copies must contain only the audited Plori binaries")
    forbidden = FORBIDDEN_WORKFLOW.search(dockerfile_path.read_text(encoding="utf-8"))
    if forbidden:
        errors.append(
            "Dockerfile.plori contains an excluded build or runtime path: "
            f"{forbidden.group(0)!r}"
        )
    if not any(
        EXPECTED_CSI_LINK in instruction
        for instruction in docker_instructions(dockerfile_path)
    ):
        errors.append(
            "Dockerfile.plori must link /bin/mount.juicefs to the supported client"
        )

    dockerignore = {
        line.strip()
        for line in (root / "Dockerfile.plori.dockerignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    if dockerignore != EXPECTED_DOCKERIGNORE:
        errors.append("Dockerfile.plori.dockerignore must expose only the audited Plori Go build inputs")

    makefile = (root / "Makefile").read_text(encoding="utf-8")
    tags_match = re.search(r"^PLORI_TAGS\s*:=\s*(.+)$", makefile, re.MULTILINE)
    tags = set(tags_match.group(1).split(",")) if tags_match else set()
    if not REQUIRED_TAGS.issubset(tags):
        errors.append(
            "PLORI_TAGS must include " + ", ".join(sorted(REQUIRED_TAGS))
        )
    # The support policy and the build tags must not be able to drift apart.
    # A binary built with `nosqlite` has no SQLite driver, yet would still
    # report the Plori version and ship a policy that promises `sqlite3`.
    forbidden_tags = sorted(FORBIDDEN_TAGS & tags)
    if forbidden_tags:
        errors.append(
            "PLORI_TAGS must not exclude a supported metadata engine: "
            + ", ".join(forbidden_tags)
        )
    for engine, exclusion in ENGINE_EXCLUSION_TAG.items():
        supported = engine in EXPECTED_POLICY["supportedMetadataEngines"]
        excluded = exclusion in tags
        if supported == excluded:
            errors.append(
                f"metadata engine {engine!r} is "
                + ("supported by the policy" if supported else "not in the policy")
                + f" but the build tag {exclusion!r} says otherwise"
            )

    workflow = (root / ".github/workflows/plori.yml").read_text(encoding="utf-8")
    forbidden = FORBIDDEN_WORKFLOW.search(workflow)
    if forbidden:
        errors.append(f"Plori workflow contains an excluded build or publish path: {forbidden.group(0)!r}")
    if not any(
        command in workflow
        for command in ("python3 hack/verify_plori_scope.py", "make test.plori.security")
    ):
        errors.append("Plori workflow does not run the support-scope verifier")
    if "python3 hack/verify_plori_release.py" not in workflow:
        errors.append("Plori workflow does not verify the release directory allowlist")
    if workflow.count("hack/verify-plori-csi-image.sh") < 2:
        errors.append("Plori workflow must verify the CSI image contract before release")
    if "hack/resolve_plori_platform_image.py" not in workflow:
        errors.append("Plori workflow must resolve exact platform manifests before scanning")
    return errors


def main() -> int:
    root = (
        pathlib.Path(sys.argv[1]).resolve()
        if len(sys.argv) == 2
        else pathlib.Path(__file__).resolve().parents[1]
    )
    if len(sys.argv) > 2:
        print(f"usage: {pathlib.Path(sys.argv[0]).name} [REPOSITORY]", file=sys.stderr)
        return 2
    errors = verify(root)
    if errors:
        for error in errors:
            print(f"Plori scope verification failed: {error}", file=sys.stderr)
        return 1
    print("Plori support scope verified (Redis + SQLite + S3 + FUSE; Hadoop/Java excluded)")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"Plori scope verification failed: {error}", file=sys.stderr)
        raise SystemExit(2)
