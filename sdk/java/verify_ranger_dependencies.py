#!/usr/bin/env python3
import pathlib
import re
import sys
import xml.etree.ElementTree as ET


MINIMUM_RANGER_VERSION = (2, 8, 0)
REQUIRED_ARTIFACTS = {
    "ranger-audit-core",
    "ranger-authz-api",
    "ranger-plugin-classloader",
    "ranger-plugins-common",
    "ranger-plugins-cred",
    "ugsync-util",
}
MAVEN_NAMESPACE = {"m": "http://maven.apache.org/POM/4.0.0"}


def parse_version(value: str) -> tuple[int, int, int] | None:
    match = re.fullmatch(r"([0-9]+)\.([0-9]+)\.([0-9]+)", value)
    return tuple(int(part) for part in match.groups()) if match else None


def verify_pom(path: pathlib.Path) -> tuple[str | None, list[str]]:
    errors = []
    source = path.read_text(encoding="utf-8")
    root = ET.fromstring(source)
    properties = root.find("m:properties", MAVEN_NAMESPACE)
    version_nodes = (
        properties.findall("m:ranger.version", MAVEN_NAMESPACE)
        if properties is not None
        else []
    )
    if len(version_nodes) != 1 or not version_nodes[0].text:
        return None, ["pom.xml must define ranger.version exactly once"]
    ranger_version = version_nodes[0].text.strip()
    parsed_version = parse_version(ranger_version)
    if parsed_version is None or parsed_version < MINIMUM_RANGER_VERSION:
        errors.append(
            "ranger.version must be at least "
            + ".".join(str(part) for part in MINIMUM_RANGER_VERSION)
        )

    dependencies = []
    for dependency in root.findall("m:dependencies/m:dependency", MAVEN_NAMESPACE):
        group = dependency.findtext("m:groupId", default="", namespaces=MAVEN_NAMESPACE).strip()
        if group != "org.apache.ranger":
            continue
        artifact = dependency.findtext(
            "m:artifactId", default="", namespaces=MAVEN_NAMESPACE
        ).strip()
        version = dependency.findtext(
            "m:version", default="", namespaces=MAVEN_NAMESPACE
        ).strip()
        dependencies.append((artifact, version))
        if version != "${ranger.version}":
            errors.append(
                f"org.apache.ranger:{artifact} must use ${{ranger.version}}, not {version!r}"
            )

    artifacts = {artifact for artifact, _ in dependencies}
    missing = sorted(REQUIRED_ARTIFACTS - artifacts)
    if missing:
        errors.append(f"pom.xml is missing required Ranger modules: {', '.join(missing)}")
    if "2.3.0" in source:
        errors.append("pom.xml still references vulnerable Ranger 2.3.0")
    return ranger_version, errors


def resolved_ranger_dependencies(source: str) -> list[tuple[str, str]]:
    dependencies = []
    for line in source.splitlines():
        if "org.apache.ranger:" not in line:
            continue
        coordinate = line.split("org.apache.ranger:", 1)[1].split()[0]
        fields = coordinate.rstrip(",").split(":")
        if len(fields) < 4:
            continue
        dependencies.append((fields[0], fields[-2]))
    return dependencies


def verify_tree(path: pathlib.Path, expected_version: str) -> list[str]:
    source = path.read_text(encoding="utf-8")
    errors = []
    dependencies = resolved_ranger_dependencies(source)
    if not dependencies:
        return ["Maven dependency tree contains no org.apache.ranger modules"]
    mismatched = sorted(
        f"{artifact}:{version}"
        for artifact, version in dependencies
        if version != expected_version
    )
    if mismatched:
        errors.append(
            f"resolved Ranger modules must all be {expected_version}: {', '.join(mismatched)}"
        )
    resolved_artifacts = {artifact for artifact, _ in dependencies}
    missing = sorted(REQUIRED_ARTIFACTS - resolved_artifacts)
    if missing:
        errors.append(f"dependency tree is missing Ranger modules: {', '.join(missing)}")
    if "org.apache.ranger" in source and "2.3.0" in source:
        errors.append("dependency tree still resolves vulnerable Ranger 2.3.0")
    return errors


def main() -> int:
    if len(sys.argv) not in (2, 3):
        print(
            f"usage: {pathlib.Path(sys.argv[0]).name} POM_XML [DEPENDENCY_TREE]",
            file=sys.stderr,
        )
        return 2
    version, errors = verify_pom(pathlib.Path(sys.argv[1]))
    if len(sys.argv) == 3 and version is not None:
        errors.extend(verify_tree(pathlib.Path(sys.argv[2]), version))
    if errors:
        for error in errors:
            print(f"Ranger dependency verification failed: {error}", file=sys.stderr)
        return 1
    tree_status = " and resolved dependency tree" if len(sys.argv) == 3 else ""
    print(f"Ranger {version} POM{tree_status} verified")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ET.ParseError) as error:
        print(f"Ranger dependency verification failed: {error}", file=sys.stderr)
        raise SystemExit(2)
