#!/usr/bin/env python3
import importlib.util
import pathlib
import tempfile
import textwrap
import unittest


SCRIPT = pathlib.Path(__file__).with_name("verify_ranger_dependencies.py")
SPEC = importlib.util.spec_from_file_location("verify_ranger_dependencies", SCRIPT)
verify = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(verify)


def pom(version: str = "2.8.0", missing: str | None = None, literal: bool = False) -> str:
    dependencies = []
    for artifact in sorted(verify.REQUIRED_ARTIFACTS):
        if artifact == missing:
            continue
        dependency_version = version if literal else "${ranger.version}"
        dependencies.append(
            f"""
            <dependency>
              <groupId>org.apache.ranger</groupId>
              <artifactId>{artifact}</artifactId>
              <version>{dependency_version}</version>
            </dependency>
            """
        )
    return textwrap.dedent(
        f"""\
        <project xmlns="http://maven.apache.org/POM/4.0.0">
          <modelVersion>4.0.0</modelVersion>
          <properties><ranger.version>{version}</ranger.version></properties>
          <dependencies>{''.join(dependencies)}</dependencies>
        </project>
        """
    )


class RangerDependencyTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def write(self, name: str, source: str) -> pathlib.Path:
        path = self.root / name
        path.write_text(source, encoding="utf-8")
        return path

    def test_repository_pom_uses_safe_unified_version(self):
        repository_pom = pathlib.Path(__file__).with_name("pom.xml")
        version, errors = verify.verify_pom(repository_pom)
        self.assertEqual(version, "2.8.0")
        self.assertEqual(errors, [])

    def test_safe_pom_is_valid(self):
        version, errors = verify.verify_pom(self.write("pom.xml", pom()))
        self.assertEqual(version, "2.8.0")
        self.assertEqual(errors, [])

    def test_vulnerable_baseline_is_rejected(self):
        _, errors = verify.verify_pom(self.write("pom.xml", pom(version="2.3.0")))
        self.assertTrue(any("at least" in error for error in errors))
        self.assertTrue(any("still references" in error for error in errors))

    def test_literal_ranger_version_is_rejected(self):
        _, errors = verify.verify_pom(self.write("pom.xml", pom(literal=True)))
        self.assertTrue(any("must use ${ranger.version}" in error for error in errors))

    def test_missing_audit_module_is_rejected(self):
        _, errors = verify.verify_pom(
            self.write("pom.xml", pom(missing="ranger-audit-core"))
        )
        self.assertTrue(any("ranger-audit-core" in error for error in errors))

    def test_matching_dependency_tree_is_valid(self):
        tree = "\n".join(
            f"[INFO] +- org.apache.ranger:{artifact}:jar:2.8.0:compile"
            for artifact in sorted(verify.REQUIRED_ARTIFACTS)
        )
        self.assertEqual(verify.verify_tree(self.write("tree.txt", tree), "2.8.0"), [])

    def test_transitive_downgrade_is_rejected(self):
        tree = "\n".join(
            f"[INFO] +- org.apache.ranger:{artifact}:jar:"
            f"{'2.3.0' if artifact == 'ranger-audit-core' else '2.8.0'}:compile"
            for artifact in sorted(verify.REQUIRED_ARTIFACTS)
        )
        errors = verify.verify_tree(self.write("tree.txt", tree), "2.8.0")
        self.assertTrue(any("ranger-audit-core:2.3.0" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
