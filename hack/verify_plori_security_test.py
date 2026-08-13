#!/usr/bin/env python3
import importlib.util
import io
import json
import pathlib
import shutil
import tarfile
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]


def load_module(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


scope = load_module("verify_plori_scope", ROOT / "hack/verify_plori_scope.py")
release = load_module("verify_plori_release", ROOT / "hack/verify_plori_release.py")
sbom = load_module("verify_plori_sbom", ROOT / "hack/verify_plori_sbom.py")
platform_image = load_module(
    "resolve_plori_platform_image", ROOT / "hack/resolve_plori_platform_image.py"
)


class ScopeTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp.name)
        for path in (
            ".dockerignore",
            "Dockerfile.plori",
            "Makefile",
            ".github/workflows/plori.yml",
            ".github/security/plori-support-policy.json",
            "hack/verify-plori-csi-image.sh",
        ):
            source = ROOT / path
            target = self.root / path
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)

    def tearDown(self):
        self.temp.cleanup()

    def test_repository_scope_is_valid(self):
        self.assertEqual(scope.verify(self.root), [])

    def test_broad_docker_copy_is_rejected(self):
        dockerfile = self.root / "Dockerfile.plori"
        dockerfile.write_text(dockerfile.read_text() + "\nCOPY . /src\n", encoding="utf-8")
        self.assertTrue(any("context sources" in error for error in scope.verify(self.root)))

    def test_extra_stage_copy_is_rejected(self):
        dockerfile = self.root / "Dockerfile.plori"
        dockerfile.write_text(
            dockerfile.read_text() + "\nCOPY --from=source /tmp/component /opt/component\n",
            encoding="utf-8",
        )
        self.assertTrue(any("stage copies" in error for error in scope.verify(self.root)))

    def test_java_build_is_rejected(self):
        workflow = self.root / ".github/workflows/plori.yml"
        workflow.write_text(workflow.read_text() + "\n# mvn package\n", encoding="utf-8")
        self.assertTrue(any("excluded build" in error for error in scope.verify(self.root)))

    def test_missing_csi_mount_helper_is_rejected(self):
        dockerfile = self.root / "Dockerfile.plori"
        dockerfile.write_text(
            dockerfile.read_text().replace(
                "    && ln -s /usr/local/bin/juicefs /bin/mount.juicefs\n", ""
            ),
            encoding="utf-8",
        )
        self.assertTrue(any("/bin/mount.juicefs" in error for error in scope.verify(self.root)))

    def test_missing_csi_image_workflow_gate_is_rejected(self):
        workflow = self.root / ".github/workflows/plori.yml"
        workflow.write_text(
            workflow.read_text().replace("hack/verify-plori-csi-image.sh", "true #"),
            encoding="utf-8",
        )
        self.assertTrue(any("CSI image contract" in error for error in scope.verify(self.root)))


class SbomTest(unittest.TestCase):
    def valid_document(self):
        return {"packages": [{"name": name} for name in sbom.REQUIRED], "files": []}

    def test_supported_profile_is_valid(self):
        self.assertEqual(sbom.verify(self.valid_document()), ([], [], []))

    def test_hadoop_package_is_rejected(self):
        document = self.valid_document()
        document["packages"].append({"name": "org.apache.hadoop:hadoop-client"})
        self.assertEqual(sbom.verify(document)[1], ["org.apache.hadoop:hadoop-client"])

    def test_jar_file_is_rejected(self):
        document = self.valid_document()
        document["files"].append({"fileName": "/opt/juicefs/juicefs-hadoop.jar"})
        self.assertEqual(sbom.verify(document)[2], ["/opt/juicefs/juicefs-hadoop.jar"])

    def test_unpacked_java_class_is_rejected(self):
        document = self.valid_document()
        document["files"].append({"fileName": "/opt/io/juicefs/JuiceFileSystem.class"})
        self.assertEqual(
            sbom.verify(document)[2],
            ["/opt/io/juicefs/JuiceFileSystem.class"],
        )


class ReleaseTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = pathlib.Path(self.temp.name)
        self.version = "1.5.0-plori.1"
        self.policy = ROOT / ".github/security/plori-support-policy.json"
        for name in release.expected_files(self.version):
            path = self.directory / name
            if name == "build-info.json":
                path.write_text(
                    json.dumps(
                        {
                            "version": self.version,
                            "supportPolicy": json.loads(
                                self.policy.read_text(encoding="utf-8")
                            ),
                        }
                    ),
                    encoding="utf-8",
                )
            elif name.endswith(".tar.gz"):
                binary = name.removesuffix(".tar.gz")
                with tarfile.open(path, mode="w:gz") as bundle:
                    payload = b"binary"
                    info = tarfile.TarInfo(binary)
                    info.size = len(payload)
                    bundle.addfile(info, io.BytesIO(payload))
            else:
                path.write_bytes(b"test")

    def tearDown(self):
        self.temp.cleanup()

    def test_release_allowlist_is_valid(self):
        self.assertEqual(release.verify(self.directory, self.version, self.policy), [])

    def test_jar_asset_is_rejected(self):
        (self.directory / "juicefs-hadoop.jar").write_bytes(b"test")
        errors = release.verify(self.directory, self.version, self.policy)
        self.assertTrue(any("unexpected release files" in error for error in errors))


class PlatformImageTest(unittest.TestCase):
    image = "ghcr.io/liu1700/juicefs-plori@sha256:" + "a" * 64

    def document(self):
        return {
            "manifests": [
                {
                    "digest": "sha256:" + "b" * 64,
                    "platform": {"os": "linux", "architecture": "amd64"},
                },
                {
                    "digest": "sha256:" + "c" * 64,
                    "platform": {"os": "linux", "architecture": "arm64"},
                },
                {
                    "digest": "sha256:" + "d" * 64,
                    "platform": {"os": "unknown", "architecture": "unknown"},
                },
            ]
        }

    def test_resolves_exact_platform_manifest(self):
        self.assertEqual(
            platform_image.resolve(self.document(), self.image, "linux/arm64"),
            "ghcr.io/liu1700/juicefs-plori@sha256:" + "c" * 64,
        )

    def test_missing_platform_is_rejected(self):
        document = self.document()
        document["manifests"] = document["manifests"][:1]
        with self.assertRaisesRegex(ValueError, "found 0"):
            platform_image.resolve(document, self.image, "linux/arm64")

    def test_duplicate_platform_is_rejected(self):
        document = self.document()
        document["manifests"].append(document["manifests"][0].copy())
        with self.assertRaisesRegex(ValueError, "found 2"):
            platform_image.resolve(document, self.image, "linux/amd64")

    def test_mutable_index_reference_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "index digest"):
            platform_image.resolve(
                self.document(), "ghcr.io/liu1700/juicefs-plori:latest", "linux/amd64"
            )


if __name__ == "__main__":
    unittest.main()
