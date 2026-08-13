#!/bin/sh
set -eu

destination=${1:-/usr/local/bin}
syft_version=1.50.0
trivy_version=0.73.0

case "$(uname -m)" in
    x86_64)
        syft_arch=amd64
        syft_sha=bf7b29ff57f06da30918266a0e1c2885a8f99784798d1bdb1628886aa015d788
        trivy_arch=64bit
        trivy_sha=2edd39da482bb4e9831962487b68f68e3928ec3137794757f54d00383d79547b
        ;;
    aarch64|arm64)
        syft_arch=arm64
        syft_sha=887c57cbcc2d0e8c5c110a4571a3fc7150058b24d74f993ee4663516e5c8ce86
        trivy_arch=ARM64
        trivy_sha=13833d97e8a1a5367471c372a173180157f593bece570e20d5d925fef552f5dd
        ;;
    *)
        echo "unsupported scanner architecture: $(uname -m)" >&2
        exit 1
        ;;
esac

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM

syft_archive="syft_${syft_version}_linux_${syft_arch}.tar.gz"
curl --fail --location --proto '=https' --tlsv1.2 \
    "https://github.com/anchore/syft/releases/download/v${syft_version}/${syft_archive}" \
    --output "$tmp_dir/$syft_archive"
printf '%s  %s\n' "$syft_sha" "$tmp_dir/$syft_archive" | sha256sum --check --status
tar -xzf "$tmp_dir/$syft_archive" -C "$tmp_dir" syft
install -m 0755 "$tmp_dir/syft" "$destination/syft"

trivy_archive="trivy_${trivy_version}_Linux-${trivy_arch}.tar.gz"
curl --fail --location --proto '=https' --tlsv1.2 \
    "https://github.com/aquasecurity/trivy/releases/download/v${trivy_version}/${trivy_archive}" \
    --output "$tmp_dir/$trivy_archive"
printf '%s  %s\n' "$trivy_sha" "$tmp_dir/$trivy_archive" | sha256sum --check --status
tar -xzf "$tmp_dir/$trivy_archive" -C "$tmp_dir" trivy
install -m 0755 "$tmp_dir/trivy" "$destination/trivy"

"$destination/syft" version
"$destination/trivy" --version
