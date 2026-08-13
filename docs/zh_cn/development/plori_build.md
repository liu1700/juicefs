---
title: Plori 最小构建配置
---

`plori` 配置是 Plori runtime 和 Orlop 专用的 JuiceFS 客户端。它只保留当前
部署契约需要的能力：

- 通过 `redis` 驱动使用兼容 Redis 的元数据服务；
- 通过 `s3` 驱动使用 S3 或兼容 S3 的对象存储；
- FUSE 客户端，以及格式化、挂载、卸载、状态检查、`durability` 远端持久化
  屏障和目录配额管理所需的运维命令。

SQL 和 KV 元数据引擎、S3 gateway、WebDAV、本地和内存对象存储，以及非 S3
对象存储均不会编入此产物。不要把它当作通用 Community Edition 发行版使用。

## 构建与验证

使用 `.go-version` 指定的 Go 版本执行：

```shell
make -B juicefs.plori VERSION=dev
make test.plori.profile
hack/verify-plori-binary.sh ./juicefs.plori
```

如果运行时注册了 Redis 以外的元数据引擎或 S3 以外的对象存储，
`make test.plori.profile` 会失败。二进制验证脚本还会拒绝被裁剪后端家族的依赖。

容器使用固定摘要的多架构基础镜像和固定版本的软件包：

```shell
docker build -f Dockerfile.plori -t juicefs-plori:dev .
```

运行镜像只包含静态客户端、CA 证书、FUSE 3、POSIX shell 和 `tini`，这是
Plori CSI mounter 所需的最小镜像契约。

## 发布与安全契约

匹配 `vX.Y.Z-plori.N` 的标签会触发 `.github/workflows/plori.yml`。发布成功后
会生成：

- Linux AMD64 和 ARM64 静态压缩包及校验和；
- SPDX JSON SBOM 和原始 `govulncheck` 证据；
- 带 provenance 和 SBOM 的多架构
  `ghcr.io/liu1700/juicefs-plori` 镜像；
- 记录源码版本、Go 版本、构建标签、镜像名和不可变镜像摘要的
  `build-info.json`。

流水线会验证 Redis + S3 格式化与挂载、FUSE I/O 和远端持久化屏障，并拒绝
可达的 Go 漏洞以及镜像中已有修复的 HIGH/CRITICAL 漏洞。

临时 Go 漏洞豁免位于 `.github/security/plori-vuln-waivers.json`。每条豁免
必须精确匹配一个产物和一个漏洞 ID，并包含到期日和原因。过期、重复、未使用
或范围过宽的豁免都会导致构建失败。

生产部署必须使用 `build-info.json` 中的镜像摘要，不能使用可变标签。替换
Community Edition mount 镜像时，应保持现有 Redis 元数据 URL、S3 bucket URL、
凭据、挂载参数和挂载路径不变。

新卷和灰度验证请使用 [Plori 不可变 chunk 配置](./plori_tuning.md)。它会明确记录
block size 决策；任何候选变更都只能用于新格式化的卷。
