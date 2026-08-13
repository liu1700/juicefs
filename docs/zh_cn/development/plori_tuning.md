---
title: Plori 不可变 chunk 配置
---

这套配置只适用于 Plori/Orlop 数据路径。Orlop 把不可变、按内容寻址的 FastCDC
chunk 存成独立文件，通过 `临时文件 + fsync + rename` 写入，整块读取，最后 unlink。
因此不能直接套用通用文件系统的调优结论。

## 选定配置

正式生产配置继续使用 4 MiB block size。新卷也应显式指定，避免选择发生漂移：

```shell
juicefs format \
  --storage s3 \
  --bucket "$S3_BUCKET" \
  --block-size 4M \
  "$META_URL" plori-orlop-v2
```

初始挂载参数如下，metrics 只监听私有地址：

```shell
juicefs mount \
  --writeback \
  --max-uploads 20 \
  --buffer-size 300M \
  --cache-size 10G \
  --cache-large-write=false \
  --prefetch 0 \
  --max-readahead 16M \
  --metrics 127.0.0.1:9567 \
  "$META_URL" /mnt/juicefs
```

上传并发显式保持已测基线 20，不假设并发越高越快。选定的 300 MiB buffer 能
容纳 20 个 8 MiB 上传并保留 140 MiB 工作余量，因此不在无证据时增加每个
mount 的内存。Orlop
总是整块读取，所以关闭 prefetch，16 MiB readahead 已覆盖最大 chunk。10 GiB
持久缓存继续承担 writeback staging 与重启恢复；不能为了避免和 Orlop 客户端
重复缓存而直接设成 0，因为 `cache-size=0` 也会关闭 JuiceFS writeback。

每次正常下线必须先完成远端持久化屏障：

```shell
juicefs durability --timeout 10m /mnt/juicefs
juicefs umount --flush /mnt/juicefs
```

## 证据和基准工具

`hack/plori-benchmark/corpus.json` 来自 Orlop commit
`b11e03e804fcff376749d9ab96e193cbb3e66b6b` 的 FastCDC 实现，共 131 个 chunk、
512 MiB。少于名义 1 MiB 下限的值是每个输入的末尾 chunk。

| JuiceFS block size | 预计数据对象数 | 相对 4 MiB |
| --- | ---: | ---: |
| 4 MiB | 202 | 基线 |
| 8 MiB | 137 | -32.2% |
| 16 MiB | 131 | -35.1% |

8 MiB 已消除相对 4 MiB 的 32.2% 请求放大；16 MiB 只再减少 4.4%，同时用到
JuiceFS 最大 block size，因此只有 8 MiB 值得作为迁移候选。ARM64
Docker/MinIO 功能验证中，三个实测 PUT 数都和预测完全一致，所有 durability
屏障均为零上传失败。轮换顺序后的三轮中位数如下：

| Block | PUT | 写 MiB/s | 首次读 MiB/s | 写 p99 ms |
| --- | ---: | ---: | ---: | ---: |
| 4 MiB | 202 | 275.4 | 801.9 | 64.0 |
| 8 MiB | 137 | 179.8 | 633.7 | 89.7 |
| 16 MiB | 131 | 227.0 | 824.1 | 104.8 |

共享本地 MinIO 噪声较大，而且 writeback cache 存在时读取阶段的 S3 GET 为 0，
所以这些数字不能代表生产或冷读。不过两个大块候选都越过了预设写入或 p99
回滚线，工具正确保留 4 MiB。现有 Plori 观测报告为：持久化顺序写 196.9 MB/s，
首次/热读 16.3-16.7 MB/s。本次发布继续使用 4 MiB，直到同机生产灰度证明
8 MiB 满足全部门槛。

创建三个全新的临时卷和挂载点，复制并修改
`hack/plori-benchmark/targets.example.json`，然后运行：

```shell
make test.plori.benchmark
python3 hack/plori-benchmark/benchmark.py \
  --targets /secure/path/targets.json \
  --repetitions 5 \
  --work-dir /local-nvme/plori-bench \
  --report-json /secure/path/plori-benchmark.json \
  --report-md /secure/path/plori-benchmark.md
```

工具会计时执行 Orlop 写入模式、durability 屏障、首次和热整块读取以及删除，
并记录 p50/p95/p99、吞吐、PUT/GET/DELETE 数量和字节、block cache、上传并发、
writeback 深度与最老年龄。可用 node-exporter 时设置 `node_metrics_url`，以便在
JuiceFS 进程 CPU/内存之外同时记录本地磁盘计数。target 可配置 argv 数组
`recovery_command` 和 `cold_prepare_command`，其中 `{mount}` 会替换为挂载点，
命令不会经过 shell。只能在隔离的基准机器上用它们重启带 staging 数据的挂载
或在首次读取前清缓存。

写入前工具会读取挂载点受保护的 `.config`，如果卷的真实 format block size 与
target 声明不一致就直接拒绝。应以挂载 owner（通常是 root）运行，并把 JSON 中
的环境和版本记录与基准证据一起保存。

生产决策至少运行五轮。每轮会轮换 target 顺序，摘要和推荐使用中位数，JSON
仍保留每一轮原始结果。

## 灰度与回滚门槛

只有直接 JuiceFS 基准和 Orlop 端到端灰度同时满足以下条件，才能创建并推广
8 MiB 候选卷：

- 预计和实测 PUT 数量不高于 8 MiB 结果；
- 写和首次读吞吐不低于同机 4 MiB 基线的 90%，同时不低于历史生产下限
  177 MB/s 写、14.7 MB/s 读；
- 写 p99 和 durability 时间不超过同机 4 MiB 基线的 120%；
- object request error 和 failed upload 始终为 0；
- 最老 pending upload 小于 60 秒，下线期间也必须满足；
- 重启后能在 Pod termination budget 内清空已有 staging；
- Orlop 的写、读、GC p95/p99 回退不超过 10%。

门槛失败时先回滚挂载参数。只有上传并发连续 10 分钟超过 90%，且缓存盘、S3
延迟、CPU 和内存仍有余量时，才同时基准调整 `max-uploads` 与 `buffer-size`。
禁止原地修改 block size。

## 新卷迁移与兼容性

Block size 存在卷元数据中，格式化后不可修改。本次发布不迁移当前卷。若以后
8 MiB 生产灰度通过全部门槛，应使用新的 Redis namespace 和 S3 prefix/bucket，
用同一个已发布 Plori 镜像同时挂载旧卷和新卷，批量复制 Orlop 不可变对象。
停止写入后先对旧卷执行 durability 屏障，再复制并校验最后增量，仅把一个
Orlop 灰度实例切到新挂载。回滚窗口结束前旧卷保持只读。

候选 8 MiB 在 JuiceFS 已支持范围内，不需要提高元数据版本或 `MinClientVersion`。
但运维上 format、迁移和 mount 必须锁定同一个不可变镜像 digest，不能让旧客户端
参与在线 writeback 和下线流程。

导入 `deploy/plori/grafana-dashboard.json` 和
`deploy/plori/prometheus-rules.yaml`。规则会对上传失败、积压/屏障过旧、S3 请求
错误，以及当前 20 上传并发持续饱和发出告警。
