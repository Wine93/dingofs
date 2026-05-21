# DingoFS Connector for LMCache

把 vLLM 生态里的 [LMCache](https://github.com/LMCache/LMCache) 接到一个 dingo-cache 集群，让 KV cache 走 dingofs 的分布式缓存。

同时实现了 LMCache 的两套接口：

| 接口 | 类 | 用途 |
|---|---|---|
| `RemoteConnector` | `DingoFSConnector` | 单远端缓存层（按 URL 寻址：`dingofs://...`） |
| `L2AdapterInterface` | `DingoFSL2Adapter` | 分布式存储层（batched + eventfd 调度） |

设计要点：

- **零拷贝 Put**：Python `memoryview` 通过 `butil::IOBuf::AppendUserData` 直接挂载到 brpc，发送路径无客户端拷贝。
- **端到端 1 次拷贝 Get**：响应 IOBuf 通过 `cutn(dst, len)` 直写 caller buffer —— brpc 框架下的物理下限（等价 mooncake/infinistore）。
- **进程内 LRU 短路 Exists**：put / get 命中即在内存 set 上加键，prefetch 阶段的 batched_async_contains 大部分调用 0 syscall。
- **pybind11 + GIL 释放**：异步提交全程释放 GIL；bthread 完成回调通过 eventfd 唤醒 demux 线程，再 `loop.call_soon_threadsafe` 跳回 asyncio。

## 前置依赖

- 一个跑着的 `dingo-mds` + `dingo-cache` 集群
- Python ≥ 3.9
- LMCache（确保 `lmcache.v1.distributed.l2_adapters.native_connector_l2_adapter` 可 import）
- 编译 dingofs 所需的全部三方库（参考 dingofs 主仓 README）

## 编译与安装

connector 的原生扩展（`_dingofs_native.so`）由 dingofs 主 CMakeLists 编译，通过开关挂载，**默认 OFF**，不影响其他构建。

```bash
# 1. 编 dingofs，开启 LMCache connector
cd dingofs
git submodule sync && git submodule update --init --recursive
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo \
      -DBUILD_UNIT_TESTS=OFF \
      -DBUILD_LMCACHE_CONNECTOR=ON \
      ..
make -j12 _dingofs_native

# `_dingofs_native.so` 现在落在
# dingofs/integration/lmcache/src/dingofs_connector/ 下，
# pip install 时会自动被 package_data 打进 wheel。

# 2. 装 Python 包
cd ../integration/lmcache
pip install -v .
# 或开发模式: pip install -e .
```

### 打 wheel 用于发布到线上

```bash
cd dingofs/integration/lmcache

# strip 一下 .so，把 413 MB 的 debug info 砍掉 → ~35 MB
strip --strip-debug src/dingofs_connector/_dingofs_native*.so

# 打 wheel，落到 ./dist/
python3 -m pip wheel . --no-deps --no-build-isolation -w dist/
# 产出：dist/dingofs_connector-0.1.0-cp39-cp39-linux_x86_64.whl  (~12 MB)
```

scp 到线上机后 `pip install dingofs_connector-*.whl` 即可，不需要装 dingofs / brpc / glog（都已静态链入 `.so`）。

## 配置 LMCache

### A) 作为 RemoteConnector

```yaml
remote_storage_plugins: ["dingofs"]
extra_config:
  remote_storage_plugin.dingofs.module_path: dingofs_connector.adapter
  remote_storage_plugin.dingofs.class_name:  DingoFSConnectorAdapter

remote_url: "dingofs://mds1:6700,mds2:6700,mds3:6700/lmcache_group"
```

### B) 作为 L2 Adapter

`l2_factory` 在包 import 时自动注册类型 `"dingofs"`，配置直接写：

```bash
--l2-adapter '{"type":"dingofs","mds_addrs":"mds1:6700,mds2:6700","cache_group":"lmcache_group"}'
```

或 YAML：

```yaml
l2_adapter:
  type: dingofs
  mds_addrs: "mds1:6700,mds2:6700"
  cache_group: "lmcache_group"
  extra:               # 可选：额外 dingofs gflag 覆盖
    cache_rpc_timeout_ms: "5000"
    cache_rpc_max_retry_times: "3"
```

## URL 语法

```
dingofs://<mds-addrs>/<cache-group>[?gflag=value&gflag2=value2&...]
```

- `<mds-addrs>` 单个 host:port，或多个逗号分隔（仿 redis-sentinel）：
  - `dingofs://mds1:6700/grp`
  - `dingofs://mds1:6700,mds2:6700,mds3:6700/grp`
- `<cache-group>` 服务端预先创建好的 cache group 名
- `?...` 查询参数透传成 dingofs gflag 覆盖（参见 [src/common/options/cache.h](../../src/common/options/cache.h)）

## 烟测客户端（验证集群读写）

`examples/client.py` 是一个 CLI 工具，**走完整的 `DingoFSConnector` 链路**（CacheEngineKey + MemoryObj + asyncio + native module + brpc），但不需要拉起完整的 LMCacheEngine —— 仅用最小 mock 提供 `LocalCPUBackend.allocate` 等几个被实际访问的方法。

适合的场景：起一个 dingo-mds + dingo-cache 集群后，先用它验证读写通路，再去看缓存节点的磁盘上有没有数据落地。

```bash
# 0. 健康探针
python examples/client.py --url dingofs://mds1:6700/lmcache_group ping

# 1. 端到端 round-trip：put 一段 deterministic 数据 → get → 字节比对
python examples/client.py \
    --url dingofs://mds1:6700,mds2:6700/lmcache_group \
    roundtrip --hash 0xcafebabe

# 2. 拆开来验证：先 put，再去集群节点上看文件，最后从客户端 get 校验
python examples/client.py --url dingofs://... put --hash 0xdeadbeef --seed 0x1234

# 此时去任一 cache group 节点：
#   ls -lh /path/to/cache_dir/tensor/<hash[0:2]>/<hash[0:4]>/
# 应能看到 smoke-client@<ws>@<wid>@deadbeef@lmcache 这样命名的文件

python examples/client.py --url dingofs://... get --hash 0xdeadbeef --seed 0x1234 --verify

# 3. exists 探测
python examples/client.py --url dingofs://... exists --hash 0xdeadbeef
```

参数说明：
- `--hash`：模拟 LMCache 的 `chunk_hash`，决定 cache key（写出去的文件路径由它和 `--model`、`--world-size`、`--worker-id` 一起决定）
- `--seed`：deterministic payload 的种子（用 8 字节 little-endian 模式填充整个 chunk）
- `--model` / `--world-size` / `--worker-id`：构造 CacheEngineKey 时用，需要 put 和 get 用同一组才能命中

退出码：`0` 成功，非 0 失败（NotFound / verify 失败 / 网络错误）。

> 注：默认 chunk 大小 = 64 KiB（fp16，shape `[2,1,256,64]`），可在 `examples/_fake_lmcache.py` 里改 `FakeMetadata` 调。

## 运行测试

```bash
cd integration/lmcache
pip install -e ".[test]"

# 纯 Python 单测（不需要集群）
pytest tests/unit

# 集成测试（需要一个跑着的 dingo-mds + dingo-cache 集群）
pytest tests/integration -m integration
```

## 运维注意

- **一个进程一个 connector 实例**：dingofs gflags 是进程全局的，pybind 层会拒绝第二次 `RemoteCache` 构造。LMCache 自己一进程一个 engine，这是天然契合。
- **dtype 不参与 server 端 key**：client 端把 dtype 信息丢弃（用占位 `"lmcache"`），server 端 path 为 `tensor/XX/XXXX/model@ws@wid@hash@lmcache`。chunk_hash 已是内容哈希，dtype 不需要再独立区分。
- **没有 list 枚举**：dingofs 缓存层无枚举 RPC。`list()` 返回空列表（用于 LMCache 诊断路径）。
- **Exists LRU 容量**：默认 100 万条 ≈ 100MB 内存。可在代码层调 `DingoFSConnector(exists_cache_capacity=...)`。LRU 容忍假阳性（远端 evict 后 Get 自然失败），不容忍假阴性 → 未命中必查远端。

## 故障排查

| 现象 | 可能原因 |
|---|---|
| 启动报 `mds_addrs is required` | URL 里没解析出 mds 地址，检查格式 |
| 启动报 `only one instance allowed per process` | 同进程已经构造过 `RemoteCache`，gflags 是全局的 |
| `Fail to ListMembers` | MDS 不通；查 mds 进程 / 网络 |
| `Fail to put block to remote cache` | cache group 节点不通 / 集群成员不健康，看 dingo-cache 日志 |
| `chunk_hash too short (need > 4 hex chars)` | 上游 key 序列化异常，提 issue |
| Build 报找不到 pybind11 | `pip install pybind11` 或 `apt install pybind11-dev`，并把 cmake prefix 暴露给 dingofs build |

## 性能基线（待补充）

在 N 节点 dingo-cache 集群上，256 KiB chunk × 64 并发：

| 路径 | 吞吐 | p99 延迟 |
|---|---|---|
| Put | TBD | TBD |
| Get | TBD | TBD |
| Batched get (32-key) | TBD | TBD |

底线参照：tmpfs 上的 `fs_l2_adapter`。

## 限制 / 未来工作

- **Get 端到端绝对零拷贝**：当前一次 `IOBuf -> dst` memcpy 是 brpc 物理下限。要消除需要 RDMA 或共享内存协议。
- **Partial chunk 处理**：v1 假设全 chunk 大小。部分读由 LMCache 的 `reshape_partial_chunk` 处理，但 dingofs 当前没暴露 `bytes_read`。
- **GPU 直存**：当前 KV 必须从 GPU 先 offload 到 host。GDR/NVME-of 是后续方向。
