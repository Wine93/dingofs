# SPDX-License-Identifier: Apache-2.0
"""LMCache RemoteConnector backed by a dingofs cache cluster.

Hot-path design:
  - Each batched op submits one native RPC fan-in and awaits its asyncio.Future.
    The native eventfd is registered with the asyncio selector via
    `loop.add_reader`, so completions wake the main asyncio thread directly —
    no demux thread, no cross-thread Future bridge, no GIL hop.
  - ExistsLRU short-circuits exists / batched_async_contains so the common
    case (just-put key, immediate prefetch check) skips the network entirely.

This wrapping pattern mirrors LMCache's own ConnectorClientBase used by the
upstream Redis connector — see native_client.py for the eventfd plumbing.
"""

from __future__ import annotations

import asyncio
from typing import List, Optional

from lmcache.logging import init_logger
from lmcache.utils import CacheEngineKey
from lmcache.v1.memory_management import MemoryObj
from lmcache.v1.storage_backend.connector.base_connector import RemoteConnector
from lmcache.v1.storage_backend.local_cpu_backend import LocalCPUBackend

from .access_log import access_log
from .config import parse_dingofs_url
from .exists_cache import ExistsLRU
from .key_mapper import cache_engine_key_to_native_str
from .native_client import DingoFSNativeClient


def _fmt_bytes(n: int) -> str:
    """Pretty-print byte count for access log args."""
    if n < 1024:
        return f"{n}B"
    if n < 1024 * 1024:
        return f"{n / 1024:.2f}KiB"
    return f"{n / 1024 / 1024:.2f}MiB"


logger = init_logger(__name__)


class DingoFSConnector(RemoteConnector):
    """RemoteConnector talking to a dingofs cache group via the native bridge."""

    def __init__(
        self,
        url: str,
        loop: asyncio.AbstractEventLoop,
        local_cpu_backend: LocalCPUBackend,
        exists_cache_capacity: int = 1_000_000,
    ) -> None:
        with access_log("__init__", lambda: f"url={url}") as r:
            super().__init__(local_cpu_backend.config, local_cpu_backend.metadata)

            endpoint = parse_dingofs_url(url)
            self._local_cpu_backend = local_cpu_backend
            self._exists_lru = ExistsLRU(capacity=exists_cache_capacity)
            self._client = DingoFSNativeClient(
                mds_addrs=endpoint.mds_addrs,
                cache_group=endpoint.cache_group,
                extra=endpoint.extra_flags,
                loop=loop,
            )

            logger.info(
                "DingoFSConnector ready: mds=%s cache_group=%s",
                endpoint.mds_addrs,
                endpoint.cache_group,
            )
            r.result = f"mds={endpoint.mds_addrs} cache_group={endpoint.cache_group}"

    # ------------------------------------------------------------------
    # exists / batched_async_contains
    # ------------------------------------------------------------------

    async def exists(self, key: CacheEngineKey) -> bool:
        key_str = cache_engine_key_to_native_str(key)
        with access_log("exists", lambda: key_str) as r:
            if self._exists_lru.has(key_str):
                r.result = "lru_hit"
                return True
            per_key = await self._client.batch_exists([key_str])
            found = bool(per_key) and bool(per_key[0])
            r.result = "found" if found else "not_found"
            if found:
                self._exists_lru.add(key_str)
            return found

    def exists_sync(self, key: CacheEngineKey) -> bool:
        key_str = cache_engine_key_to_native_str(key)
        with access_log("exists_sync", lambda: key_str) as r:
            if self._exists_lru.has(key_str):
                r.result = "lru_hit"
                return True
            try:
                found = self._client.exists_sync(key_str)
            except Exception as e:
                logger.warning("exists_sync failed for %s: %s", key_str, e)
                r.result = f"error: {e}"
                return False
            r.result = "found" if found else "not_found"
            if found:
                self._exists_lru.add(key_str)
            return found

    def support_batched_async_contains(self) -> bool:
        return True

    async def batched_async_contains(
        self,
        lookup_id: str,
        keys: List[CacheEngineKey],
        pin: bool = False,
    ) -> int:
        _ = (lookup_id, pin)  # unused by dingofs
        n = len(keys)
        with access_log("batched_async_contains",
                        lambda: f"{n} keys") as r:
            if not keys:
                r.result = "empty"
                return 0
            key_strs = [cache_engine_key_to_native_str(k) for k in keys]
            # Find the first key not in the LRU; everything before it is a hit.
            for i, ks in enumerate(key_strs):
                if not self._exists_lru.has(ks):
                    remaining = key_strs[i:]
                    per_key = await self._client.batch_exists(remaining)
                    if not per_key:
                        r.result = f"prefix={i} (lru) +0 (no resp)"
                        return i
                    for j, found in enumerate(per_key):
                        if not found:
                            r.result = f"prefix={i + j} (lru={i}, remote={j})"
                            return i + j
                        self._exists_lru.add(remaining[j])
                    r.result = f"prefix={n} (all hit; lru={i})"
                    return n
            r.result = f"prefix={n} (all lru hit)"
            return n

    # ------------------------------------------------------------------
    # get / put
    # ------------------------------------------------------------------

    async def get(self, key: CacheEngineKey) -> Optional[MemoryObj]:
        key_str = cache_engine_key_to_native_str(key)
        with access_log("get", lambda: key_str) as r:
            memory_obj = self._allocate_chunk()
            if memory_obj is None:
                r.result = "alloc_failed"
                return None

            handed_off = False
            try:
                _ok, per_key = await self._client.batch_get(
                    [key_str], [memory_obj.byte_array]
                )
                if per_key and per_key[0]:
                    self._exists_lru.add(key_str)
                    handed_off = True
                    r.result = f"ok {_fmt_bytes(len(memory_obj.byte_array))}"
                    return memory_obj
                r.result = "not_found"
                return None  # NotFound — caller treats as cache miss
            finally:
                if not handed_off:
                    memory_obj.ref_count_down()

    async def put(self, key: CacheEngineKey, memory_obj: MemoryObj) -> None:
        # NOTE: we do NOT ref_count_down memory_obj here. The caller
        # (RemoteBackend.submit_put_task) hands us a serialized
        # ``compressed_memory_obj`` whose ref count it never up'd — its
        # lifetime is managed by the serializer. Mirrors RedisConnector.put.
        key_str = cache_engine_key_to_native_str(key)
        size = len(memory_obj.byte_array)
        with access_log("put",
                        lambda: f"{key_str}, {_fmt_bytes(size)}") as r:
            ok, _ = await self._client.batch_set(
                [key_str], [memory_obj.byte_array]
            )
            if ok:
                self._exists_lru.add(key_str)
            r.result = "ok" if ok else "partial_fail"

    # ------------------------------------------------------------------
    # batched_put / batched_get
    # ------------------------------------------------------------------

    def support_batched_put(self) -> bool:
        return True

    async def batched_put(
        self,
        keys: List[CacheEngineKey],
        memory_objs: List[MemoryObj],
    ) -> None:
        # NOTE: we do NOT ref_count_down memory_objs here. See put() above —
        # these are the serializer's compressed_memory_objs whose ref counts
        # the RemoteBackend caller never up'd. Mirrors RedisConnector.batched_put.
        n = len(keys)
        size = (len(memory_objs[0].byte_array) * n) if memory_objs else 0
        with access_log("batched_put",
                        lambda: f"{n} keys, {_fmt_bytes(size)}") as r:
            if not keys:
                r.result = "empty"
                return
            if len(keys) != len(memory_objs):
                r.result = "FAIL length_mismatch"
                raise ValueError("keys and memory_objs length mismatch")
            key_strs = [cache_engine_key_to_native_str(k) for k in keys]
            views = [obj.byte_array for obj in memory_objs]
            ok, _ = await self._client.batch_set(key_strs, views)
            if ok:
                self._exists_lru.add_many(key_strs)
            r.result = "ok" if ok else "partial_fail"

    def support_batched_get(self) -> bool:
        return True

    async def batched_get(
        self,
        keys: List[CacheEngineKey],
    ) -> List[Optional[MemoryObj]]:
        n = len(keys)
        with access_log("batched_get", lambda: f"{n} keys") as r:
            if not keys:
                r.result = "empty"
                return []
            key_strs = [cache_engine_key_to_native_str(k) for k in keys]

            # Single-pass allocate; on any failure release what we've taken
            # so far and abort the whole batch.
            objs: List[MemoryObj] = []
            for _ in keys:
                obj = self._allocate_chunk()
                if obj is None:
                    for o in objs:
                        o.ref_count_down()
                    r.result = "alloc_failed"
                    return [None] * n
                objs.append(obj)

            views = [o.byte_array for o in objs]
            try:
                _ok, per_key = await self._client.batch_get(key_strs, views)
            except Exception:
                for o in objs:
                    o.ref_count_down()
                raise

            per_key_list = list(per_key or [])
            out: List[Optional[MemoryObj]] = [None] * n
            hit_keys: List[str] = []
            for i, obj in enumerate(objs):
                if i < len(per_key_list) and per_key_list[i]:
                    out[i] = obj
                    hit_keys.append(key_strs[i])
                else:
                    obj.ref_count_down()
            if hit_keys:
                self._exists_lru.add_many(hit_keys)
            r.result = f"hits={len(hit_keys)}/{n}"
            return out

    # ------------------------------------------------------------------
    # ping / list / close
    # ------------------------------------------------------------------

    def support_ping(self) -> bool:
        return True

    async def ping(self) -> int:
        # ping_sync just checks the engine's running flag — connectivity
        # was verified at construction (MDS ListMembers). Cheap; no
        # off-thread offload needed.
        with access_log("ping", lambda: "") as r:
            try:
                self._client.ping_sync()
                r.result = "ok"
                return 0
            except Exception as e:
                logger.warning("ping failed: %s", e)
                r.result = f"FAIL {e}"
                return 1

    async def list(self) -> List[str]:
        with access_log("list", lambda: "") as r:
            # dingofs has no enumeration RPC. LMCache uses list() mostly for
            # diagnostics; returning empty is safe and documented.
            r.result = "unsupported (returning [])"
            return []

    async def close(self) -> None:
        with access_log("close", lambda: ""):
            self._client.close()

    # ------------------------------------------------------------------
    # lifecycle / utility hooks (post_init, reshape_partial_chunk)
    # ------------------------------------------------------------------

    def post_init(self) -> None:
        # One-shot setup hook fired by LMCache right after __init__.
        # Logging this captures the bootstrap timeline.
        with access_log("post_init", lambda: ""):
            super().post_init()

    def reshape_partial_chunk(self, memory_obj, bytes_read):
        # Called by LMCache's storage_manager on a get whose payload was
        # shorter than full_chunk_size_bytes. dingofs currently always
        # returns full chunks (no bytes_read plumbing), so this should
        # not fire in practice — but if it ever does, the log line will
        # tell us.
        full = self.full_chunk_size_bytes
        with access_log("reshape_partial_chunk",
                        lambda: f"{bytes_read}/{full} bytes"):
            return super().reshape_partial_chunk(memory_obj, bytes_read)

    # ------------------------------------------------------------------
    # batched_get_non_blocking / remove_sync / batched_contains
    # ------------------------------------------------------------------
    #
    # These three exist on RemoteConnector but we didn't customise them.
    # Wrap-and-delegate so the access log shows whether (and how) LMCache
    # actually calls them in practice — research target, not perf path.

    def support_batched_get_non_blocking(self) -> bool:
        # Base default is True; we keep it.
        return True

    async def batched_get_non_blocking(
        self,
        lookup_id: str,
        keys: List[CacheEngineKey],
    ) -> List[MemoryObj]:
        # Base impl in base_connector.py does asyncio.gather(self.get for k in
        # keys) and trims to the longest consecutive prefix. Each per-key get
        # still emits its own access_log entry; this outer line just confirms
        # the entry point itself was hit and what prefix LMCache got.
        n = len(keys)
        with access_log("batched_get_non_blocking",
                        lambda: f"{n} keys lookup_id={lookup_id}") as r:
            result = await super().batched_get_non_blocking(lookup_id, keys)
            r.result = f"prefix={len(result)}/{n}"
            return result

    def remove_sync(self, key: CacheEngineKey) -> bool:
        # Base raises NotImplementedError. Log the call so we know whether
        # LMCache ever invokes it (eviction path), then propagate the same
        # behaviour. Returning False would silently lie about success.
        key_str = cache_engine_key_to_native_str(key)
        with access_log("remove_sync", lambda: key_str) as r:
            r.result = "FAIL NotImplementedError"
            raise NotImplementedError("dingofs connector has no remove path yet")

    def support_batched_contains(self) -> bool:
        # Base default is False; we keep it.
        return False

    def batched_contains(self, keys: List[CacheEngineKey]) -> int:
        # support_batched_contains() returns False above — LMCache shouldn't
        # call this. Wrap anyway to catch the case if it ever does.
        n = len(keys)
        with access_log("batched_contains", lambda: f"{n} keys") as r:
            r.result = "FAIL NotImplementedError"
            raise NotImplementedError(
                "dingofs connector does not support sync batched_contains"
            )

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _allocate_chunk(self) -> Optional[MemoryObj]:
        # Full-chunk-sized buffer; partial reads write a prefix and the rest
        # stays uninitialized (LMCache reshapes by bytes_read at the
        # storage_manager layer).
        return self._local_cpu_backend.allocate(
            self.meta_shapes[0],
            self.meta_dtypes[0],
            self.meta_fmt,
        )
