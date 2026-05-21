# SPDX-License-Identifier: Apache-2.0
"""LMCache RemoteConnector backed by a dingofs cache cluster.

Hot-path design:
  - Submit goes through one pybind call (GIL released for the duration of the
    underlying brpc submission), bouncing onto dingofs bthreads.
  - Native completions arrive on a single eventfd; a demux thread drains them
    and resolves the awaiting concurrent.futures.Future. asyncio.wrap_future
    handles the cross-thread bridge back to the caller's event loop.
  - ExistsLRU short-circuits exists / batched_async_contains so the common
    case (just-put key, immediate prefetch check) skips the network entirely.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import select
import threading
from typing import List, Optional

from lmcache.logging import init_logger
from lmcache.utils import CacheEngineKey
from lmcache.v1.memory_management import MemoryObj
from lmcache.v1.storage_backend.connector.base_connector import RemoteConnector
from lmcache.v1.storage_backend.local_cpu_backend import LocalCPUBackend

from .config import parse_dingofs_url
from .exists_cache import ExistsLRU
from .key_mapper import cache_engine_key_to_native_str

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
        super().__init__(local_cpu_backend.config, local_cpu_backend.metadata)

        endpoint = parse_dingofs_url(url)
        self._loop = loop
        self._local_cpu_backend = local_cpu_backend
        self._exists_lru = ExistsLRU(capacity=exists_cache_capacity)

        # Imported lazily so unit tests can exercise the pure-Python pieces
        # without the compiled extension.
        from . import _dingofs_native  # type: ignore[attr-defined]

        self._native = _dingofs_native.RemoteCache(
            mds_addrs=endpoint.mds_addrs,
            cache_group=endpoint.cache_group,
            extra=endpoint.extra_flags,
        )

        self._pending_lock = threading.Lock()
        self._pending: "dict[int, concurrent.futures.Future]" = {}

        self._stop = threading.Event()
        self._demux = threading.Thread(
            target=self._demux_loop,
            name="dingofs-connector-demux",
            daemon=True,
        )
        self._demux.start()

        logger.info(
            "DingoFSConnector ready: mds=%s cache_group=%s",
            endpoint.mds_addrs,
            endpoint.cache_group,
        )

    # ------------------------------------------------------------------
    # demux thread
    # ------------------------------------------------------------------

    def _demux_loop(self) -> None:
        efd = int(self._native.event_fd())
        poller = select.poll()
        poller.register(efd, select.POLLIN)

        while not self._stop.is_set():
            if not poller.poll(500):
                continue
            try:
                completions = self._native.drain_completions()
            except Exception:  # pragma: no cover
                logger.exception("drain_completions failed")
                continue

            for future_id, ok, error, per_key in completions:
                with self._pending_lock:
                    fut = self._pending.pop(int(future_id), None)
                if fut is None:
                    logger.warning("orphan completion future_id=%d", future_id)
                    continue
                if not fut.set_running_or_notify_cancel():
                    continue
                fut.set_result((bool(ok), str(error), per_key))

    def _submit(self, native_call, *args) -> concurrent.futures.Future:
        """Submit a native batched op and register its future under the lock.

        The lock window covers only the table insertion — the native call
        itself is fast (releases GIL, bounces onto a bthread). Inserting
        BEFORE submission avoids a race where the completion lands before
        the pending entry exists.
        """
        fut: concurrent.futures.Future = concurrent.futures.Future()
        with self._pending_lock:
            future_id = int(native_call(*args))
            self._pending[future_id] = fut
        return fut

    async def _await(self, fut: concurrent.futures.Future):
        return await asyncio.wrap_future(fut, loop=self._loop)

    # ------------------------------------------------------------------
    # exists / batched_async_contains
    # ------------------------------------------------------------------

    async def exists(self, key: CacheEngineKey) -> bool:
        key_str = cache_engine_key_to_native_str(key)
        if self._exists_lru.has(key_str):
            return True
        fut = self._submit(self._native.submit_batch_exists, [key_str])
        _, err, per_key = await self._await(fut)
        if err:
            raise RuntimeError(err)
        found = bool(per_key) and bool(per_key[0])
        if found:
            self._exists_lru.add(key_str)
        return found

    def exists_sync(self, key: CacheEngineKey) -> bool:
        key_str = cache_engine_key_to_native_str(key)
        if self._exists_lru.has(key_str):
            return True
        try:
            found = bool(self._native.exists_sync(key_str))
        except Exception as e:
            logger.warning("exists_sync failed for %s: %s", key_str, e)
            return False
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
        if not keys:
            return 0
        key_strs = [cache_engine_key_to_native_str(k) for k in keys]

        # Find the first key not in the LRU; everything before it is a hit.
        for i, ks in enumerate(key_strs):
            if not self._exists_lru.has(ks):
                remaining = key_strs[i:]
                fut = self._submit(self._native.submit_batch_exists, remaining)
                _, err, per_key = await self._await(fut)
                if err:
                    raise RuntimeError(err)
                if not per_key:
                    return i
                for j, found in enumerate(per_key):
                    if not found:
                        return i + j
                    self._exists_lru.add(remaining[j])
                return len(keys)
        return len(keys)

    # ------------------------------------------------------------------
    # get / put
    # ------------------------------------------------------------------

    async def get(self, key: CacheEngineKey) -> Optional[MemoryObj]:
        memory_obj = self._allocate_chunk()
        if memory_obj is None:
            return None

        key_str = cache_engine_key_to_native_str(key)
        try:
            fut = self._submit(
                self._native.submit_batch_get,
                [key_str],
                [memory_obj.byte_array],
            )
            ok, err, _ = await self._await(fut)
        except Exception:
            memory_obj.ref_count_down()
            raise

        if err:
            memory_obj.ref_count_down()
            raise RuntimeError(err)
        if not ok:
            memory_obj.ref_count_down()
            return None

        self._exists_lru.add(key_str)
        return memory_obj

    async def put(self, key: CacheEngineKey, memory_obj: MemoryObj) -> None:
        key_str = cache_engine_key_to_native_str(key)
        try:
            fut = self._submit(
                self._native.submit_batch_set,
                [key_str],
                [memory_obj.byte_array],
            )
            ok, err, _ = await self._await(fut)
            if err:
                raise RuntimeError(err)
            if ok:
                self._exists_lru.add(key_str)
        finally:
            memory_obj.ref_count_down()

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
        if not keys:
            return
        if len(keys) != len(memory_objs):
            raise ValueError("keys and memory_objs length mismatch")
        key_strs = [cache_engine_key_to_native_str(k) for k in keys]
        mvs = [obj.byte_array for obj in memory_objs]
        try:
            fut = self._submit(self._native.submit_batch_set, key_strs, mvs)
            ok, err, _ = await self._await(fut)
            if err:
                raise RuntimeError(err)
            if ok:
                self._exists_lru.add_many(key_strs)
        finally:
            for obj in memory_objs:
                obj.ref_count_down()

    def support_batched_get(self) -> bool:
        return True

    async def batched_get(
        self,
        keys: List[CacheEngineKey],
    ) -> List[Optional[MemoryObj]]:
        if not keys:
            return []
        key_strs = [cache_engine_key_to_native_str(k) for k in keys]
        objs: List[Optional[MemoryObj]] = [self._allocate_chunk() for _ in keys]
        # If any allocation failed, drop everything and abort cleanly.
        if any(o is None for o in objs):
            for o in objs:
                if o is not None:
                    o.ref_count_down()
            return [None] * len(keys)

        # All non-None at this point — narrowed via assertion for type checker.
        mvs = [o.byte_array for o in objs if o is not None]

        try:
            fut = self._submit(self._native.submit_batch_get, key_strs, mvs)
            _, err, per_key = await self._await(fut)
        except Exception:
            for o in objs:
                if o is not None:
                    o.ref_count_down()
            raise

        if err:
            for o in objs:
                if o is not None:
                    o.ref_count_down()
            raise RuntimeError(err)

        out: List[Optional[MemoryObj]] = []
        for i, found in enumerate(per_key or []):
            obj = objs[i]
            if found and obj is not None:
                self._exists_lru.add(key_strs[i])
                out.append(obj)
            else:
                if obj is not None:
                    obj.ref_count_down()
                out.append(None)
        return out

    # ------------------------------------------------------------------
    # ping / list / close
    # ------------------------------------------------------------------

    def support_ping(self) -> bool:
        return True

    async def ping(self) -> int:
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, self._native.ping)
            return 0
        except Exception as e:
            logger.warning("ping failed: %s", e)
            return 1

    async def list(self) -> List[str]:
        # dingofs has no enumeration RPC. LMCache uses list() mostly for
        # diagnostics; returning empty is safe and documented.
        return []

    async def close(self) -> None:
        self._stop.set()
        try:
            self._demux.join(timeout=5.0)
        except RuntimeError:
            pass
        try:
            self._native.close()
        except Exception as e:  # pragma: no cover
            logger.warning("native.close failed: %s", e)

        with self._pending_lock:
            for fut in self._pending.values():
                if not fut.done():
                    fut.cancel()
            self._pending.clear()

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _allocate_chunk(self) -> Optional[MemoryObj]:
        # We allocate a full-chunk-sized buffer; partial reads write a prefix
        # and the rest stays uninitialized (LMCache reshapes by bytes_read at
        # the storage_manager layer).
        return self._local_cpu_backend.allocate(
            self.meta_shapes[0],
            self.meta_dtypes[0],
            self.meta_fmt,
        )
