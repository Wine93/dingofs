# SPDX-License-Identifier: Apache-2.0
#
# Integration tests for DingoFSConnector and DingoFSConnectorAdapter.
# These tests require LMCache to be installed.

# Standard
import asyncio
import os
import sys

# Third Party
import pytest

# Skip all tests in this file if LMCache is not installed
try:
    import torch
    from lmcache.v1.config import LMCacheEngineConfig
    from lmcache.v1.storage_backend.connector import parse_remote_url

    LMCACHE_AVAILABLE = True
except ImportError:
    LMCACHE_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not LMCACHE_AVAILABLE, reason="LMCache not installed"
)

# Import shared helper from conftest
from conftest import create_test_key


class TestDingoFSConnectorAdapter:
    """Tests for the connector adapter (URL parsing, creation)."""

    def test_can_parse(self):
        """Test URL scheme matching."""
        from dingofs_connector.adapter import DingoFSConnectorAdapter

        adapter = DingoFSConnectorAdapter()
        assert adapter.can_parse("dingofs://host:0/mnt/dingofs/cache")
        assert adapter.can_parse("dingofs://localhost:9999/data")
        assert not adapter.can_parse("redis://localhost:6379")
        assert not adapter.can_parse("fs:///tmp/cache")
        assert not adapter.can_parse("s3://bucket/prefix")

    def test_url_parsing(self):
        """Test that URL is correctly parsed."""
        url = "dingofs://myhost:1234/mnt/dingofs/kv_cache"
        parsed = parse_remote_url(url)
        assert parsed.path == "/mnt/dingofs/kv_cache"

    def test_url_without_path_raises(self):
        """Test that URL without path raises ValueError."""
        from dingofs_connector.adapter import DingoFSConnectorAdapter
        from unittest.mock import MagicMock

        adapter = DingoFSConnectorAdapter()

        context = MagicMock()
        context.url = "dingofs://host:0"
        context.config = MagicMock()
        context.config.extra_config = {}

        with pytest.raises(ValueError):
            adapter.create_connector(context)


class TestDingoFSConnectorIntegration:
    """Integration tests for DingoFSConnector with LMCache interfaces.

    These tests create a real DingoFSConnector and test put/get/exists
    operations using LMCache's config and metadata infrastructure.
    """

    @pytest.fixture
    def config(self, tmp_dir):
        """Create a minimal LMCache config for testing."""
        return LMCacheEngineConfig.from_defaults(
            chunk_size=256,
            remote_url=f"dingofs://host:0{tmp_dir}",
            remote_serde="naive",
            lmcache_instance_id="test_dingofs",
        )

    def _create_connector(self, tmp_dir, async_loop, local_cpu_backend):
        """Helper to create a DingoFSConnector instance."""
        from dingofs_connector.connector import DingoFSConnector

        config = {"cache_dir": tmp_dir}
        return DingoFSConnector(
            config=config,
            loop=async_loop,
            local_cpu_backend=local_cpu_backend,
        )

    def _create_memory_obj(self, connector, local_cpu_backend, key_id=0):
        """Create a MemoryObj with deterministic random data."""
        torch.manual_seed(42 + key_id)
        memory_obj = local_cpu_backend.allocate(
            connector.meta_shapes, connector.meta_dtypes, connector.meta_fmt
        )
        memory_obj.ref_count_up()
        test_tensor = torch.randint(
            0, 100, memory_obj.raw_data.shape, dtype=torch.int64
        )
        memory_obj.raw_data.copy_(
            test_tensor.to(torch.float32).to(memory_obj.metadata.dtype)
        )
        return memory_obj

    def _run(self, coro, loop, timeout=10.0):
        """Run an async coroutine on the async_loop and wait for result."""
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        return future.result(timeout=timeout)

    def _close_connector(self, connector, loop):
        """Close a connector on the async_loop."""
        self._run(connector.close(), loop)

    def test_init(self, tmp_dir, async_loop, local_cpu_backend):
        """Create connector and verify basic attributes."""
        connector = self._create_connector(tmp_dir, async_loop, local_cpu_backend)
        try:
            assert connector.config["cache_dir"] == tmp_dir
            assert connector.loop is async_loop
            assert connector.local_cpu_backend is local_cpu_backend
        finally:
            self._close_connector(connector, async_loop)

    def test_exists_key_not_exists(self, tmp_dir, async_loop, local_cpu_backend):
        """Non-existent key returns False."""
        connector = self._create_connector(tmp_dir, async_loop, local_cpu_backend)
        try:
            key = create_test_key(99)
            result = self._run(connector.exists(key), async_loop)
            assert result is False
        finally:
            self._close_connector(connector, async_loop)

    def test_put_and_get_roundtrip(
        self, tmp_dir, async_loop, local_cpu_backend
    ):
        """Single key: put -> exists -> get -> compare byte_array."""
        connector = self._create_connector(tmp_dir, async_loop, local_cpu_backend)
        try:
            key = create_test_key(0)
            original = self._create_memory_obj(connector, local_cpu_backend, 0)

            self._run(connector.put(key, original), async_loop)

            assert self._run(connector.exists(key), async_loop) is True

            result = self._run(connector.get(key), async_loop)
            assert result is not None
            assert bytes(result.byte_array) == bytes(original.byte_array)
        finally:
            self._close_connector(connector, async_loop)

    def test_get_nonexistent_key(
        self, tmp_dir, async_loop, local_cpu_backend
    ):
        """GET on non-existent key should return None."""
        connector = self._create_connector(tmp_dir, async_loop, local_cpu_backend)
        try:
            key = create_test_key(999)
            result = self._run(connector.get(key), async_loop)
            assert result is None
        finally:
            self._close_connector(connector, async_loop)

    def test_batched_put_and_get(
        self, tmp_dir, async_loop, local_cpu_backend
    ):
        """Multiple keys (5): batch put -> batch exists -> batch get -> compare."""
        connector = self._create_connector(tmp_dir, async_loop, local_cpu_backend)
        try:
            num_keys = 5
            keys = [create_test_key(i) for i in range(num_keys)]
            originals = [
                self._create_memory_obj(connector, local_cpu_backend, i)
                for i in range(num_keys)
            ]

            self._run(connector.batched_put(keys, originals), async_loop)

            # Verify all exist
            count = connector.batched_contains(keys)
            assert count == num_keys

            # Batch get and compare
            results = self._run(connector.batched_get(keys), async_loop)
            assert len(results) == num_keys
            for i in range(num_keys):
                assert bytes(results[i].byte_array) == bytes(
                    originals[i].byte_array
                )
        finally:
            self._close_connector(connector, async_loop)

    def test_batched_contains_prefix(
        self, tmp_dir, async_loop, local_cpu_backend
    ):
        """Write keys 0-2, check batched_contains([0,1,2,3,4]) returns 3."""
        connector = self._create_connector(tmp_dir, async_loop, local_cpu_backend)
        try:
            # Put keys 0, 1, 2
            for i in range(3):
                key = create_test_key(i)
                obj = self._create_memory_obj(connector, local_cpu_backend, i)
                self._run(connector.put(key, obj), async_loop)

            # Query keys 0-4; only 0-2 exist, so consecutive prefix = 3
            all_keys = [create_test_key(i) for i in range(5)]
            count = connector.batched_contains(all_keys)
            assert count == 3
        finally:
            self._close_connector(connector, async_loop)

    def test_batched_async_contains(
        self, tmp_dir, async_loop, local_cpu_backend
    ):
        """Async version of prefix matching."""
        connector = self._create_connector(tmp_dir, async_loop, local_cpu_backend)
        try:
            for i in range(3):
                key = create_test_key(i)
                obj = self._create_memory_obj(connector, local_cpu_backend, i)
                self._run(connector.put(key, obj), async_loop)

            all_keys = [create_test_key(i) for i in range(5)]
            count = self._run(
                connector.batched_async_contains("test_lookup", all_keys),
                async_loop,
            )
            assert count == 3
        finally:
            self._close_connector(connector, async_loop)

    def test_batched_get_non_blocking(
        self, tmp_dir, async_loop, local_cpu_backend
    ):
        """Test non-blocking batched get (prefetch) returns correct data."""
        connector = self._create_connector(tmp_dir, async_loop, local_cpu_backend)
        try:
            num_keys = 3
            keys = [create_test_key(i) for i in range(num_keys)]
            originals = [
                self._create_memory_obj(connector, local_cpu_backend, i)
                for i in range(num_keys)
            ]

            self._run(connector.batched_put(keys, originals), async_loop)

            # Non-blocking get
            results = self._run(
                connector.batched_get_non_blocking("prefetch_test", keys),
                async_loop,
            )
            assert len(results) == num_keys
            for i in range(num_keys):
                assert bytes(results[i].byte_array) == bytes(
                    originals[i].byte_array
                )
        finally:
            self._close_connector(connector, async_loop)

    def test_list_returns_empty(
        self, tmp_dir, async_loop, local_cpu_backend
    ):
        """Test that list() returns empty (not implemented for native engine)."""
        connector = self._create_connector(tmp_dir, async_loop, local_cpu_backend)
        try:
            # Put some data
            key = create_test_key(0)
            obj = self._create_memory_obj(connector, local_cpu_backend, 0)
            self._run(connector.put(key, obj), async_loop)

            # list() should return empty list (current implementation)
            result = self._run(connector.list(), async_loop)
            assert result == []
        finally:
            self._close_connector(connector, async_loop)

    def test_file_persistence(
        self, tmp_dir, async_loop, local_cpu_backend
    ):
        """Write -> close -> new connector -> verify data persists."""
        connector = self._create_connector(tmp_dir, async_loop, local_cpu_backend)
        key = create_test_key(0)
        original = self._create_memory_obj(connector, local_cpu_backend, 0)

        self._run(connector.put(key, original), async_loop)
        self._close_connector(connector, async_loop)

        # Create a new connector pointing to the same directory
        connector2 = self._create_connector(
            tmp_dir, async_loop, local_cpu_backend
        )
        try:
            assert self._run(connector2.exists(key), async_loop) is True
            result = self._run(connector2.get(key), async_loop)
            assert result is not None
            assert bytes(result.byte_array) == bytes(original.byte_array)
        finally:
            self._close_connector(connector2, async_loop)

    def test_sequential_operations(
        self, tmp_dir, async_loop, local_cpu_backend
    ):
        """5 sequential put/get cycles."""
        connector = self._create_connector(tmp_dir, async_loop, local_cpu_backend)
        try:
            for i in range(5):
                key = create_test_key(i)
                original = self._create_memory_obj(
                    connector, local_cpu_backend, i
                )
                self._run(connector.put(key, original), async_loop)
                result = self._run(connector.get(key), async_loop)
                assert result is not None
                assert bytes(result.byte_array) == bytes(
                    original.byte_array
                )
        finally:
            self._close_connector(connector, async_loop)

    def test_concurrent_operations(
        self, tmp_dir, async_loop, local_cpu_backend
    ):
        """Concurrent put 5 keys -> concurrent get -> compare."""
        connector = self._create_connector(tmp_dir, async_loop, local_cpu_backend)
        try:
            num_keys = 5
            keys = [create_test_key(i) for i in range(num_keys)]
            originals = [
                self._create_memory_obj(connector, local_cpu_backend, i)
                for i in range(num_keys)
            ]

            # Concurrent puts
            async def concurrent_puts():
                await asyncio.gather(
                    *[
                        connector.put(keys[i], originals[i])
                        for i in range(num_keys)
                    ]
                )

            self._run(concurrent_puts(), async_loop)

            # Concurrent gets
            async def concurrent_gets():
                return await asyncio.gather(
                    *[connector.get(keys[i]) for i in range(num_keys)]
                )

            results = self._run(concurrent_gets(), async_loop)

            for i in range(num_keys):
                assert results[i] is not None
                assert bytes(results[i].byte_array) == bytes(
                    originals[i].byte_array
                )
        finally:
            self._close_connector(connector, async_loop)
