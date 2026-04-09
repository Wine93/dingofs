# SPDX-License-Identifier: Apache-2.0
#
# Tests for DingoFSConnectorAdapter and DingoFSConnectorClient.

# Standard
import asyncio
import os
import sys

# Third Party
import pytest

# Skip all tests in this file if LMCache is not installed
try:
    from lmcache.v1.storage_backend.connector import parse_remote_url

    LMCACHE_AVAILABLE = True
except (ImportError, SyntaxError):
    LMCACHE_AVAILABLE = False

lmcache_only = pytest.mark.skipif(
    not LMCACHE_AVAILABLE, reason="LMCache not installed"
)

# Check if pybind11 _native module is available
try:
    import _native

    NATIVE_AVAILABLE = True
except ImportError:
    NATIVE_AVAILABLE = False

native_only = pytest.mark.skipif(
    not NATIVE_AVAILABLE, reason="_native pybind11 module not available"
)


@lmcache_only
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


@native_only
class TestDingoFSConnectorClient:
    """Tests for the native connector client (ConnectorClientBase)."""

    def test_create_and_close(self, tmp_dir, async_loop):
        """Create a client and verify it can be closed."""
        from dingofs_connector.client import DingoFSConnectorClient

        client = DingoFSConnectorClient(
            config={"cache_dir": tmp_dir},
            loop=async_loop,
        )
        try:
            assert client._fd >= 0
            assert not client._closed
        finally:
            client.close()
            assert client._closed

    def _run(self, coro, loop, timeout=10.0):
        """Run an async coroutine on the async_loop."""
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        return future.result(timeout=timeout)

    def test_set_and_get(self, tmp_dir, async_loop):
        """Set a key then get it back, verify data matches."""
        from dingofs_connector.client import DingoFSConnectorClient

        client = DingoFSConnectorClient(
            config={"cache_dir": tmp_dir},
            loop=async_loop,
        )
        try:
            key = "test_model@1@0@abcd1234@bfloat16"
            data = b"\x42" * 1024  # 1KB of data
            buf = memoryview(bytearray(data))

            self._run(client.set(key, buf), async_loop)

            result = self._run(client.exists(key), async_loop)
            assert result is True

            recv_buf = memoryview(bytearray(1024))
            self._run(client.get(key, recv_buf), async_loop)
            assert bytes(recv_buf) == data
        finally:
            client.close()

    def test_exists_nonexistent(self, tmp_dir, async_loop):
        """Non-existent key returns False."""
        from dingofs_connector.client import DingoFSConnectorClient

        client = DingoFSConnectorClient(
            config={"cache_dir": tmp_dir},
            loop=async_loop,
        )
        try:
            result = self._run(
                client.exists("nonexistent@1@0@deadbeef@float16"),
                async_loop,
            )
            assert result is False
        finally:
            client.close()

    def test_batch_set_and_get(self, tmp_dir, async_loop):
        """Batch set 3 keys then batch get, verify data matches."""
        from dingofs_connector.client import DingoFSConnectorClient

        client = DingoFSConnectorClient(
            config={"cache_dir": tmp_dir},
            loop=async_loop,
        )
        try:
            num_keys = 3
            keys = [
                f"test_model@1@0@{i:08x}@bfloat16"
                for i in range(num_keys)
            ]
            data = [bytes([i + 1] * 2048) for i in range(num_keys)]
            send_bufs = [memoryview(bytearray(d)) for d in data]

            self._run(client.batch_set(keys, send_bufs), async_loop)

            recv_bufs = [memoryview(bytearray(2048)) for _ in range(num_keys)]
            self._run(client.batch_get(keys, recv_bufs), async_loop)

            for i in range(num_keys):
                assert bytes(recv_bufs[i]) == data[i]
        finally:
            client.close()

    def test_batch_exists(self, tmp_dir, async_loop):
        """Batch exists checks."""
        from dingofs_connector.client import DingoFSConnectorClient

        client = DingoFSConnectorClient(
            config={"cache_dir": tmp_dir},
            loop=async_loop,
        )
        try:
            key = "test_model@1@0@11111111@bfloat16"
            buf = memoryview(bytearray(512))
            self._run(client.set(key, buf), async_loop)

            results = self._run(
                client.batch_exists([
                    key,
                    "test_model@1@0@99999999@bfloat16",
                ]),
                async_loop,
            )
            assert results[0] is True
            assert results[1] is False
        finally:
            client.close()
