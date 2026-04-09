# SPDX-License-Identifier: Apache-2.0

# Standard
import asyncio
import os

# Third Party
import pytest

# Local
from dingofs_connector.native_engine import NativeCacheEngine


def _make_config(tmp_dir):
    return {"cache_dir": tmp_dir}


class TestNativeClientSync:
    """Synchronous tests for NativeCacheEngine (native C++ cache engine)."""

    def test_create_and_close(self, tmp_dir):
        """Test basic lifecycle."""
        client = NativeCacheEngine(config=_make_config(tmp_dir))
        try:
            assert client.event_fd() >= 0
        finally:
            client.close()

    def test_single_set_get(self, tmp_dir):
        """Test single key SET + GET round-trip."""
        client = NativeCacheEngine(config=_make_config(tmp_dir))
        try:
            data = bytearray(b"hello dingofs!" * 100)
            buf = memoryview(data)

            # SET
            client.set_sync("test_key", buf)

            # GET
            read_data = bytearray(len(data))
            read_buf = memoryview(read_data)
            client.get_sync("test_key", read_buf)

            assert data == read_data
        finally:
            client.close()

    def test_exists(self, tmp_dir):
        """Test EXISTS for present and absent keys."""
        client = NativeCacheEngine(config=_make_config(tmp_dir))
        try:
            # Non-existent key
            assert client.exists_sync("no_such_key") is False

            # Write a key
            data = bytearray(b"x" * 512)
            client.set_sync("my_key", memoryview(data))

            # Now should exist
            assert client.exists_sync("my_key") is True
        finally:
            client.close()

    def test_batch_operations(self, tmp_dir):
        """Test batch SET + GET + EXISTS."""
        client = NativeCacheEngine(config=_make_config(tmp_dir))
        try:
            num_keys = 5
            data_size = 2048
            keys = [f"batch_key_{i}" for i in range(num_keys)]

            # Prepare write buffers
            write_bufs = []
            for i in range(num_keys):
                data = bytearray(bytes([i & 0xFF]) * data_size)
                write_bufs.append(data)

            # Batch SET
            client.batch_set_sync(keys, [memoryview(b) for b in write_bufs])

            # Batch EXISTS
            results = client.batch_exists_sync(keys)
            assert len(results) == num_keys
            assert all(results)

            # Batch GET
            read_bufs = [bytearray(data_size) for _ in range(num_keys)]
            client.batch_get_sync(keys, [memoryview(b) for b in read_bufs])

            for i in range(num_keys):
                assert write_bufs[i] == read_bufs[i], f"Data mismatch for key {i}"
        finally:
            client.close()

    def test_large_data(self, tmp_dir):
        """Test with 1MB data (typical KV cache chunk size)."""
        client = NativeCacheEngine(config=_make_config(tmp_dir))
        try:
            data_size = 1024 * 1024  # 1 MB
            data = bytearray(os.urandom(data_size))

            client.set_sync("large_key", memoryview(data))

            read_data = bytearray(data_size)
            client.get_sync("large_key", memoryview(read_data))

            assert data == read_data
        finally:
            client.close()

    def test_error_on_get_nonexistent(self, tmp_dir):
        """Test that GET on non-existent key raises an error."""
        client = NativeCacheEngine(config=_make_config(tmp_dir))
        try:
            data = bytearray(1024)
            with pytest.raises(RuntimeError, match="not found|No such file"):
                client.get_sync("nonexistent", memoryview(data))
        finally:
            client.close()

    def test_multiple_workers(self, tmp_dir):
        """Test with many workers for concurrency."""
        client = NativeCacheEngine(config=_make_config(tmp_dir))
        try:
            num_keys = 20
            data_size = 4096
            keys = [f"worker_key_{i}" for i in range(num_keys)]
            write_bufs = [bytearray(os.urandom(data_size)) for _ in range(num_keys)]

            client.batch_set_sync(keys, [memoryview(b) for b in write_bufs])

            read_bufs = [bytearray(data_size) for _ in range(num_keys)]
            client.batch_get_sync(keys, [memoryview(b) for b in read_bufs])

            for i in range(num_keys):
                assert write_bufs[i] == read_bufs[i]
        finally:
            client.close()

    # ------------------------------------------------------------------
    # New functional tests
    # ------------------------------------------------------------------

    def test_default_config(self, tmp_dir):
        """Test SET + GET correctness with default config."""
        client = NativeCacheEngine(config=_make_config(tmp_dir))
        try:
            data = bytearray(os.urandom(4096))
            client.set_sync("sync_none_key", memoryview(data))

            read_data = bytearray(len(data))
            client.get_sync("sync_none_key", memoryview(read_data))

            assert data == read_data
        finally:
            client.close()

    def test_data_overwrite(self, tmp_dir):
        """Test that writing the same key twice overwrites correctly."""
        client = NativeCacheEngine(config=_make_config(tmp_dir))
        try:
            data_size = 2048

            # First write
            data_v1 = bytearray(b"\x01" * data_size)
            client.set_sync("overwrite_key", memoryview(data_v1))

            # Second write with different data
            data_v2 = bytearray(b"\x02" * data_size)
            client.set_sync("overwrite_key", memoryview(data_v2))

            # Read back — should get v2
            read_data = bytearray(data_size)
            client.get_sync("overwrite_key", memoryview(read_data))

            assert read_data == data_v2
            assert read_data != data_v1
        finally:
            client.close()

    def test_persistence_across_engines(self, tmp_dir):
        """Test that data persists after engine close and reopen."""
        data_size = 4096
        data = bytearray(os.urandom(data_size))

        # Write with first engine
        client1 = NativeCacheEngine(config=_make_config(tmp_dir))
        try:
            client1.set_sync("persist_key", memoryview(data))
        finally:
            client1.close()

        # Read with second engine
        client2 = NativeCacheEngine(config=_make_config(tmp_dir))
        try:
            assert client2.exists_sync("persist_key") is True

            read_data = bytearray(data_size)
            client2.get_sync("persist_key", memoryview(read_data))
            assert data == read_data
        finally:
            client2.close()

    def test_special_key_characters(self, tmp_dir):
        """Test keys with special characters (/, ., -)."""
        client = NativeCacheEngine(config=_make_config(tmp_dir))
        try:
            special_keys = [
                "model/layer_0/chunk_42",
                "test.model.key",
                "key-with-dashes",
                "model/layer_1/head_8/chunk_99",
            ]

            data_size = 1024
            for key in special_keys:
                data = bytearray(os.urandom(data_size))
                client.set_sync(key, memoryview(data))

                read_data = bytearray(data_size)
                client.get_sync(key, memoryview(read_data))
                assert data == read_data, f"Data mismatch for key: {key}"

            # Verify all exist
            results = client.batch_exists_sync(special_keys)
            assert all(results)
        finally:
            client.close()

    def test_concurrent_read_write(self, tmp_dir):
        """Test concurrent read/write using batch operations.

        NativeCacheEngine's sync API uses a single eventfd internally, so
        concurrent calls from multiple threads can deadlock. Instead, we
        test concurrency through the C++ worker pool by issuing large
        batch operations that are tiled across workers.
        """
        client = NativeCacheEngine(config=_make_config(tmp_dir))
        try:
            num_keys = 32
            data_size = 4096
            keys = [f"thread_key_{i}" for i in range(num_keys)]
            write_bufs = [bytearray(os.urandom(data_size)) for _ in range(num_keys)]

            # Batch write exercises all 8 workers concurrently via tiling
            client.batch_set_sync(keys, [memoryview(b) for b in write_bufs])

            # Batch read also exercises concurrent worker threads
            read_bufs = [bytearray(data_size) for _ in range(num_keys)]
            client.batch_get_sync(keys, [memoryview(b) for b in read_bufs])

            for i in range(num_keys):
                assert write_bufs[i] == read_bufs[i], f"Mismatch for thread_key_{i}"
        finally:
            client.close()

    def test_batch_exists_mixed(self, tmp_dir):
        """Test batch EXISTS with mix of existing and non-existing keys."""
        client = NativeCacheEngine(config=_make_config(tmp_dir))
        try:
            # Write only even-indexed keys
            data = bytearray(b"x" * 512)
            for i in range(0, 10, 2):
                client.set_sync(f"mixed_key_{i}", memoryview(data))

            # Check all keys (even exist, odd don't)
            all_keys = [f"mixed_key_{i}" for i in range(10)]
            results = client.batch_exists_sync(all_keys)

            for i in range(10):
                if i % 2 == 0:
                    assert results[i] is True, f"Key {i} should exist"
                else:
                    assert results[i] is False, f"Key {i} should not exist"
        finally:
            client.close()


class TestNativeClientAsync:
    """Async tests for NativeCacheEngine (native C++ I/O engine)."""

    @pytest.mark.asyncio
    async def test_async_set_get(self, tmp_dir):
        """Test async SET + GET."""
        loop = asyncio.get_running_loop()
        client = NativeCacheEngine(config=_make_config(tmp_dir), loop=loop)
        try:
            data = bytearray(b"async hello!" * 100)
            await client.set("async_key", memoryview(data))

            read_data = bytearray(len(data))
            await client.get("async_key", memoryview(read_data))

            assert data == read_data
        finally:
            client.close()

    @pytest.mark.asyncio
    async def test_async_exists(self, tmp_dir):
        """Test async EXISTS."""
        loop = asyncio.get_running_loop()
        client = NativeCacheEngine(config=_make_config(tmp_dir), loop=loop)
        try:
            assert await client.exists("no_key") is False

            data = bytearray(b"x" * 512)
            await client.set("yes_key", memoryview(data))

            assert await client.exists("yes_key") is True
        finally:
            client.close()

    @pytest.mark.asyncio
    async def test_async_batch(self, tmp_dir):
        """Test async batch operations."""
        loop = asyncio.get_running_loop()
        client = NativeCacheEngine(config=_make_config(tmp_dir), loop=loop)
        try:
            keys = [f"abatch_{i}" for i in range(5)]
            data_size = 1024
            write_bufs = [bytearray(os.urandom(data_size)) for _ in keys]

            await client.batch_set(keys, [memoryview(b) for b in write_bufs])

            results = await client.batch_exists(keys)
            assert all(results)

            read_bufs = [bytearray(data_size) for _ in keys]
            await client.batch_get(keys, [memoryview(b) for b in read_bufs])

            for i, key in enumerate(keys):
                assert write_bufs[i] == read_bufs[i], f"Mismatch for {key}"
        finally:
            client.close()

    @pytest.mark.asyncio
    async def test_async_overwrite(self, tmp_dir):
        """Test async key overwrite."""
        loop = asyncio.get_running_loop()
        client = NativeCacheEngine(config=_make_config(tmp_dir), loop=loop)
        try:
            data_size = 2048

            # First write
            data_v1 = bytearray(b"\xAA" * data_size)
            await client.set("async_ow_key", memoryview(data_v1))

            # Overwrite
            data_v2 = bytearray(b"\xBB" * data_size)
            await client.set("async_ow_key", memoryview(data_v2))

            # Read back
            read_data = bytearray(data_size)
            await client.get("async_ow_key", memoryview(read_data))

            assert read_data == data_v2
        finally:
            client.close()

    @pytest.mark.asyncio
    async def test_async_concurrent_mixed_ops(self, tmp_dir):
        """Test concurrent async set/get/exists operations."""
        loop = asyncio.get_running_loop()
        client = NativeCacheEngine(config=_make_config(tmp_dir), loop=loop)
        try:
            num_keys = 10
            data_size = 2048
            keys = [f"concurrent_{i}" for i in range(num_keys)]
            write_bufs = [bytearray(os.urandom(data_size)) for _ in range(num_keys)]

            # Submit all sets concurrently
            set_tasks = [
                client.set(keys[i], memoryview(write_bufs[i]))
                for i in range(num_keys)
            ]
            await asyncio.gather(*set_tasks)

            # Submit exists + get concurrently
            exists_task = client.batch_exists(keys)
            get_bufs = [bytearray(data_size) for _ in range(num_keys)]
            get_tasks = [
                client.get(keys[i], memoryview(get_bufs[i]))
                for i in range(num_keys)
            ]

            results = await asyncio.gather(exists_task, *get_tasks)
            exists_results = results[0]

            assert all(exists_results)
            for i in range(num_keys):
                assert get_bufs[i] == write_bufs[i], f"Mismatch for {keys[i]}"
        finally:
            client.close()
