# SPDX-License-Identifier: Apache-2.0

# Standard
import asyncio
import shutil
import tempfile
import threading

# Third Party
import pytest

# LMCache availability check (used by integration tests)
try:
    import torch
    from lmcache.utils import CacheEngineKey
    from lmcache.v1.config import LMCacheEngineConfig
    from lmcache.v1.memory_management import PinMemoryAllocator
    from lmcache.v1.metadata import LMCacheMetadata
    from lmcache.v1.storage_backend.local_cpu_backend import LocalCPUBackend

    LMCACHE_AVAILABLE = True
except ImportError:
    LMCACHE_AVAILABLE = False


@pytest.fixture
def tmp_dir():
    """Create a temporary directory for test data."""
    d = tempfile.mkdtemp(prefix="dingofs_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def async_loop():
    """Create an asyncio event loop running in a separate thread.

    Used by integration tests that need to bridge sync test code with async
    connector operations (same pattern as LMCache's test_fs_connector.py).

    Propagates exceptions from the background loop thread so that failures
    in async code surface as test errors instead of being silently swallowed.
    """
    loop = asyncio.new_event_loop()
    exc_holder: list = []

    def _run_loop():
        try:
            loop.run_forever()
        except Exception as e:
            exc_holder.append(e)

    thread = threading.Thread(target=_run_loop, name="test-async-loop", daemon=True)
    thread.start()
    yield loop
    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout=5.0)
    if exc_holder:
        raise RuntimeError(
            f"async_loop thread crashed: {exc_holder[0]}"
        ) from exc_holder[0]


# ------------------------------------------------------------------
# LMCache integration fixtures (skipped when LMCache is not installed)
# ------------------------------------------------------------------


def _create_test_metadata():
    """Create test LMCacheMetadata."""
    if not LMCACHE_AVAILABLE:
        return None
    return LMCacheMetadata(
        model_name="test_model",
        world_size=1,
        local_world_size=1,
        worker_id=0,
        local_worker_id=0,
        kv_dtype=torch.bfloat16,
        kv_shape=(32, 2, 256, 8, 128),
    )


def create_test_key(key_id: int = 0):
    """Create a test CacheEngineKey with a unique chunk_hash."""
    if not LMCACHE_AVAILABLE:
        return None
    return CacheEngineKey(
        model_name="test_model",
        world_size=1,
        worker_id=0,
        chunk_hash=hash(key_id),
        dtype=torch.bfloat16,
    )


@pytest.fixture
def lmcache_metadata():
    """LMCacheMetadata for integration tests."""
    pytest.importorskip("lmcache")
    return _create_test_metadata()


@pytest.fixture
def memory_allocator():
    """PinMemoryAllocator (1 GB) for integration tests."""
    pytest.importorskip("lmcache")
    alloc = PinMemoryAllocator(1024 * 1024 * 1024)
    yield alloc
    alloc.close()


@pytest.fixture
def local_cpu_backend(memory_allocator, lmcache_metadata):
    """LocalCPUBackend for integration tests."""
    config = LMCacheEngineConfig.from_legacy(chunk_size=256)
    backend = LocalCPUBackend(
        config=config,
        metadata=lmcache_metadata,
        memory_allocator=memory_allocator,
    )
    yield backend
    if hasattr(backend, "close"):
        backend.close()
