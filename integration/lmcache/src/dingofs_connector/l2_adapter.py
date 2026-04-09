# SPDX-License-Identifier: Apache-2.0

"""DingoFS L2 Adapter configuration for LMCache MP mode.

Provides factory function to create a NativeConnectorL2Adapter
backed by the DingoFS CacheEngine.
"""

from .native_engine import NativeCacheEngine


class DingoFSNativeClient:
    """Wrapper that adapts NativeCacheEngine to the interface
    expected by NativeConnectorL2Adapter."""

    def __init__(self, config: dict):
        self._engine = NativeCacheEngine(config=config)

    def event_fd(self) -> int:
        return self._engine.event_fd()

    def submit_batch_get(self, keys, memoryviews):
        bufs = [memoryview(mv) for mv in memoryviews]
        return self._engine.batch_get_submit(keys, bufs)

    def submit_batch_set(self, keys, memoryviews):
        bufs = [memoryview(mv) for mv in memoryviews]
        return self._engine.batch_set_submit(keys, bufs)

    def submit_batch_exists(self, keys):
        return self._engine.batch_exists_submit(keys)

    def drain_completions(self):
        return self._engine._drain_raw()

    def close(self):
        self._engine.close()


def create_dingofs_l2_adapter(config: dict):
    """Factory function for creating a DingoFS L2 Adapter.

    Usage in LMCache config:
        l2_config:
            type: "native_plugin"
            factory: "dingofs_connector.l2_adapter:create_dingofs_l2_adapter"
            config:
                cache_dir: "/data/dingofs/cache"
                cache_size_mb: 102400
    """
    try:
        from lmcache.v1.distributed.l2_adapters.native_connector_l2_adapter import (
            NativeConnectorL2Adapter,
        )
    except ImportError:
        raise ImportError(
            "NativeConnectorL2Adapter requires LMCache >= 0.4.0 with MP mode support"
        )

    client = DingoFSNativeClient(config)
    return NativeConnectorL2Adapter(client)
