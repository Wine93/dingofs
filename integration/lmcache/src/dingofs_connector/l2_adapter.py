# SPDX-License-Identifier: Apache-2.0

"""DingoFS L2 Adapter for LMCache MP mode.

The pybind11-bound DingoFSConnector exposes event_fd(),
submit_batch_get/set/exists(), drain_completions(), and close()
— exactly the interface NativeConnectorL2Adapter expects.
"""


def create_dingofs_l2_adapter(config: dict):
    """Factory function for DingoFS L2 Adapter (MP mode).

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
            "NativeConnectorL2Adapter requires LMCache >= 0.5.0 "
            "with MP mode support"
        )

    from _native import DingoFSConnector

    cache_dir = config["cache_dir"]
    fs_id = int(config.get("fs_id", 1))
    ino = int(config.get("ino", 1))
    cache_size_mb = int(config.get("cache_size_mb", 102400))
    exists_cache_capacity = int(
        config.get("exists_cache_capacity", 100000)
    )
    num_workers = int(config.get("num_workers", 4))

    connector = DingoFSConnector(
        cache_dir, fs_id, ino, cache_size_mb,
        exists_cache_capacity, num_workers,
    )
    return NativeConnectorL2Adapter(connector)
