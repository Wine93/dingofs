# SPDX-License-Identifier: Apache-2.0

"""DingoFS connector client for LMCache.

Non-MP mode entry point. Wraps the C++ DingoFSConnector (pybind11)
with ConnectorClientBase for asyncio integration.
"""

# Standard
from typing import Optional
import asyncio

# Local
from .connector_client_base import ConnectorClientBase


class DingoFSConnectorClient(ConnectorClientBase):
    """Non-MP mode client backed by the native C++ DingoFSConnector."""

    def __init__(
        self,
        config: dict,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        **kwargs,
    ):
        from _native import DingoFSConnector

        cache_dir = config["cache_dir"]
        fs_id = int(config.get("fs_id", 1))
        ino = int(config.get("ino", 1))
        cache_size_mb = int(config.get("cache_size_mb", 102400))
        exists_cache_capacity = int(
            config.get("exists_cache_capacity", 100000)
        )
        num_workers = int(config.get("num_workers", 4))

        native_client = DingoFSConnector(
            cache_dir, fs_id, ino, cache_size_mb,
            exists_cache_capacity, num_workers,
        )
        super().__init__(native_client, loop)
