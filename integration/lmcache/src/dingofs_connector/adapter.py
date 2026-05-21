# SPDX-License-Identifier: Apache-2.0
"""LMCache RemoteConnector adapter registration for the dingofs:// scheme.

Wire-up in LMCache config:

    remote_storage_plugins: ["dingofs"]
    extra_config:
      remote_storage_plugin.dingofs.module_path: dingofs_connector.adapter
      remote_storage_plugin.dingofs.class_name:  DingoFSConnectorAdapter
    remote_url: "dingofs://mds1:6700,mds2:6700/cache_group"
"""

from __future__ import annotations

from lmcache.logging import init_logger
from lmcache.v1.storage_backend.connector import (
    ConnectorAdapter,
    ConnectorContext,
)
from lmcache.v1.storage_backend.connector.base_connector import RemoteConnector

from .access_log import access_log
from .remote_connector import DingoFSConnector

logger = init_logger(__name__)


class DingoFSConnectorAdapter(ConnectorAdapter):
    """URL adapter; LMCache picks this when remote_url starts with dingofs://."""

    def __init__(self) -> None:
        with access_log("adapter.__init__", lambda: ""):
            super().__init__("dingofs://")

    def create_connector(self, context: ConnectorContext) -> RemoteConnector:
        with access_log("adapter.create_connector",
                        lambda: f"url={context.url}"):
            logger.info("Creating DingoFSConnector for URL: %s", context.url)
            return DingoFSConnector(
                url=context.url,
                loop=context.loop,
                local_cpu_backend=context.local_cpu_backend,
            )
