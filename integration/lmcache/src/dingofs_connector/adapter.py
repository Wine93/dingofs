# SPDX-License-Identifier: Apache-2.0

# First Party
from lmcache.logging import init_logger
from lmcache.v1.storage_backend.connector import ConnectorAdapter, ConnectorContext
from lmcache.v1.storage_backend.connector import parse_remote_url
from lmcache.v1.storage_backend.connector.base_connector import RemoteConnector

logger = init_logger(__name__)


class DingoFSConnectorAdapter(ConnectorAdapter):
    """Adapter for the DingoFS connector.

    Handles URLs of the form: dingofs://host:port/mount/path
    - host:port is ignored (kept for URL compatibility)
    - /mount/path is the DingoFS mount point used as the storage directory

    Extra config options (via extra_config):
        dingofs_cache_size_mb (int): Cache size in MB. Default: 102400.
        dingofs_exists_cache_capacity (int): Exists cache capacity.
            Default: 100000.
        dingofs_fs_id (int): Filesystem ID. Default: 0.
    """

    def __init__(self) -> None:
        super().__init__("dingofs://")

    def create_connector(self, context: ConnectorContext) -> RemoteConnector:
        """Create a DingoFSConnector from the given context.

        Args:
            context: Connector context containing URL, config, etc.

        Returns:
            A DingoFSConnector instance.
        """
        # Local import to avoid circular dependencies and allow lazy loading
        from .client import DingoFSConnectorClient

        logger.info(f"Creating DingoFS connector for URL: {context.url}")

        parsed_url = parse_remote_url(context.url)
        base_path = parsed_url.path

        if not base_path:
            raise ValueError(
                f"DingoFS URL must include a path: {context.url}. "
                "Example: dingofs://host:0/mnt/dingofs/cache"
            )

        # Read extra config
        lmcache_config = context.config
        extra = (
            lmcache_config.extra_config
            if lmcache_config and lmcache_config.extra_config
            else {}
        )
        cache_size_mb = int(extra.get("dingofs_cache_size_mb", 102400))
        exists_cache_capacity = int(
            extra.get("dingofs_exists_cache_capacity", 100000)
        )
        fs_id = int(extra.get("dingofs_fs_id", 1))

        config = {
            "cache_dir": base_path,
            "fs_id": fs_id,
            "cache_size_mb": cache_size_mb,
            "exists_cache_capacity": exists_cache_capacity,
        }

        return DingoFSConnectorClient(
            config=config,
            loop=context.loop,
        )
