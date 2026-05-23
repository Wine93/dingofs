# SPDX-License-Identifier: Apache-2.0
"""LMCache RemoteConnector adapter registration for dingofs.

Supports two URL forms:

  1. Plugin mode (LMCache >= 0.4.5, recommended). Wire-up in LMCache config:

         remote_storage_plugins: ["dingofs"]
         extra_config:
           remote_storage_plugin.dingofs.module_path: dingofs_connector.adapter
           remote_storage_plugin.dingofs.class_name:  DingoFSConnectorAdapter
           remote_storage_plugin.dingofs.url:         dingofs://mds1:6700/grp

     LMCache synthesizes the connection URL as ``plugin://dingofs`` and the
     adapter pulls the real ``dingofs://...`` endpoint from
     ``extra_config["remote_storage_plugin.dingofs.url"]``.

  2. Legacy mode. Wire-up:

         remote_storage_plugins: ["dingofs"]
         extra_config:
           remote_storage_plugin.dingofs.module_path: dingofs_connector.adapter
           remote_storage_plugin.dingofs.class_name:  DingoFSConnectorAdapter
         remote_url: "dingofs://mds1:6700/grp"

     Direct ``dingofs://...`` URL goes through; deprecated by LMCache but still
     works. Smoke client (examples/client.py) also uses this form.
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

_PLUGIN_TYPE = "dingofs"
_PLUGIN_URL_PREFIX = "plugin://"
_LEGACY_URL_PREFIX = "dingofs://"


def _extract_plugin_type(plugin_name: str) -> str:
    # Mirror of lmcache.v1.storage_backend.connector.extract_plugin_type
    # (only present in lmcache >= 0.4.5). Inlined for back-compat.
    return plugin_name.split(".", 1)[0]


class DingoFSConnectorAdapter(ConnectorAdapter):
    """URL adapter for dingofs.

    Matches both ``dingofs://...`` (legacy) and ``plugin://dingofs[.instance]``
    (LMCache plugin mode).
    """

    def __init__(self) -> None:
        with access_log("adapter.__init__", lambda: ""):
            super().__init__(_LEGACY_URL_PREFIX)

    def can_parse(self, url: str) -> bool:
        if url.startswith(_LEGACY_URL_PREFIX):
            return True
        if url.startswith(_PLUGIN_URL_PREFIX):
            pname = url[len(_PLUGIN_URL_PREFIX):]
            return _extract_plugin_type(pname) == _PLUGIN_TYPE
        return False

    def create_connector(self, context: ConnectorContext) -> RemoteConnector:
        with access_log("adapter.create_connector",
                        lambda: f"url={context.url}"):
            target_url = self._resolve_target_url(context)
            logger.info(
                "Creating DingoFSConnector for context_url=%s target_url=%s",
                context.url,
                target_url,
            )
            return DingoFSConnector(
                url=target_url,
                loop=context.loop,
                local_cpu_backend=context.local_cpu_backend,
            )

    @staticmethod
    def _resolve_target_url(context: ConnectorContext) -> str:
        """Return the real ``dingofs://...`` URL to hand to DingoFSConnector.

        - Legacy mode: ``context.url`` is already ``dingofs://...``.
        - Plugin mode: read from ``extra_config["remote_storage_plugin.<name>.url"]``
          where ``<name>`` is the full plugin instance name (e.g. ``dingofs`` or
          ``dingofs.primary``).
        """
        url = context.url
        if url.startswith(_LEGACY_URL_PREFIX):
            return url

        # Plugin mode. Prefer context.plugin_name (lmcache >= 0.4.5);
        # fall back to parsing the synthesized plugin URL for older paths.
        plugin_name = getattr(context, "plugin_name", None) or url[
            len(_PLUGIN_URL_PREFIX):
        ]

        extra_config = (
            getattr(context.config, "extra_config", None)
            if context.config is not None
            else None
        )
        if not extra_config:
            raise ValueError(
                "Plugin mode requires extra_config "
                f"['remote_storage_plugin.{plugin_name}.url']; "
                "extra_config is empty"
            )

        key = f"remote_storage_plugin.{plugin_name}.url"
        target_url = extra_config.get(key)
        if not target_url:
            raise ValueError(
                f"Plugin mode requires extra_config[{key!r}] to be a "
                "dingofs:// URL (e.g. 'dingofs://mds1:6700/cache_group')"
            )
        if not target_url.startswith(_LEGACY_URL_PREFIX):
            raise ValueError(
                f"extra_config[{key!r}]={target_url!r} must start with "
                f"{_LEGACY_URL_PREFIX!r}"
            )
        return target_url
