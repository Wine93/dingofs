# SPDX-License-Identifier: Apache-2.0

__all__ = ["DingoFSConnectorClient"]


def __getattr__(name: str):
    if name == "DingoFSConnectorClient":
        from dingofs_connector.client import DingoFSConnectorClient

        return DingoFSConnectorClient
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
