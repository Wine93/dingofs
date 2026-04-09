# SPDX-License-Identifier: Apache-2.0

from dingofs_connector.native_engine import NativeCacheEngine

# Backward compatibility alias
NativeIOEngine = NativeCacheEngine

__all__ = ["DingoFSConnector", "NativeCacheEngine"]


def __getattr__(name: str):
    if name == "DingoFSConnector":
        from dingofs_connector.connector import DingoFSConnector

        return DingoFSConnector
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
