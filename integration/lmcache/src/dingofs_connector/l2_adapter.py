# SPDX-License-Identifier: Apache-2.0
"""DingoFS L2 adapter.

Inherits the upstream NativeConnectorL2Adapter unchanged: that class already
implements the eventfd demux (1 native -> 3 user), the pending-op table with
race-safe registration, and the per-key result bitmaps. The dingofs native
RemoteCache module's submit_batch_* signature matches what the upstream
adapter expects (`list[str]` keys serialized via _object_key_to_string),
so wiring it in is just a constructor call.
"""

from __future__ import annotations

from lmcache.v1.distributed.l2_adapters.native_connector_l2_adapter import (
    NativeConnectorL2Adapter,
)

__all__ = ["DingoFSL2Adapter"]


class DingoFSL2Adapter(NativeConnectorL2Adapter):
    """Marker subclass; behavior is fully inherited from the upstream adapter."""

    pass
