# SPDX-License-Identifier: Apache-2.0
"""Minimal LMCache shims sufficient to drive DingoFSConnector end-to-end.

Real LMCacheEngine wires together a heavy stack (config + metadata +
allocator + workers + ...) just to feed a RemoteConnector. For a smoke
client all DingoFSConnector touches on its backend is:

  - local_cpu_backend.config         (.extra_config, .use_layerwise)
  - local_cpu_backend.metadata       (.get_shapes(), .get_dtypes(), .use_mla,
                                      .chunk_size, .get_num_groups())
  - local_cpu_backend.allocate(shape, dtype, fmt) -> MemoryObj
                                     (with .byte_array, .ref_count_down())

So this module exposes exactly that surface, no more. Keeps the smoke client
self-contained — no LMCacheEngine bring-up needed.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import List, Optional

import torch


@dataclass
class FakeConfig:
    """Subset of LMCacheEngineConfig consulted by RemoteConnector.__init__."""
    extra_config: Optional[dict] = None
    use_layerwise: bool = False


@dataclass
class FakeMetadata:
    """Subset of LMCacheMetadata consulted by RemoteConnector.__init__.

    Defaults model one chunk of fp16 KV: shape [2, 1, 256, 64], 65536 bytes.
    full_chunk_size_bytes (computed inside RemoteConnector) must be divisible
    by chunk_size — the defaults satisfy 65536 % 256 == 0.
    """
    use_mla: bool = False
    chunk_size: int = 256
    _shapes: List[torch.Size] = field(
        default_factory=lambda: [torch.Size([2, 1, 256, 64])]
    )
    _dtypes: List[torch.dtype] = field(
        default_factory=lambda: [torch.float16]
    )

    def get_shapes(self) -> List[torch.Size]:
        return self._shapes

    def get_dtypes(self) -> List[torch.dtype]:
        return self._dtypes

    def get_num_groups(self) -> int:
        return 1


class FakeMemoryObj:
    """Bare-bones MemoryObj: owns a bytearray, exposes a memoryview."""

    def __init__(self, nbytes: int):
        self._buf = bytearray(nbytes)

    @property
    def byte_array(self) -> memoryview:
        return memoryview(self._buf)

    def ref_count_down(self) -> None:
        # No refcounting in this smoke shim — connector calls this once after
        # put/get to drop its reference; we just no-op.
        pass


class FakeLocalCPUBackend:
    """Just enough of LocalCPUBackend for DingoFSConnector to call allocate()."""

    def __init__(
        self,
        config: Optional[FakeConfig] = None,
        metadata: Optional[FakeMetadata] = None,
    ):
        self.config = config or FakeConfig()
        self.metadata = metadata or FakeMetadata()

    def allocate(self, shape, dtype, fmt) -> FakeMemoryObj:
        # fmt is unused: LMCache uses it for compressed layouts, dingofs is
        # opaque about the payload.
        del fmt
        elements = math.prod(shape)
        elem_bytes = torch.empty((), dtype=dtype).element_size()
        return FakeMemoryObj(elements * elem_bytes)

    def get_full_chunk_size_bytes(self) -> int:
        shapes = self.metadata.get_shapes()
        dtypes = self.metadata.get_dtypes()
        total = 0
        for shape, dtype in zip(shapes, dtypes):
            total += math.prod(shape) * torch.empty((), dtype=dtype).element_size()
        return total
