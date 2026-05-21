#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Smoke client that drives DingoFSConnector against a real dingo-cache cluster.

Mirrors the call shape of LMCache (CacheEngineKey + MemoryObj going through
the RemoteConnector interface) without needing a full LMCacheEngine bring-up.

Examples (assuming `dingofs://mds1:6700/lmcache_group` is reachable):

    # quick liveness
    python examples/client.py --url dingofs://mds1:6700/grp ping

    # write a deterministic 64 KiB payload under chunk_hash=0xdeadbeef
    python examples/client.py --url dingofs://mds1:6700/grp put --hash 0xdeadbeef

    # read it back and verify byte-for-byte
    python examples/client.py --url dingofs://mds1:6700/grp get --hash 0xdeadbeef --verify

    # check existence (LRU short-circuits if it was just put in this process)
    python examples/client.py --url dingofs://mds1:6700/grp exists --hash 0xdeadbeef

    # one-shot self-test: put then get then byte-compare
    python examples/client.py --url dingofs://mds1:6700/grp roundtrip --hash 0xcafebabe

After put / roundtrip, you can inspect the cache nodes — the data lives at
something like `tensor/<hash[0:2]>/<hash[0:4]>/<filename>` on the disks of
the cache group members.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import struct
import sys
from pathlib import Path
from typing import Optional

# Allow `python examples/client.py ...` from the package root without install.
_HERE = Path(__file__).resolve().parent
_PKG_ROOT = _HERE.parent / "src"
if _PKG_ROOT.exists() and str(_PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(_PKG_ROOT))
sys.path.insert(0, str(_HERE))  # for _fake_lmcache

import torch
from lmcache.utils import CacheEngineKey

from _fake_lmcache import FakeLocalCPUBackend, FakeMemoryObj
from dingofs_connector.remote_connector import DingoFSConnector

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

_PATTERN_LEN = 8  # bytes


def _make_key(args: argparse.Namespace) -> CacheEngineKey:
    return CacheEngineKey(
        model_name=args.model,
        world_size=args.world_size,
        worker_id=args.worker_id,
        chunk_hash=args.hash,
        dtype=torch.float16,
    )


def _fill_pattern(mv: memoryview, seed: int) -> None:
    """Write a deterministic 8-byte pattern repeatedly across the buffer."""
    pattern = struct.pack("<Q", seed & 0xFFFFFFFFFFFFFFFF)
    full = len(mv) // _PATTERN_LEN
    for i in range(full):
        mv[i * _PATTERN_LEN : (i + 1) * _PATTERN_LEN] = pattern
    tail = len(mv) % _PATTERN_LEN
    if tail:
        mv[-tail:] = pattern[:tail]


def _verify_pattern(mv: memoryview, seed: int) -> bool:
    pattern = struct.pack("<Q", seed & 0xFFFFFFFFFFFFFFFF)
    full = len(mv) // _PATTERN_LEN
    for i in range(full):
        if bytes(mv[i * _PATTERN_LEN : (i + 1) * _PATTERN_LEN]) != pattern:
            return False
    tail = len(mv) % _PATTERN_LEN
    if tail and bytes(mv[-tail:]) != pattern[:tail]:
        return False
    return True


def _allocate(backend: FakeLocalCPUBackend) -> FakeMemoryObj:
    shape = backend.metadata.get_shapes()[0]
    dtype = backend.metadata.get_dtypes()[0]
    return backend.allocate(shape, dtype, fmt=None)


async def _with_connector(url: str, fn):
    backend = FakeLocalCPUBackend()
    loop = asyncio.get_running_loop()
    conn = DingoFSConnector(url=url, loop=loop, local_cpu_backend=backend)
    try:
        return await fn(conn, backend)
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# commands
# ---------------------------------------------------------------------------

async def cmd_ping(args: argparse.Namespace) -> int:
    async def go(conn: DingoFSConnector, _backend):
        ret = await conn.ping()
        print(f"ping: ret={ret}")
        return ret
    return await _with_connector(args.url, go)


async def cmd_put(args: argparse.Namespace) -> int:
    async def go(conn: DingoFSConnector, backend: FakeLocalCPUBackend):
        key = _make_key(args)
        obj = _allocate(backend)
        _fill_pattern(obj.byte_array, args.seed)
        print(
            f"put: key={key.to_string()} "
            f"size={len(obj.byte_array)} seed=0x{args.seed:x}"
        )
        await conn.put(key, obj)
        print("put: ok")
        return 0
    return await _with_connector(args.url, go)


async def cmd_get(args: argparse.Namespace) -> int:
    async def go(conn: DingoFSConnector, _backend):
        key = _make_key(args)
        obj: Optional[FakeMemoryObj] = await conn.get(key)  # type: ignore[assignment]
        if obj is None:
            print(f"get: NOT FOUND key={key.to_string()}")
            return 1
        print(f"get: ok key={key.to_string()} size={len(obj.byte_array)}")
        if args.verify:
            ok = _verify_pattern(obj.byte_array, args.seed)
            print(f"get: verify={'PASS' if ok else 'FAIL'} seed=0x{args.seed:x}")
            return 0 if ok else 1
        return 0
    return await _with_connector(args.url, go)


async def cmd_exists(args: argparse.Namespace) -> int:
    async def go(conn: DingoFSConnector, _backend):
        key = _make_key(args)
        present = await conn.exists(key)
        print(f"exists: key={key.to_string()} -> {present}")
        return 0 if present else 1
    return await _with_connector(args.url, go)


async def cmd_roundtrip(args: argparse.Namespace) -> int:
    """put → get → byte-compare. The single most useful command for verifying
    a fresh cluster end-to-end."""
    async def go(conn: DingoFSConnector, backend: FakeLocalCPUBackend):
        key = _make_key(args)

        # [1/3] put
        src = _allocate(backend)
        _fill_pattern(src.byte_array, args.seed)
        print(
            f"[1/3] put key={key.to_string()} "
            f"size={len(src.byte_array)} seed=0x{args.seed:x}"
        )
        await conn.put(key, src)

        # [2/3] get
        print(f"[2/3] get key={key.to_string()}")
        got = await conn.get(key)
        if got is None:
            print("FAIL: get returned None after put — cluster did not persist")
            return 1

        # [3/3] verify
        print("[3/3] verify byte pattern")
        if not _verify_pattern(got.byte_array, args.seed):
            print("FAIL: byte pattern mismatch")
            return 1
        print("PASS")
        return 0
    return await _with_connector(args.url, go)


# ---------------------------------------------------------------------------
# argparse wiring
# ---------------------------------------------------------------------------

def _hex_int(s: str) -> int:
    return int(s, 0)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--url", required=True,
                   help="dingofs URL, e.g. dingofs://mds1:6700,mds2:6700/cache_group")
    p.add_argument("--model", default="smoke-client",
                   help="model_name field of the CacheEngineKey")
    p.add_argument("--world-size", type=int, default=1)
    p.add_argument("--worker-id", type=int, default=0)
    sub = p.add_subparsers(dest="cmd", required=True)

    for name in ("put", "get", "exists", "roundtrip"):
        sp = sub.add_parser(name)
        sp.add_argument("--hash", required=True, type=_hex_int,
                        help="chunk_hash (decimal or 0x-prefixed hex)")
        sp.add_argument("--seed", type=_hex_int, default=0xCAFEBABE,
                        help="seed for deterministic payload (default 0xcafebabe)")
        if name == "get":
            sp.add_argument("--verify", action="store_true",
                            help="after get, byte-compare against --seed pattern")

    sub.add_parser("ping")
    return p


def main() -> None:
    args = build_parser().parse_args()
    cmd = {
        "put": cmd_put,
        "get": cmd_get,
        "exists": cmd_exists,
        "roundtrip": cmd_roundtrip,
        "ping": cmd_ping,
    }[args.cmd]
    rc = asyncio.run(cmd(args))
    sys.exit(rc)


if __name__ == "__main__":
    main()
