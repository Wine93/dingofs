# SPDX-License-Identifier: Apache-2.0
"""URL parsing and L2 adapter config for the dingofs LMCache connector."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional

__all__ = [
    "DingoFSEndpoint",
    "parse_dingofs_url",
    "DingoFSL2AdapterConfig",
]


@dataclass(frozen=True)
class DingoFSEndpoint:
    mds_addrs: str         # comma-separated host:port list
    cache_group: str
    extra_flags: Dict[str, str] = field(default_factory=dict)


_SCHEME = "dingofs://"


def parse_dingofs_url(url: str) -> DingoFSEndpoint:
    """Parse a dingofs URL.

    Grammar:
        dingofs://<mds1>:<port1>[,<mds2>:<port2>,...]/<cache_group>[?k=v&...]

    The naive urllib parser treats commas in the host as part of the path, so
    we slice the URL by hand instead. Query parameters (when present) become
    extra dingofs gflag overrides.
    """
    if not url.startswith(_SCHEME):
        raise ValueError(f"not a dingofs:// URL: {url}")

    body = url[len(_SCHEME):]
    query: Optional[str] = None
    if "?" in body:
        body, query = body.split("?", 1)

    if "/" not in body:
        raise ValueError(f"missing /<cache_group> in {url}")
    mds_addrs, cache_group = body.split("/", 1)
    if not mds_addrs:
        raise ValueError(f"missing mds_addrs in {url}")
    if not cache_group:
        raise ValueError(f"missing cache_group in {url}")

    extra: Dict[str, str] = {}
    if query:
        for kv in query.split("&"):
            if not kv:
                continue
            if "=" not in kv:
                raise ValueError(f"malformed query param '{kv}' in {url}")
            k, v = kv.split("=", 1)
            extra[k] = v

    return DingoFSEndpoint(mds_addrs=mds_addrs, cache_group=cache_group,
                           extra_flags=extra)


# Avoid importing the LMCache L2 base at module top-level so this file stays
# usable from unit tests that don't have lmcache.v1.distributed installed.
try:
    from lmcache.v1.distributed.l2_adapters.config import L2AdapterConfigBase
except ImportError:  # pragma: no cover
    L2AdapterConfigBase = object  # type: ignore[assignment,misc]


class DingoFSL2AdapterConfig(L2AdapterConfigBase):
    """Config for the dingofs L2 adapter (cli: --l2-adapter '{"type":"dingofs",...}')."""

    def __init__(self, mds_addrs: str, cache_group: str,
                 extra: Optional[Dict[str, str]] = None) -> None:
        self.mds_addrs = mds_addrs
        self.cache_group = cache_group
        self.extra = extra or {}

    @classmethod
    def from_dict(cls, d: dict) -> "DingoFSL2AdapterConfig":
        mds_addrs = d.get("mds_addrs")
        if not isinstance(mds_addrs, str) or not mds_addrs:
            raise ValueError("mds_addrs must be a non-empty string")
        cache_group = d.get("cache_group")
        if not isinstance(cache_group, str) or not cache_group:
            raise ValueError("cache_group must be a non-empty string")
        extra = d.get("extra", {})
        if not isinstance(extra, dict):
            raise ValueError("extra must be a dict[str,str]")
        return cls(mds_addrs=mds_addrs, cache_group=cache_group,
                   extra={str(k): str(v) for k, v in extra.items()})

    @classmethod
    def help(cls) -> str:
        return (
            "DingoFS L2 adapter config fields:\n"
            "- mds_addrs (str): comma-separated MDS endpoints, e.g. "
            "\"10.0.0.1:6700,10.0.0.2:6700\" (required)\n"
            "- cache_group (str): cache group name (required)\n"
            "- extra (dict[str,str]): additional dingofs gflag overrides "
            "(optional)"
        )
