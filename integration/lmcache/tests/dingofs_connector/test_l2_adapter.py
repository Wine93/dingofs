# SPDX-License-Identifier: Apache-2.0
#
# Tests for the DingoFS L2 Adapter factory function.

# Standard
from unittest.mock import MagicMock, patch

# Third Party
import pytest


class TestCreateDingoFSL2Adapter:
    """Tests for the create_dingofs_l2_adapter factory function."""

    def test_factory_raises_without_lmcache_mp(self):
        """Verify ImportError when NativeConnectorL2Adapter is missing."""
        with patch.dict(
            "sys.modules",
            {
                "lmcache.v1.distributed.l2_adapters"
                ".native_connector_l2_adapter": None,
            },
        ):
            from dingofs_connector.l2_adapter import (
                create_dingofs_l2_adapter,
            )
            with pytest.raises(ImportError, match="LMCache >= 0.5.0"):
                create_dingofs_l2_adapter({"cache_dir": "/tmp/test"})
