# SPDX-License-Identifier: Apache-2.0
#
# Tests for the DingoFS L2 Adapter (DingoFSNativeClient + factory function).

# Standard
from unittest.mock import MagicMock, patch

# Third Party
import pytest


class TestDingoFSNativeClient:
    """Tests for DingoFSNativeClient interface adapter."""

    def _make_mock_engine(self):
        """Create a mock NativeCacheEngine with all expected methods."""
        engine = MagicMock()
        engine.event_fd.return_value = 42
        engine.batch_get_submit.return_value = 1
        engine.batch_set_submit.return_value = 2
        engine.batch_exists_submit.return_value = 3
        engine.drain_raw.return_value = []
        engine.close.return_value = None
        return engine

    @patch("dingofs_connector.l2_adapter.NativeCacheEngine")
    def test_create_client(self, mock_cls):
        """Test that DingoFSNativeClient creates engine with config."""
        from dingofs_connector.l2_adapter import DingoFSNativeClient

        mock_cls.return_value = self._make_mock_engine()
        config = {"cache_dir": "/tmp/test", "fs_id": 1, "ino": 1}

        client = DingoFSNativeClient(config)

        mock_cls.assert_called_once_with(config=config)
        assert client is not None

    @patch("dingofs_connector.l2_adapter.NativeCacheEngine")
    def test_event_fd(self, mock_cls):
        """Test event_fd delegation."""
        from dingofs_connector.l2_adapter import DingoFSNativeClient

        mock_engine = self._make_mock_engine()
        mock_cls.return_value = mock_engine

        client = DingoFSNativeClient({"cache_dir": "/tmp/test"})
        fd = client.event_fd()

        assert fd == 42
        mock_engine.event_fd.assert_called_once()

    @patch("dingofs_connector.l2_adapter.NativeCacheEngine")
    def test_submit_batch_get(self, mock_cls):
        """Test submit_batch_get delegation."""
        from dingofs_connector.l2_adapter import DingoFSNativeClient

        mock_engine = self._make_mock_engine()
        mock_cls.return_value = mock_engine

        client = DingoFSNativeClient({"cache_dir": "/tmp/test"})

        keys = ["key1", "key2"]
        bufs = [bytearray(1024), bytearray(1024)]
        result = client.submit_batch_get(keys, bufs)

        assert result == 1
        mock_engine.batch_get_submit.assert_called_once()

    @patch("dingofs_connector.l2_adapter.NativeCacheEngine")
    def test_submit_batch_set(self, mock_cls):
        """Test submit_batch_set delegation."""
        from dingofs_connector.l2_adapter import DingoFSNativeClient

        mock_engine = self._make_mock_engine()
        mock_cls.return_value = mock_engine

        client = DingoFSNativeClient({"cache_dir": "/tmp/test"})

        keys = ["key1", "key2"]
        bufs = [bytearray(1024), bytearray(1024)]
        result = client.submit_batch_set(keys, bufs)

        assert result == 2
        mock_engine.batch_set_submit.assert_called_once()

    @patch("dingofs_connector.l2_adapter.NativeCacheEngine")
    def test_submit_batch_exists(self, mock_cls):
        """Test submit_batch_exists delegation."""
        from dingofs_connector.l2_adapter import DingoFSNativeClient

        mock_engine = self._make_mock_engine()
        mock_cls.return_value = mock_engine

        client = DingoFSNativeClient({"cache_dir": "/tmp/test"})

        keys = ["key1", "key2"]
        result = client.submit_batch_exists(keys)

        assert result == 3
        mock_engine.batch_exists_submit.assert_called_once()

    @patch("dingofs_connector.l2_adapter.NativeCacheEngine")
    def test_drain_completions(self, mock_cls):
        """Test drain_completions delegation."""
        from dingofs_connector.l2_adapter import DingoFSNativeClient

        mock_engine = self._make_mock_engine()
        mock_engine.drain_raw.return_value = [(1, True, "", None)]
        mock_cls.return_value = mock_engine

        client = DingoFSNativeClient({"cache_dir": "/tmp/test"})
        result = client.drain_completions()

        assert result == [(1, True, "", None)]
        mock_engine.drain_raw.assert_called_once()

    @patch("dingofs_connector.l2_adapter.NativeCacheEngine")
    def test_close(self, mock_cls):
        """Test close delegation."""
        from dingofs_connector.l2_adapter import DingoFSNativeClient

        mock_engine = self._make_mock_engine()
        mock_cls.return_value = mock_engine

        client = DingoFSNativeClient({"cache_dir": "/tmp/test"})
        client.close()

        mock_engine.close.assert_called_once()

    @patch("dingofs_connector.l2_adapter.NativeCacheEngine")
    def test_all_interface_methods_exist(self, mock_cls):
        """Verify DingoFSNativeClient exposes the full interface."""
        from dingofs_connector.l2_adapter import DingoFSNativeClient

        mock_cls.return_value = self._make_mock_engine()
        client = DingoFSNativeClient({"cache_dir": "/tmp/test"})

        required_methods = [
            "event_fd",
            "submit_batch_get",
            "submit_batch_set",
            "submit_batch_exists",
            "drain_completions",
            "close",
        ]
        for method_name in required_methods:
            assert hasattr(client, method_name), (
                f"Missing method: {method_name}"
            )
            assert callable(getattr(client, method_name)), (
                f"Not callable: {method_name}"
            )


class TestCreateDingoFSL2Adapter:
    """Tests for the create_dingofs_l2_adapter factory function."""

    @patch("dingofs_connector.l2_adapter.NativeCacheEngine")
    def test_factory_creates_adapter(self, mock_engine_cls):
        """Test that factory creates an adapter when LMCache is available."""
        mock_engine_cls.return_value = MagicMock()

        # Mock the LMCache import
        mock_adapter_cls = MagicMock()
        mock_adapter_instance = MagicMock()
        mock_adapter_cls.return_value = mock_adapter_instance

        with patch.dict(
            "sys.modules",
            {
                "lmcache": MagicMock(),
                "lmcache.v1": MagicMock(),
                "lmcache.v1.distributed": MagicMock(),
                "lmcache.v1.distributed.l2_adapters": MagicMock(),
                "lmcache.v1.distributed.l2_adapters.native_connector_l2_adapter": MagicMock(
                    NativeConnectorL2Adapter=mock_adapter_cls
                ),
            },
        ):
            from dingofs_connector.l2_adapter import create_dingofs_l2_adapter

            config = {"cache_dir": "/tmp/test", "cache_size_mb": 1024}
            result = create_dingofs_l2_adapter(config)

            mock_engine_cls.assert_called_once_with(config=config)
            mock_adapter_cls.assert_called_once()
            assert result is mock_adapter_instance

    @patch("dingofs_connector.l2_adapter.NativeCacheEngine")
    def test_factory_raises_without_lmcache(self, mock_engine_cls):
        """Test that factory raises ImportError without LMCache."""
        mock_engine_cls.return_value = MagicMock()

        # Ensure the lmcache module is not available
        import sys
        modules_to_remove = [
            k for k in sys.modules
            if k.startswith("lmcache.v1.distributed.l2_adapters.native_connector")
        ]
        saved = {}
        for k in modules_to_remove:
            saved[k] = sys.modules.pop(k)

        try:
            # Re-import to get a fresh version
            import importlib
            import dingofs_connector.l2_adapter as mod
            importlib.reload(mod)

            with pytest.raises(ImportError, match="NativeConnectorL2Adapter"):
                mod.create_dingofs_l2_adapter({"cache_dir": "/tmp/test"})
        except ImportError:
            # If LMCache is actually not installed, this is expected behavior
            pass
        finally:
            sys.modules.update(saved)
