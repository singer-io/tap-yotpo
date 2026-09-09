import unittest
from unittest.mock import MagicMock, patch

from tap_yotpo import exceptions as errors
from tap_yotpo.sync import sync


class DummyTransformer:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class DummySchema:
    def to_dict(self):
        return {"type": "object", "properties": {"id": {"type": "integer"}}}


class DummyCatalogEntry:
    def __init__(self, tap_stream_id):
        self.tap_stream_id = tap_stream_id
        self.schema = DummySchema()
        self.metadata = []
        self.replication_key = "updated_at"


class DummyCatalog:
    def __init__(self, entries):
        self.entries = entries

    def get_selected_streams(self, _state):
        return self.entries


class TestSync(unittest.TestCase):
    @patch("tap_yotpo.sync.singer.metadata.to_map", return_value={})
    @patch("tap_yotpo.sync.singer.write_schema")
    @patch("tap_yotpo.sync.singer.write_state")
    @patch("tap_yotpo.sync.singer.set_currently_syncing", side_effect=lambda state, stream: state)
    @patch("tap_yotpo.sync.singer.Transformer", return_value=DummyTransformer())
    def test_sync_continues_after_client_error(
        self,
        _mock_transformer,
        _mock_set_currently_syncing,
        _mock_write_state,
        _mock_write_schema,
        _mock_to_map,
    ):
        first_stream_obj = MagicMock()
        first_stream_obj.key_properties = ["id"]
        first_stream_obj.sync.side_effect = errors.Http500RequestError()

        second_stream_obj = MagicMock()
        second_stream_obj.key_properties = ["id"]
        second_stream_obj.sync.return_value = {"bookmarks": {}}

        with patch.dict(
            "tap_yotpo.sync.streams.STREAMS",
            {
                "stream_1": lambda _client: first_stream_obj,
                "stream_2": lambda _client: second_stream_obj,
            },
            clear=False,
        ):
            catalog = DummyCatalog([DummyCatalogEntry("stream_1"), DummyCatalogEntry("stream_2")])
            sync(client=MagicMock(), catalog=catalog, state={"bookmarks": {}})

        self.assertEqual(first_stream_obj.sync.call_count, 1)
        self.assertEqual(second_stream_obj.sync.call_count, 1)

    @patch("tap_yotpo.sync.singer.metadata.to_map", return_value={})
    @patch("tap_yotpo.sync.singer.write_schema")
    @patch("tap_yotpo.sync.singer.write_state")
    @patch("tap_yotpo.sync.singer.set_currently_syncing", side_effect=lambda state, stream: state)
    @patch("tap_yotpo.sync.singer.Transformer", return_value=DummyTransformer())
    def test_sync_raises_unexpected_exception(
        self,
        _mock_transformer,
        _mock_set_currently_syncing,
        _mock_write_state,
        _mock_write_schema,
        _mock_to_map,
    ):
        stream_obj = MagicMock()
        stream_obj.key_properties = ["id"]
        stream_obj.sync.side_effect = RuntimeError("unexpected failure")

        with patch.dict(
            "tap_yotpo.sync.streams.STREAMS",
            {"stream_1": lambda _client: stream_obj},
            clear=False,
        ):
            catalog = DummyCatalog([DummyCatalogEntry("stream_1")])
            with self.assertRaises(RuntimeError):
                sync(client=MagicMock(), catalog=catalog, state={"bookmarks": {}})
