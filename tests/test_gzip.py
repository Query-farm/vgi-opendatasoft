"""Portals that gzip responses unasked (opendata.paris.fr) are decoded."""

import gzip
import io
import json
from email.message import Message

from opendatasoft_worker import client


class _Resp(io.BytesIO):
    def __init__(self, body: bytes, encoding: str | None) -> None:
        super().__init__(body)
        self.headers = Message()
        if encoding:
            self.headers["Content-Encoding"] = encoding


def test_gzip_response_is_decoded_and_closes_the_socket() -> None:
    raw = _Resp(gzip.compress(b'{"dataset_id": "arrondissements"}'), "gzip")
    with client._decoded(raw) as response:
        assert json.load(response) == {"dataset_id": "arrondissements"}
    assert raw.closed


def test_plain_response_passes_through() -> None:
    raw = _Resp(b"{}", None)
    assert client._decoded(raw) is raw
