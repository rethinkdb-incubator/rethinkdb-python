# Tests for query.py
from unittest.mock import patch


from rethinkdb.query import (
  json
)


def test_json():
  with patch("rethinkdb.query.json") as json_mock:
    json_mock("foo")
    
    json_mock.assert_called_once_with("foo")
