# Tests for query.py
from unittest.mock import patch


from rethinkdb.query import (
  json,
  js
)


@patch("rethinkdb.query.ast")
def test_json(mock_ast):
  mock_ast.Json.return_value = Mock()
  
  result = json("foo")
  mock_ast.assert_called_once_with("foo")
  
  assert result == mock_ast.Json.return_value
  """"
  with patch("rethinkdb.query.json") as json_mock:
    json_mock("foo")
    
    json_mock.assert_called_once_with("foo")
  """

@patch("rethinkdb.query.ast")
def test_js(mock_ast):
  mock_ast.JavaScript.return_value = Mock()
  
  result = js("foo", foo="foo")
  mock_ast.assert.called_once_with("foo", foo="foo")
  
  assert result == mock_ast.JavaScript.return_value
