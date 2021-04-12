# Tests for query.py
from unittest.mock import Mock, patch


from rethinkdb.query import (
  json,
  js,
  args,
  http,
  error,
  random,
  do,
  table,
  db,
  db_create,
  db_drop,
  table_list,
  grant,
  branch,
  union,
  map,
  group,
  reduce,
  count,
  sum,
  avg,
  min,
  max,
  distinct,
  contains
)


@patch("rethinkdb.query.ast")
def test_json(mock_ast):
  mock_ast.Json.return_value = Mock()
  
  result = json("foo")
  mock_ast.Json.assert_called_once_with("foo")
  
  assert result == mock_ast.Json.return_value


@patch("rethinkdb.query.ast")
def test_js(mock_ast):
  mock_ast.JavaScript.return_value = Mock()
  
  result = js("foo", foo="foo")
  mock_ast.JavaScript.assert_called_once_with("foo", foo="foo")
  
  assert result == mock_ast.JavaScript.return_value
  

@patch("rethinkdb.query.ast")
def test_args(mock_ast):
  mock_ast.Args.return_value = Mock()
  
  result = args("foo")
  mock_ast.Args.assert_called_once_with("foo")
  
  assert result == mock_ast.Args.return_value
  
  
@patch("rethinkdb.query.ast")
def test_http(mock_ast):
  mock_ast.Http.return_value = Mock()
  
  result = http("foo", foo="foo")
  mock_ast.Http.assert_called_once_with("foo", foo="foo")
  
  assert result == mock_ast.Http.return_value
  
  
@patch("rethinkdb.query.ast")
def test_error(mock_ast):
  mock_ast.UserError.return_value = Mock()
  
  result = error("foo")
  mock_ast.UserError.assert_called_once_with("foo")
  
  assert result == mock_ast.UserError.return_value
  
  
@patch("rethinkdb.query.ast")
def test_random(mock_ast):
  mock_ast.Random.return_value = Mock()
  
  result = random("foo", foo="foo")
  mock_ast.Random.assert_called_once_with("foo", foo="foo")
  
  assert result == mock_ast.Random.return_value
  
  
@patch("rethinkdb.query.ast")
def test_do(mock_ast):
  mock_ast.FunCall.return_value = Mock()
  
  result = do("foo")
  mock_ast.FunCall.assert_called_once_with("foo")
  
  assert result == mock_ast.FunCall.return_value

  
@patch("rethinkdb.query.ast")
def test_table(mock_ast):
  mock_ast.Table.return_value = Mock()
  
  result = table("foo", foo="foo")
  mock_ast.Table.assert_called_once_with("foo", foo="foo")
  
  assert result == mock_ast.Table.return_value
  
  
@patch("rethinkdb.query.ast")
def test_db(mock_ast):
  mock_ast.DB.return_value = Mock()
  
  result = db("foo")
  mock_ast.DB.assert_called_once_with("foo")
  
  assert result == mock_ast.DB.return_value
  
  
@patch("rethinkdb.query.ast")
def test_db_create(mock_ast):
  mock_ast.DbCreate.return_value = Mock()
  
  result = db_create("foo")
  mock_ast.DbCreate.assert_called_once_with("foo")
  
  assert result == mock_ast.DbCreate.return_value
  
  
@patch("rethinkdb.query.ast")
def test_db_drop(mock_ast):
  mock_ast.DbDrop.return_value = Mock()
  
  result = db_drop("foo")
  mock_ast.DbDrop.assert_called_once_with("foo")
  
  assert result == mock_ast.DbDrop.return_value
  
  
@patch("rethinkdb.query.ast")
def test_table_list(mock_ast):
  mock_ast.TableListTL.return_value = Mock()
  
  result = table_list("foo")
  mock_ast.TableListTL.assert_called_once_with("foo")
  
  assert result == mock_ast.TableListTL.return_value
  
  
@patch("rethinkdb.query.ast")
def test_grant(mock_ast):
  mock_ast.GrantTL.return_value = Mock()
  
  result = grant("foo", foo="foo")
  mock_ast.GrantTL.assert_called_once_with("foo", foo="foo")
  
  assert result == mock_ast.GrantTL.return_value
  
  
@patch("rethinkdb.query.ast")
def test_branch(mock_ast):
  mock_ast.Branch.return_value = Mock()
  
  result = branch("foo")
  mock_ast.Branch.assert_called_once_with("foo")
  
  assert result == mock_ast.Branch.return_value
  
  
  
@patch("rethinkdb.query.ast")
def test_union(mock_ast):
  mock_ast.Union.return_value = Mock()
  
  result = union("foo")
  mock_ast.Union.assert_called_once_with("foo")
  
  assert result == mock_ast.Union.return_value
  
  
@patch("rethinkdb.query.ast")
def test_map(mock_ast):
  mock_ast.Map.return_value = Mock()
  
  result = map(["foo"])
  mock_ast.Map.assert_called_once_with(["foo"])
  
  assert result == mock_ast.Map.return_value
  
  
@patch("rethinkdb.query.ast")
def test_group(mock_ast):
  mock_ast.Group.return_value = Mock()
  
  result = group(["foo"])
  mock_ast.Group.assert_called_once_with("foo")
  
  assert result == mock_ast.Group.return_value
  
  
@patch("rethinkdb.query.ast")
def test_reduce(mock_ast):
  mock_ast.Reduce.return_value = Mock()
  
  result = reduce(["foo"])
  mock_ast.Reduce.assert_called_once_with("foo")
  
  assert result == mock_ast.Reduce.return_value
  
  
@patch("rethinkdb.query.ast")
def test_count(mock_ast):
  mock_ast.Count.return_value = Mock()
  
  result = count(["foo"])
  mock_ast.Count.assert_called_once_with("foo")
  
  assert result == mock_ast.Count.return_value
  
  
@patch("rethinkdb.query.ast")
def test_sum(mock_ast):
  mock_ast.Sum.return_value = Mock()
  
  result = sum(["foo"])
  mock_ast.Sum.assert_called_once_with("foo")
  
  assert result == mock_ast.Sum.return_value
  
  
@patch("rethinkdb.query.ast")
def test_avg(mock_ast):
  mock_ast.Avg.return_value = Mock()
  
  result = avg(["foo"])
  mock_ast.Avg.assert_called_once_with("foo")
  
  assert result == mock_ast.Avg.return_value
  
  
@patch("rethinkdb.query.ast")
def test_min(mock_ast):
  mock_ast.Min.return_value = Mock()
  
  result = min(["foo"])
  mock_ast.Min.assert_called_once_with("foo")
  
  assert result == mock_ast.Min.return_value
  
  
@patch("rethinkdb.query.ast")
def test_max(mock_ast):
  mock_ast.Max.return_value = Mock()
  
  result = max(["foo"])
  mock_ast.Max.assert_called_once_with("foo")
  
  assert result == mock_ast.Max.return_value
  
  
@patch("rethinkdb.query.ast")
def test_distinct(mock_ast):
  mock_ast.Distinct.return_value = Mock()
  
  result = distinct(["foo"])
  mock_ast.Distinct.assert_called_once_with("foo")
  
  assert result == mock_ast.Distinct.return_value
  
  
@patch("rethinkdb.query.ast")
def test_contains(mock_ast):
  mock_ast.Contains.return_value = Mock()
  
  result = contains(["foo"])
  mock_ast.Contains.assert_called_once_with("foo")
  
  assert result == mock_ast.Contains.return_value
