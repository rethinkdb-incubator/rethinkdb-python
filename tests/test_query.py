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
  contains,
  asc,
  desc,
  eq,
  ne,
  lt,
  le,
  gt,
  ge,
  add,
  sub,
  mul,
  div,
  mod,
  bit_and,
  bit_or,
  bit_xor,
  bit_not,
  bit_sal,
  bit_sar,
  floor,
  ceil,
  round,
  not_,
  and_,
  or_,
  type_of,
  info,
  binary,
  range
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
  
  
@patch("rethinkdb.query.ast")
def test_asc(mock_ast):
  mock_ast.Asc.return_value = Mock()
  
  result = asc(["foo"])
  mock_ast.Asc.assert_called_once_with("foo")
  
  assert result == mock_ast.Asc.return_value

  
@patch("rethinkdb.query.ast")
def test_desc(mock_ast):
  mock_ast.Desc.return_value = Mock()
  
  result = desc(["foo"])
  mock_ast.Desc.assert_called_once_with("foo")
  
  assert result == mock_ast.Desc.return_value
  
  
@patch("rethinkdb.query.ast")
def test_eq(mock_ast):
  mock_ast.Eq.return_value = Mock()
  
  result = eq(["foo"])
  mock_ast.Eq.assert_called_once_with("foo")
  
  assert result == mock_ast.Eq.return_value
  
  
@patch("rethinkdb.query.ast")
def test_ne(mock_ast):
  mock_ast.Ne.return_value = Mock()
  
  result = ne(["foo"])
  mock_ast.Ne.assert_called_once_with("foo")
  
  assert result == mock_ast.Ne.return_value
  
  
@patch("rethinkdb.query.ast")
def test_lt(mock_ast):
  mock_ast.Lt.return_value = Mock()
  
  result = lt(["foo"])
  mock_ast.Lt.assert_called_once_with("foo")
  
  assert result == mock_ast.Lt.return_value
  
  
@patch("rethinkdb.query.ast")
def test_le(mock_ast):
  mock_ast.Le.return_value = Mock()
  
  result = le(["foo"])
  mock_ast.Le.assert_called_once_with("foo")
  
  assert result == mock_ast.Le.return_value
  
  
@patch("rethinkdb.query.ast")
def test_gt(mock_ast):
  mock_ast.Gt.return_value = Mock()
  
  result = gt(["foo"])
  mock_ast.Gt.assert_called_once_with("foo")
  
  assert result == mock_ast.Gt.return_value
  
  
@patch("rethinkdb.query.ast")
def test_ge(mock_ast):
  mock_ast.Ge.return_value = Mock()
  
  result = ge(["foo"])
  mock_ast.Ge.assert_called_once_with("foo")
  
  assert result == mock_ast.Ge.return_value
  
  
@patch("rethinkdb.query.ast")
def test_add(mock_ast):
  mock_ast.Add.return_value = Mock()
  
  result = add(["foo"])
  mock_ast.Add.assert_called_once_with("foo")
  
  assert result == mock_ast.Add.return_value
  
  
@patch("rethinkdb.query.ast")
def test_sub(mock_ast):
  mock_ast.Sub.return_value = Mock()
  
  result = sub(["foo"])
  mock_ast.Sub.assert_called_once_with("foo")
  
  assert result == mock_ast.Sub.return_value
  
  
@patch("rethinkdb.query.ast")
def test_mul(mock_ast):
  mock_ast.Mul.return_value = Mock()
  
  result = mul(["foo"])
  mock_ast.Mul.assert_called_once_with("foo")
  
  assert result == mock_ast.Mul.return_value
  
  
@patch("rethinkdb.query.ast")
def test_div(mock_ast):
  mock_ast.Div.return_value = Mock()
  
  result = div(["foo"])
  mock_ast.Div.assert_called_once_with("foo")
  
  assert result == mock_ast.Div.return_value
  
  
@patch("rethinkdb.query.ast")
def test_mod(mock_ast):
  mock_ast.Mod.return_value = Mock()
  
  result = mod(["foo"])
  mock_ast.Mod.assert_called_once_with("foo")
  
  assert result == mock_ast.Mod.return_value
  
  
@patch("rethinkdb.query.ast")
def test_bit_and(mock_ast):
  mock_ast.BitAnd.return_value = Mock()
  
  result = bit_and(["foo"])
  mock_ast.BitAnd.assert_called_once_with("foo")
  
  assert result == mock_ast.BitAnd.return_value
  
  
@patch("rethinkdb.query.ast")
def test_bit_or(mock_ast):
  mock_ast.BitOr.return_value = Mock()
  
  result = bit_or(["foo"])
  mock_ast.BitOr.assert_called_once_with("foo")
  
  assert result == mock_ast.BitOr.return_value
  
  
@patch("rethinkdb.query.ast")
def test_bit_xor(mock_ast):
  mock_ast.BitXor.return_value = Mock()
  
  result = bit_xor(["foo"])
  mock_ast.BitXor.assert_called_once_with("foo")
  
  assert result == mock_ast.BitXor.return_value
  
  
@patch("rethinkdb.query.ast")
def test_bit_not(mock_ast):
  mock_ast.BitNot.return_value = Mock()
  
  result = bit_not(["foo"])
  mock_ast.BitNot.assert_called_once_with("foo")
  
  assert result == mock_ast.BitNot.return_value
  
  
@patch("rethinkdb.query.ast")
def test_bit_sal(mock_ast):
  mock_ast.BitSal.return_value = Mock()
  
  result = bit_sal(["foo"])
  mock_ast.BitSal.assert_called_once_with("foo")
  
  assert result == mock_ast.BitSal.return_value
  
  
@patch("rethinkdb.query.ast")
def test_bit_sar(mock_ast):
  mock_ast.BitSar.return_value = Mock()
  
  result = bit_sar(["foo"])
  mock_ast.BitSar.assert_called_once_with("foo")
  
  assert result == mock_ast.BitSar.return_value
  
  
@patch("rethinkdb.query.ast")
def test_floor(mock_ast):
  mock_ast.Floor.return_value = Mock()
  
  result = floor(["foo"])
  mock_ast.Floor.assert_called_once_with("foo")
  
  assert result == mock_ast.Floor.return_value
  
  
@patch("rethinkdb.query.ast")
def test_ceil(mock_ast):
  mock_ast.Ceil.return_value = Mock()
  
  result = ceil(["foo"])
  mock_ast.Ceil.assert_called_once_with("foo")
  
  assert result == mock_ast.Ceil.return_value
  
  
@patch("rethinkdb.query.ast")
def test_round(mock_ast):
  mock_ast.Round.return_value = Mock()
  
  result = round(["foo"])
  mock_ast.Round.assert_called_once_with("foo")
  
  assert result == mock_ast.Round.return_value
  
  
@patch("rethinkdb.query.ast")
def test_not_(mock_ast):
  mock_ast.Not.return_value = Mock()
  
  result = not_(["foo"])
  mock_ast.Not.assert_called_once_with("foo")
  
  assert result == mock_ast.Not.return_value
  
  
@patch("rethinkdb.query.ast")
def test_and_(mock_ast):
  mock_ast.And.return_value = Mock()
  
  result = and_(["foo"])
  mock_ast.And.assert_called_once_with("foo")
  
  assert result == mock_ast.And.return_value

  
@patch("rethinkdb.query.ast")
def test_or_(mock_ast):
  mock_ast.Or.return_value = Mock()
  
  result = or_(["foo"])
  mock_ast.Or.assert_called_once_with("foo")
  
  assert result == mock_ast.Or.return_value
  
  
@patch("rethinkdb.query.ast")
def test_type_of(mock_ast):
  mock_ast.TypeOf.return_value = Mock()
  
  result = type_of(["foo"])
  mock_ast.TypeOf.assert_called_once_with("foo")
  
  assert result == mock_ast.TypeOf.return_value
  
  
@patch("rethinkdb.query.ast")
def test_info(mock_ast):
  mock_ast.info.return_value = Mock()
  
  result = info(["foo"])
  mock_ast.Info.assert_called_once_with("foo")
  
  assert result == mock_ast.Info.return_value
  
  
@patch("rethinkdb.query.ast")
def test_binary(mock_ast):
  mock_ast.Binary.return_value = Mock()
  
  result = binary(["foo"])
  mock_ast.Binary.assert_called_once_with("foo")
  
  assert result == mock_ast.Binary.return_value
  
  
@patch("rethinkdb.query.ast")
def test_range(mock_ast):
  mock_ast.Range.return_value = Mock()
  
  result = range(["foo"])
  mock_ast.Range.assert_called_once_with("foo")
  
  assert result == mock_ast.Range.return_value
  
