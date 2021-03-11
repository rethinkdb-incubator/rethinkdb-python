# Copyright 2020 RethinkDB
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This file incorporates work covered by the following copyright:
# Copyright 2010-2014 RethinkDB, all rights reserved.

__all__ = [
    "js",
    "http",
    "json",
    "args",
    "error",
    "random",
    "do",
    "row",
    "branch",
    "union",
    "map",
    "object",
    "binary",
    "uuid",
    "type_of",
    "info",
    "range",
    "literal",
    "asc",
    "desc",
    "db",
    "db_create",
    "db_drop",
    "db_list",
    "table",
    "table_create",
    "table_drop",
    "table_list",
    "grant",
    "group",
    "reduce",
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "distinct",
    "contains",
    "eq",
    "ne",
    "le",
    "ge",
    "lt",
    "gt",
    "and_",
    "or_",
    "not_",
    "add",
    "sub",
    "mul",
    "div",
    "mod",
    "bit_and",
    "bit_or",
    "bit_xor",
    "bit_not",
    "bit_sal",
    "bit_sar",
    "floor",
    "ceil",
    "round",
    "time",
    "iso8601",
    "epoch_time",
    "now",
    "make_timezone",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
    "minval",
    "maxval",
    "geojson",
    "point",
    "line",
    "polygon",
    "distance",
    "intersects",
    "circle",
]

from . import ast

#
# All top level functions defined here are the starting points for RQL queries
#


def json(*args):
    """
    Transform *args parameters into JSON.
    """
    return ast.Json(*args)


def js(*args, **kwargs):
    return ast.JavaScript(*args, **kwargs)


def args(*args):
    return ast.Args(*args)


def http(url, **kwargs):
    return ast.Http(ast.func_wrap(url), **kwargs)


def error(*msg):
    return ast.UserError(*msg)


def random(*args, **kwargs):
    return ast.Random(*args, **kwargs)


def do(*args):
    return ast.FunCall(*args)


row = ast.ImplicitVar()


def table(*args, **kwargs):
    return ast.Table(*args, **kwargs)


def db(*args):
    return ast.DB(*args)


def db_create(*args):
    return ast.DbCreate(*args)


def db_drop(*args):
    return ast.DbDrop(*args)


def db_list(*args):
    return ast.DbList(*args)


def db_config(*args):
    return ast.DbConfig(*args)


def table_create(*args, **kwargs):
    return ast.TableCreateTL(*args, **kwargs)


def table_drop(*args):
    return ast.TableDropTL(*args)


def table_list(*args):
    return ast.TableListTL(*args)


def grant(*args, **kwargs):
    return ast.GrantTL(*args, **kwargs)


def branch(*args):
    return ast.Branch(*args)


def union(*args):
    return ast.Union(*args)


def map(*args):
    if len(args) > 0:
        # `func_wrap` only the last argument
        return ast.Map(*(args[:-1] + (ast.func_wrap(args[-1]),)))
    else:
        return ast.Map()

#################################
## Aggregation Functionalities ##
#################################


def group(*args):
    return ast.Group(*[ast.func_wrap(arg) for arg in args])


def reduce(*args):
    return ast.Reduce(*[ast.func_wrap(arg) for arg in args])


def count(*args):
    return ast.Count(*[ast.func_wrap(arg) for arg in args])


def sum(*args):
    return ast.Sum(*[ast.func_wrap(arg) for arg in args])


def avg(*args):
    return ast.Avg(*[ast.func_wrap(arg) for arg in args])


def min(*args):
    return ast.Min(*[ast.func_wrap(arg) for arg in args])


def max(*args):
    return ast.Max(*[ast.func_wrap(arg) for arg in args])


def distinct(*args):
    return ast.Distinct(*[ast.func_wrap(arg) for arg in args])


def contains(*args):
    return ast.Contains(*[ast.func_wrap(arg) for arg in args])


############################
## Ordering functionality ##
############################

def asc(*args):
    return ast.Asc(*[ast.func_wrap(arg) for arg in args])


def desc(*args):
    return ast.Desc(*[ast.func_wrap(arg) for arg in args])


####################
## Math and Logic ##
####################

def eq(*args):
    """
    Equals function.
    """
    return ast.Eq(*args)


def ne(*args):
    """
    Not equal function.
    """
    return ast.Ne(*args)


def lt(*args):
    """
    Less than function.
    """
    return ast.Lt(*args)


def le(*args):
    """
    Less or equal than function.
    """
    return ast.Le(*args)


def gt(*args):
    """
    Greater than function.
    """
    return ast.Gt(*args)


def ge(*args):
    """
    Greater or equal than function.
    """
    return ast.Ge(*args)


def add(*args):
    """
    Add function.
    """
    return ast.Add(*args)


def sub(*args):
    """
    Subtract function.
    """
    return ast.Sub(*args)


def mul(*args):
    """
    Multiply function.
    """
    return ast.Mul(*args)


def div(*args):
    """
    Divide function.
    """
    return ast.Div(*args)


def mod(*args):
    """
    Module function.
    """
    return ast.Mod(*args)


def bit_and(*args):
    """
    Bitwise AND function.
    """
    return ast.BitAnd(*args)


def bit_or(*args):
    """
    Bitwise OR function.
    """
    return ast.BitOr(*args)


def bit_xor(*args):
    """
    Bitwise XOR function.
    """
    return ast.BitXor(*args)


def bit_not(*args):
    """
    Bit negation function.
    """
    return ast.BitNot(*args)


def bit_sal(*args):
    return ast.BitSal(*args)


def bit_sar(*args):
    return ast.BitSar(*args)


def floor(*args):
    """
    Floor function.
    """
    return ast.Floor(*args)


def ceil(*args):
    """
    Ceil function.
    """
    return ast.Ceil(*args)


def round(*args):
    """
    Round function.
    """
    return ast.Round(*args)


def not_(*args):
    """
    Not function.
    """
    return ast.Not(*args)


def and_(*args):
    """
    AND function.
    """
    return ast.And(*args)


def or_(*args):
    """
    OR function.
    """
    return ast.Or(*args)


def type_of(*args):
    """
    Type function.
    """
    return ast.TypeOf(*args)


def info(*args):
    """
    Information function.
    """
    return ast.Info(*args)


def binary(data):
    """
    Binary function.
    """
    return ast.Binary(data)


def range(*args):
    """
    Range function.
    """
    return ast.Range(*args)


##################
## Time-related ##
##################

def make_timezone(*args):
    """
    Add timezone function.
    """
    return ast.RqlTzinfo(*args)


def time(*args):
    """
    Time function.
    """
    return ast.Time(*args)


def iso8601(*args, **kwargs):
    """
    ISO8601 function.
    """
    return ast.ISO8601(*args, **kwargs)


def epoch_time(*args):
    """
    Epoch time function.
    """
    return ast.EpochTime(*args)


def now(*args):
    """
    Now function.
    """
    return ast.Now(*args)


class RqlConstant(ast.RqlQuery):
    """
    Rethinkdb constant.
    Maps a real world constant to a representation for the db.
    """
    def __init__(self, statement, term_type):
        self.statement = statement
        self.term_type = term_type
        super(RqlConstant, self).__init__()

    def compose(self, args, optargs):
        return "r." + self.statement


# Time enum values
# Convert days of week into constants
monday = RqlConstant("monday", ql2_pb2.Term.TermType.MONDAY)
tuesday = RqlConstant("tuesday", ql2_pb2.Term.TermType.TUESDAY)
wednesday = RqlConstant("wednesday", ql2_pb2.Term.TermType.WEDNESDAY)
thursday = RqlConstant("thursday", ql2_pb2.Term.TermType.THURSDAY)
friday = RqlConstant("friday", ql2_pb2.Term.TermType.FRIDAY)
saturday = RqlConstant("saturday", ql2_pb2.Term.TermType.SATURDAY)
sunday = RqlConstant("sunday", ql2_pb2.Term.TermType.SUNDAY)

# Convert months of the year into constants
january = RqlConstant("january", ql2_pb2.Term.TermType.JANUARY)
february = RqlConstant("february", ql2_pb2.Term.TermType.FEBRUARY)
march = RqlConstant("march", ql2_pb2.Term.TermType.MARCH)
april = RqlConstant("april", ql2_pb2.Term.TermType.APRIL)
may = RqlConstant("may", ql2_pb2.Term.TermType.MAY)
june = RqlConstant("june", ql2_pb2.Term.TermType.JUNE)
july = RqlConstant("july", ql2_pb2.Term.TermType.JULY)
august = RqlConstant("august", ql2_pb2.Term.TermType.AUGUST)
september = RqlConstant("september", ql2_pb2.Term.TermType.SEPTEMBER)
october = RqlConstant("october", ql2_pb2.Term.TermType.OCTOBER)
november = RqlConstant("november", ql2_pb2.Term.TermType.NOVEMBER)
december = RqlConstant("december", ql2_pb2.Term.TermType.DECEMBER)

minval = RqlConstant("minval", ql2_pb2.Term.TermType.MINVAL)
maxval = RqlConstant("maxval", ql2_pb2.Term.TermType.MAXVAL)


# Merge values
def literal(*args):
    return ast.Literal(*args)


def object(*args):
    return ast.Object(*args)


def uuid(*args):
    return ast.UUID(*args)


##############
# Geospatial #
##############


# Global geospatial operations
def geojson(*args):
    return ast.GeoJson(*args)


def point(*args):
    return ast.Point(*args)


def line(*args):
    return ast.Line(*args)


def polygon(*args):
    return ast.Polygon(*args)


def distance(*args, **kwargs):
    return ast.Distance(*args, **kwargs)


def intersects(*args):
    return ast.Intersects(*args)


def circle(*args, **kwargs):
    return ast.Circle(*args, **kwargs)
