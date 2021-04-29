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

from rethinkdb import ast, ql2_pb2

#
# All top level functions defined here are the starting points for RQL queries
#


def json(*args):
    """
    Transform *args parameters into JSON.
    """
    return ast.Json(*args)


def js(*args, **kwargs):
    """
    Create a javascript expression.
    """
    return ast.JavaScript(*args, **kwargs)


def args(*args):
    """
    r.args is a special term that’s used to splice an array of arguments into
    another term. This is useful when you want to call a variadic term such as
    get_all with a set of arguments produced at runtime.

    This is analogous to unpacking argument lists in Python. (However, note
    that args evaluates all its arguments before passing them into the parent
    term, even if the parent term otherwise allows lazy evaluation.)
    """
    return ast.Args(*args)


def http(url, **kwargs):
    """
    Retrieve data from the specified URL over HTTP. The return type depends on
    the result_format option, which checks the Content-Type of the response by
    default. Make sure that you never use this command for user provided URLs.
    """
    return ast.Http(ast.func_wrap(url), **kwargs)


def error(*msg):
    """
    Throw a runtime error. If called with no arguments inside the second
    argument to default, re-throw the current error.
    """
    return ast.UserError(*msg)


def random(*args, **kwargs):
    """
    Generate a random number between given (or implied) bounds. random takes
    zero, one or two arguments.


    * With zero arguments, the result will be a floating-point number in the
      range [0,1) (from 0 up to but not including 1).
    * With one argument x, the result will be in the range [0,x), and will be
      integer unless float=True is given as an option. Specifying a floating
      point number without the float option will raise an error.
    * With two arguments x and y, the result will be in the range [x,y), and
      will be integer unless float=True is given as an option. If x and y are
      equal an error will occur, unless the floating-point option has been
      specified, in which case x will be returned. Specifying a floating point
      number without the float option will raise an error.

    Note: The last argument given will always be the ‘open’ side of the range,
    but when generating a floating-point number, the ‘open’ side may be less
    than the ‘closed’ side.
    """
    return ast.Random(*args, **kwargs)


def do(*args):
    """
    Call an anonymous function using return values from other ReQL commands or
    queries as arguments.

    The last argument to do (or, in some forms, the only argument) is an
    expression or an anonymous function which receives values from either the
    previous arguments or from prefixed commands chained before do. The do
    command is essentially a single-element map, letting you map a function
    over just one document. This allows you to bind a query result to a local
    variable within the scope of do, letting you compute the result just once
    and reuse it in a complex expression or in a series of ReQL commands.

    Arguments passed to the do function must be basic data types, and cannot
    be streams or selections. (Read about ReQL data types.) While the
    arguments will all be evaluated before the function is executed, they may
    be evaluated in any order, so their values should not be dependent on one
    another. The type of do’s result is the type of the value returned from the
    function or last expression.
    """
    return ast.FunCall(*args)


row = ast.ImplicitVar()


def table(*args, **kwargs):
    """
    Return all documents in a table. Other commands may be chained after table
    to return a subset of documents (such as get and filter) or perform further
    processing.
    """
    return ast.Table(*args, **kwargs)


def db(*args):
    """
    Reference a database.

    The db command is optional. If it is not present in a query, the query will
    run against the database specified in the db argument given to run if one
    was specified. Otherwise, the query will run against the default database
    for the connection, specified in the db argument to connect.
    """
    return ast.DB(*args)


def db_create(*args):
    """
    Create a database. A RethinkDB database is a collection of tables, similar
    to relational databases.
    """
    return ast.DbCreate(*args)


def db_drop(*args):
    """
    Drop a database. The database, all its tables, and corresponding data will
    be deleted.
    """
    return ast.DbDrop(*args)


def db_list(*args):
    """
    List all database names in the system. The result is a list of strings.
    """
    return ast.DbList(*args)


def db_config(*args):
    """
    TODO: find the proper description.
    """
    return ast.DbConfig(*args)


def table_create(*args, **kwargs):
    """
    Create a table. A RethinkDB table is a collection of JSON documents.
    """
    return ast.TableCreateTL(*args, **kwargs)


def table_drop(*args):
    """
    Drop a table. The table and all its data will be deleted.
    """
    return ast.TableDropTL(*args)


def table_list(*args):
    """
    List all table names in a database. The result is a list of strings.
    """
    return ast.TableListTL(*args)


def grant(*args, **kwargs):
    """
    Grant or deny access permissions for a user account, globally or on a
    per-database or per-table basis.
    """
    return ast.GrantTL(*args, **kwargs)


def branch(*args):
    """
    Perform a branching conditional equivalent to if-then-else.

    The branch command takes 2n+1 arguments: pairs of conditional expressions
    and commands to be executed if the conditionals return any value but False
    or None (i.e., “truthy” values), with a final “else” command to be
    evaluated if all of the conditionals are False or None.
    """
    return ast.Branch(*args)


def union(*args):
    """
    Merge two or more sequences.
    """
    return ast.Union(*args)


def map(*args):
    """
    Transform each element of one or more sequences by applying a mapping
    function to them. If map is run with two or more sequences, it will
    iterate for as many items as there are in the shortest sequence.

    Note that map can only be applied to sequences, not single values. If
    you wish to apply a function to a single value/selection (including an
    array), use the do command.
    """
    if len(args) > 0:
        # `func_wrap` only the last argument
        return ast.Map(*(args[:-1] + (ast.func_wrap(args[-1]),)))

    return ast.Map()


#################################
## Aggregation Functionalities ##
#################################


def group(*args):
    """
    Takes a stream and partitions it into multiple groups based on the fields
    or functions provided.

    With the multi flag single documents can be assigned to multiple groups,
    similar to the behavior of multi-indexes. When multi is True and the
    grouping value is an array, documents will be placed in each group that
    corresponds to the elements of the array. If the array is empty the row
    will be ignored.
    """
    return ast.Group(*[ast.func_wrap(arg) for arg in args])


def reduce(*args):
    """
    Produce a single value from a sequence through repeated application of a
    reduction function.
    """
    return ast.Reduce(*[ast.func_wrap(arg) for arg in args])


def count(*args):
    """
    Counts the number of elements in a sequence or key/value pairs in an
    object, or returns the size of a string or binary object.

    When count is called on a sequence with a predicate value or function, it
    returns the number of elements in the sequence equal to that value or
    where the function returns True. On a binary object, count returns the
    size of the object in bytes; on strings, count returns the string’s length.
    This is determined by counting the number of Unicode codepoints in the
    string, counting combining codepoints separately.
    """
    return ast.Count(*[ast.func_wrap(arg) for arg in args])


def sum(*args):
    """
    Sums all the elements of a sequence. If called with a field name, sums all
    the values of that field in the sequence, skipping elements of the sequence
    that lack that field. If called with a function, calls that function on
    every element of the sequence and sums the results, skipping elements of
    the sequence where that function returns None or a non-existence error.

    Returns 0 when called on an empty sequence.
    """
    return ast.Sum(*[ast.func_wrap(arg) for arg in args])


def avg(*args):
    """
    Averages all the elements of a sequence. If called with a field name,
    averages all the values of that field in the sequence, skipping elements of
    the sequence that lack that field. If called with a function, calls that
    function on every element of the sequence and averages the results, skipping
    elements of the sequence where that function returns None or a non-existence
    error.

    Produces a non-existence error when called on an empty sequence. You can
    handle this case with default.
    """
    return ast.Avg(*[ast.func_wrap(arg) for arg in args])


def min(*args):
    """
    Finds the minimum element of a sequence.
    """
    return ast.Min(*[ast.func_wrap(arg) for arg in args])


def max(*args):
    """
    Finds the maximum element of a sequence.
    """
    return ast.Max(*[ast.func_wrap(arg) for arg in args])


def distinct(*args):
    """
    Removes duplicate elements from a sequence.

    The distinct command can be called on any sequence or table with an index.
    """
    return ast.Distinct(*[ast.func_wrap(arg) for arg in args])


def contains(*args):
    """
    When called with values, returns True if a sequence contains all the
    specified values. When called with predicate functions, returns True if
    for each predicate there exists at least one element of the stream where
    that predicate returns True.

    Values and predicates may be mixed freely in the argument list.
    """
    return ast.Contains(*[ast.func_wrap(arg) for arg in args])


############################
## Ordering functionality ##
############################


def asc(*args):
    """
    Sort the sequence by document values of the given key(s). To specify the
    ordering, wrap the attribute with either r.asc or r.desc (defaults to
    ascending).
    """
    return ast.Asc(*[ast.func_wrap(arg) for arg in args])


def desc(*args):
    """
    Sort the sequence by document values of the given key(s). To specify the
    ordering, wrap the attribute with either r.asc or r.desc (defaults to
    ascending).
    """
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
        """
        Compose Rql statement.
        """
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
    """
    Replace an object in a field instead of merging it with an existing object
    in a merge or update operation. = Using literal with no arguments in a
    merge or update operation will remove the corresponding field.
    """
    return ast.Literal(*args)


def object(*args):
    """
    Creates an object from a list of key-value pairs, where the keys must be
    strings. r.object(A, B, C, D) is equivalent to
    r.expr([[A, B], [C, D]]).coerce_to('OBJECT').
    """
    return ast.Object(*args)


def uuid(*args):
    """
    Return a UUID (universally unique identifier), a string that can be used as
    a unique ID. If a string is passed to uuid as an argument, the UUID will be
    deterministic, derived from the string’s SHA-1 hash.
    """
    return ast.UUID(*args)


##############
# Geospatial #
##############


# Global geospatial operations
def geojson(*args):
    """
    Convert a GeoJSON object to a ReQL geometry object.

    RethinkDB only allows conversion of GeoJSON objects which have ReQL
    equivalents: Point, LineString, and Polygon. MultiPoint, MultiLineString,
    and MultiPolygon are not supported. (You could, however, store multiple
    points, lines and polygons in an array and use a geospatial multi index
    with them.)
    """
    return ast.GeoJson(*args)


def point(*args):
    """
    Construct a geometry object of type Point. The point is specified by two
    floating point numbers, the longitude (−180 to 180) and latitude (−90 to 90)
    of the point on a perfect sphere
    """
    return ast.Point(*args)


def line(*args):
    """
    Construct a geometry object of type Line.
    """
    return ast.Line(*args)


def polygon(*args):
    """
    Construct a geometry object of type Polygon.
    """
    return ast.Polygon(*args)


def distance(*args, **kwargs):
    """
    Compute the distance between a point and another geometry object. At least
    one of the geometry objects specified must be a point.
    """
    return ast.Distance(*args, **kwargs)


def intersects(*args):
    """
    Tests whether two geometry objects intersect with one another. When applied
    to a sequence of geometry objects, intersects acts as a filter, returning a
    sequence of objects from the sequence that intersect with the argument.
    """
    return ast.Intersects(*args)


def circle(*args, **kwargs):
    """
    Construct a circular line or polygon. A circle in RethinkDB is a polygon or
    line approximating a circle of a given radius around a given center,
    consisting of a specified number of vertices (default 32).
    """
    return ast.Circle(*args, **kwargs)
