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
# Copyright 2010-2016 RethinkDB, all rights reserved.

"""
AST module contains the way the queries are serialized and deserialized.
"""

# It is known and expected that the ast module will be lot longer than the
# usual module length, so we disabled it.
# pylint: disable=too-many-lines

# TODO: Check that we pass the right parameters when calling super's init.

# FIXME: do a major refactoring and re-enable docstring checks
# pylint: disable=missing-function-docstring,missing-class-docstring

__all__ = ["expr", "RqlQuery", "RqlBinary", "RqlTzinfo"]

import base64
import binascii
from collections import abc
import datetime
import threading
from typing import Any, Callable, Iterable, List, Mapping, Optional
from typing import Union as TUnion

from rethinkdb import ql2_pb2
from rethinkdb.errors import QueryPrinter, ReqlDriverCompileError, ReqlDriverError
from rethinkdb.repl import Repl
from rethinkdb.utilities import EnhancedTuple

P_TERM = ql2_pb2.Term.TermType  # pylint: disable=invalid-name


class RqlQuery:  # pylint: disable=too-many-public-methods
    """
    The RethinkDB Query object which determines the operations we can request
    from the server.
    """

    def __init__(self, *args, **kwargs: dict):
        self._args = [expr(e) for e in args]
        self.kwargs = {k: expr(v) for k, v in kwargs.items()}
        self.term_type: Optional[int] = None

        # TODO: when doing a refactor, make this parameter required and use
        # proper `super` call everywhere we reuse this class.
        self.statement: str = ""

    # TODO: add Connection type to connection when net module is migrated
    # TODO: add return value when net module is migrated
    def run(self, connection=None, **global_optargs: dict):
        """
        Send the query to the server for execution and return the result of the
        evaluation.
        """

        repl = Repl()
        connection = connection or repl.get_connection()

        if connection is None:
            if repl.is_repl_active:
                raise ReqlDriverError(
                    "RqlQuery.run must be given a connection to run on. "
                    "A default connection has been set with "
                    "`repl()` on another thread, but not this one."
                )

            raise ReqlDriverError("RqlQuery.run must be given a connection to run on.")

        return connection._start(  # pylint: disable=protected-access
            self, **global_optargs
        )

    def __str__(self) -> str:
        """
        Return the string representation of the query.
        """

        return QueryPrinter(self).query

    def __repr__(self) -> str:
        """
        Return the representation string of the object.
        """

        return f"<RqlQuery instance: {str(self)} >"

    def build(self) -> List[str]:
        """
        Compile the query to a json-serializable object.
        """

        # TODO: Have a more specific typing here
        res: List[Any] = [self.term_type, self._args]

        if len(self.kwargs) > 0:
            res.append(self.kwargs)

        return res

    # The following are all operators and methods that operate on
    # Rql queries to build up more complex operations

    # Comparison operators
    def __eq__(self, other):
        return Eq(self, other)

    def __ne__(self, other):
        return Ne(self, other)

    def __lt__(self, other):
        return Lt(self, other)

    def __le__(self, other):
        return Le(self, other)

    def __gt__(self, other):
        return Gt(self, other)

    def __ge__(self, other):
        return Ge(self, other)

    # Numeric operators
    def __invert__(self):
        return Not(self)

    def __add__(self, other):
        return Add(self, other)

    def __radd__(self, other):
        return Add(other, self)

    def __sub__(self, other):
        return Sub(self, other)

    def __rsub__(self, other):
        return Sub(other, self)

    def __mul__(self, other):
        return Mul(self, other)

    def __rmul__(self, other):
        return Mul(other, self)

    def __div__(self, other):
        return Div(self, other)

    def __rdiv__(self, other):
        return Div(other, self)

    def __truediv__(self, other):
        return Div(self, other)

    def __rtruediv__(self, other):
        return Div(other, self)

    def __mod__(self, other):
        return Mod(self, other)

    def __rmod__(self, other):
        return Mod(other, self)

    def __and__(self, other):
        query = And(self, other)
        query.set_infix()
        return query

    def __rand__(self, other):
        query = And(other, self)
        query.set_infix()
        return query

    def __or__(self, other):
        query = Or(self, other)
        query.set_infix()
        return query

    def __ror__(self, other):
        query = Or(other, self)
        query.set_infix()
        return query

    # Non-operator versions of the above
    def eq(self, *args):  # pylint: disable=invalid-name
        """
        Non-operator version of ``__eq__``.
        """
        return Eq(self, *args)

    def ne(self, *args):  # pylint: disable=invalid-name
        """
        Non-operator version of ``__ne__``.
        """
        return Ne(self, *args)

    def lt(self, *args):  # pylint: disable=invalid-name
        """
        Non-operator version of ``__lt__``.
        """
        return Lt(self, *args)

    def le(self, *args):  # pylint: disable=invalid-name
        """
        Non-operator version of ``__le__``.
        """
        return Le(self, *args)

    def gt(self, *args):  # pylint: disable=invalid-name
        """
        Non-operator version of ``__gt__``.
        """
        return Gt(self, *args)

    def ge(self, *args):  # pylint: disable=invalid-name
        """
        Non-operator version of ``__ge__``.
        """
        return Ge(self, *args)

    def add(self, *args):
        """
        Non-operator version of ``__add__``.
        """
        return Add(self, *args)

    def sub(self, *args):
        """
        Non-operator version of ``__sub__``.
        """
        return Sub(self, *args)

    def mul(self, *args):
        """
        Non-operator version of ``__mul__``.
        """
        return Mul(self, *args)

    def div(self, *args):
        """
        Non-operator version of ``__div__``.
        """
        return Div(self, *args)

    def mod(self, *args):
        """
        Non-operator version of ``__mod__``.
        """
        return Mod(self, *args)

    def bit_and(self, *args):
        """
        Bitwise AND operator.

        A bitwise AND is a binary operation that takes two equal-length binary
        representations and performs the logical AND operation on each pair of
        the corresponding bits, which is equivalent to multiplying them. Thus,
        if both bits in the compared position are 1, the bit in the resulting
        binary representation is 1 (1 × 1 = 1); otherwise, the result is
        0 (1 × 0 = 0 and 0 × 0 = 0).
        """
        return BitAnd(self, *args)

    def bit_or(self, *args):
        """
        Bitwise OR operator.

        A bitwise OR is a binary operation that takes two bit patterns of equal
        length and performs the logical inclusive OR operation on each pair of
        corresponding bits. The result in each position is 0 if both bits are 0,
        while otherwise the result is 1.
        """
        return BitOr(self, *args)

    def bit_xor(self, *args):
        """
        Bitwise XOR operator.

        A bitwise XOR is a binary operation that takes two bit patterns of equal
        length and performs the logical exclusive OR operation on each pair of
        corresponding bits. The result in each position is 1 if only the first
        bit is 1 or only the second bit is 1, but will be 0 if both are 0 or
        both are 1. In this we perform the comparison of two bits, being 1 if
        the two bits are different, and 0 if they are the same.
        """
        return BitXor(self, *args)

    def bit_not(self, *args):
        """
        Bitwise NOT operator.

        A bitwise NOT, or complement, is a unary operation that performs logical
        negation on each bit, forming the ones’ complement of the given binary
        value. Bits that are 0 become 1, and those that are 1 become 0.
        """
        return BitNot(self, *args)

    def bit_sal(self, *args):
        """
        Bitwise SAL operator.

        In an arithmetic shift (also referred to as signed shift), like a
        logical shift, the bits that slide off the end disappear (except for the
        last, which goes into the carry flag). But in an arithmetic shift, the
        spaces are filled in such a way to preserve the sign of the number being
        slid. For this reason, arithmetic shifts are better suited for signed
        numbers in two’s complement format.

        Note: SHL and SAL are the same, and differentiation only happens because
        SAR and SHR (right shifting) has differences in their implementation.
        """
        return BitSal(self, *args)

    def bit_sar(self, *args):
        """
        Bitwise SAR operator.

        In an arithmetic shift (also referred to as signed shift), like a
        logical shift, the bits that slide off the end disappear (except for the
        last, which goes into the carry flag). But in an arithmetic shift, the
        spaces are filled in such a way to preserve the sign of the number being
        slid. For this reason, arithmetic shifts are better suited for signed
        numbers in two’s complement format.
        """
        return BitSar(self, *args)

    def floor(self, *args):
        """
        Rounds the given value down, returning the largest integer value less
        than or equal to the given value (the value’s floor).
        """
        return Floor(self, *args)

    def ceil(self, *args):
        """
        Rounds the given value up, returning the smallest integer value greater
        than or equal to the given value (the value’s ceiling).
        """
        return Ceil(self, *args)

    def round(self, *args):
        """
        Rounds the given value to the nearest whole integer.
        """
        return Round(self, *args)

    def and_(self, *args):
        """
        Non-operator version of ``__and__``.
        """
        return And(self, *args)

    def or_(self, *args):
        """
        Non-operator version of ``__or__``.
        """
        return Or(self, *args)

    def not_(self, *args):
        """
        Non-operator version of ``__not__``.
        """
        return Not(self, *args)

    # N.B. Cannot use 'in' operator because it must return a boolean
    def contains(self, *args):
        """
        When called with values, returns True if a sequence contains all the
        specified values. When called with predicate functions, returns True if
        for each predicate there exists at least one element of the stream where
        that predicate returns True.
        """
        return Contains(self, *[func_wrap(arg) for arg in args])

    def has_fields(self, *args):
        """
        Test if an object has one or more fields. An object has a field if it
        has that key and the key has a non-null value. For instance, the object
        {'a': 1,'b': 2,'c': null} has the fields a and b.

        When applied to a single object, has_fields returns true if the object
        has the fields and false if it does not. When applied to a sequence, it
        will return a new sequence (an array or stream) containing the elements
        that have the specified fields.
        """
        return HasFields(self, *args)

    def with_fields(self, *args):
        """
        Plucks one or more attributes from a sequence of objects, filtering out
        any objects in the sequence that do not have the specified fields.
        Functionally, this is identical to has_fields followed by pluck on a
        sequence.
        """
        return WithFields(self, *args)

    def keys(self, *args):
        """
        Return an array containing all of an object’s keys. Note that the keys
        will be sorted as described in ReQL data types (for strings,
        lexicographically).
        """
        return Keys(self, *args)

    def values(self, *args):
        """
        Return an array containing all of an object’s values. values()
        guarantees the values will come out in the same order as keys.
        """
        return Values(self, *args)

    def changes(self, *args, **kwargs):
        """
        Turn a query into a changefeed, an infinite stream of objects
        representing changes to the query’s results as they occur. A changefeed
        may return changes to a table or an individual document (a “point”
        changefeed). Commands such as filter or map may be used before the
        changes command to transform or filter the output, and many commands
        that operate on sequences can be chained after changes.
        """
        return Changes(self, *args, **kwargs)

    # Polymorphic object/sequence operations
    def pluck(self, *args):
        """
        Plucks out one or more attributes from either an object or a sequence of
        objects (projection).
        """
        return Pluck(self, *args)

    def without(self, *args):
        """
        The opposite of pluck; takes an object or a sequence of objects, and
        returns them with the specified paths removed.
        """
        return Without(self, *args)

    def do(self, *args):  # pylint: disable=invalid-name
        return FunCall(self, *args)

    def default(self, *args):
        return Default(self, *args)

    def update(self, *args, **kwargs):
        return Update(self, *[func_wrap(arg) for arg in args], **kwargs)

    def replace(self, *args, **kwargs):
        return Replace(self, *[func_wrap(arg) for arg in args], **kwargs)

    def delete(self, *args, **kwargs):
        return Delete(self, *args, **kwargs)

    # Rql type inspection
    def coerce_to(self, *args):
        return CoerceTo(self, *args)

    def ungroup(self, *args):
        return Ungroup(self, *args)

    def type_of(self, *args):
        return TypeOf(self, *args)

    def merge(self, *args):
        return Merge(self, *[func_wrap(arg) for arg in args])

    def append(self, *args):
        return Append(self, *args)

    def prepend(self, *args):
        return Prepend(self, *args)

    def difference(self, *args):
        return Difference(self, *args)

    def set_insert(self, *args):
        return SetInsert(self, *args)

    def set_union(self, *args):
        return SetUnion(self, *args)

    def set_intersection(self, *args):
        return SetIntersection(self, *args)

    def set_difference(self, *args):
        return SetDifference(self, *args)

    # Operator used for get attr / nth / slice. Non-operator versions below
    # in cases of ambiguity
    # TODO
    # Undestand the type of index. Apparently it can be of type slice
    # but of some type accepted by Bracket,
    # which I can't understand where it's defined
    def __getitem__(self, index):
        if not isinstance(index, slice):
            return Bracket(self, index, bracket_operator=True)

        if index.stop:
            return Slice(self, index.start or 0, index.stop, bracket_operator=True)

        return Slice(
            self,
            index.start or 0,
            -1,
            right_bound="closed",
            bracket_operator=True,
        )

    def __iter__(self):
        raise ReqlDriverError(
            "__iter__ called on an RqlQuery object.\n"
            "To iterate over the results of a query, call run first.\n"
            "To iterate inside a query, use map or for_each."
        )

    def get_field(self, *args):
        return GetField(self, *args)

    def nth(self, *args):
        return Nth(self, *args)

    def to_json(self, *args):
        return ToJsonString(self, *args)

    # DEPRECATE: Remove this function in the next release
    def to_json_string(self, *args):
        """
        Function `to_json_string` is an alias for `to_json` and will be removed
        in the future.
        """

        return self.to_json(*args)

    def match(self, *args):
        return Match(self, *args)

    def split(self, *args):
        return Split(self, *args)

    def upcase(self, *args):
        return Upcase(self, *args)

    def downcase(self, *args):
        return Downcase(self, *args)

    def is_empty(self, *args):
        return IsEmpty(self, *args)

    def offsets_of(self, *args):
        return OffsetsOf(self, *[func_wrap(arg) for arg in args])

    def slice(self, *args, **kwargs):
        return Slice(self, *args, **kwargs)

    def skip(self, *args):
        return Skip(self, *args)

    def limit(self, *args):
        return Limit(self, *args)

    def reduce(self, *args):
        return Reduce(self, *[func_wrap(arg) for arg in args])

    def sum(self, *args):
        return Sum(self, *[func_wrap(arg) for arg in args])

    def avg(self, *args):
        return Avg(self, *[func_wrap(arg) for arg in args])

    def min(self, *args, **kwargs):
        return Min(self, *[func_wrap(arg) for arg in args], **kwargs)

    def max(self, *args, **kwargs):
        return Max(self, *[func_wrap(arg) for arg in args], **kwargs)

    def map(self, *args):
        if len(args) > 0:
            # `func_wrap` only the last argument
            return Map(self, *(args[:-1] + (func_wrap(args[-1]),)))

        return Map(self)

    def fold(self, *args, **kwargs):
        if len(args) > 0:
            # `func_wrap` only the last argument before optional arguments
            return Fold(
                self,
                *(args[:-1] + (func_wrap(args[-1]),)),
                **{k: func_wrap(kwargs[k]) for k in kwargs},
            )

        return Fold(self)

    def filter(self, *args, **kwargs):
        return Filter(self, *[func_wrap(arg) for arg in args], **kwargs)

    def concat_map(self, *args):
        return ConcatMap(self, *[func_wrap(arg) for arg in args])

    def order_by(self, *args, **kwargs):
        args = [arg if isinstance(arg, (Asc, Desc)) else func_wrap(arg) for arg in args]
        return OrderBy(self, *args, **kwargs)

    def between(self, *args, **kwargs):
        return Between(self, *args, **kwargs)

    def distinct(self, *args, **kwargs):
        return Distinct(self, *args, **kwargs)

    # Can't overload __len__ because Python doesn't allow us to return a non-integer
    def count(self, *args):
        return Count(self, *[func_wrap(arg) for arg in args])

    def union(self, *args, **kwargs):
        func_kwargs = {key: func_wrap(kwargs[key]) for key in kwargs}
        return Union(self, *args, **func_kwargs)

    def inner_join(self, *args):
        return InnerJoin(self, *args)

    def outer_join(self, *args):
        return OuterJoin(self, *args)

    def eq_join(self, *args, **kwargs):
        return EqJoin(self, *[func_wrap(arg) for arg in args], **kwargs)

    def zip(self, *args):
        return Zip(self, *args)

    def group(self, *args, **kwargs):
        return Group(self, *[func_wrap(arg) for arg in args], **kwargs)

    def branch(self, *args):
        return Branch(self, *args)

    def for_each(self, *args):
        return ForEach(self, *[func_wrap(arg) for arg in args])

    def info(self, *args):
        return Info(self, *args)

    # Array only operations
    def insert_at(self, *args):
        return InsertAt(self, *args)

    def splice_at(self, *args):
        return SpliceAt(self, *args)

    def delete_at(self, *args):
        return DeleteAt(self, *args)

    def change_at(self, *args):
        return ChangeAt(self, *args)

    def sample(self, *args):
        return Sample(self, *args)

    # Time support
    def to_iso8601(self, *args):
        return ToISO8601(self, *args)

    def to_epoch_time(self, *args):
        return ToEpochTime(self, *args)

    def during(self, *args, **kwargs):
        return During(self, *args, **kwargs)

    def date(self, *args):
        return Date(self, *args)

    def time_of_day(self, *args):
        return TimeOfDay(self, *args)

    def timezone(self, *args):
        return Timezone(self, *args)

    def year(self, *args):
        return Year(self, *args)

    def month(self, *args):
        return Month(self, *args)

    def day(self, *args):
        return Day(self, *args)

    def day_of_week(self, *args):
        return DayOfWeek(self, *args)

    def day_of_year(self, *args):
        return DayOfYear(self, *args)

    def hours(self, *args):
        return Hours(self, *args)

    def minutes(self, *args):
        return Minutes(self, *args)

    def seconds(self, *args):
        return Seconds(self, *args)

    def in_timezone(self, *args):
        return InTimezone(self, *args)

    # Geospatial support
    def to_geojson(self, *args):
        return ToGeoJson(self, *args)

    def distance(self, *args, **kwargs):
        return Distance(self, *args, **kwargs)

    def intersects(self, *args):
        return Intersects(self, *args)

    def includes(self, *args):
        return Includes(self, *args)

    def fill(self, *args):
        return Fill(self, *args)

    def polygon_sub(self, *args):
        return PolygonSub(self, *args)


class RqlBoolOperQuery(RqlQuery):
    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)
        self.infix = False

    def set_infix(self):
        self.infix = True

    def compose(self, args, kwargs):  # pylint: disable=unused-argument
        term_args = [
            EnhancedTuple("r.expr(", args[i], ")")
            if needs_wrap(self._args[i])
            else args[i]
            for i in range(len(args))
        ]

        if self.infix:
            infix = EnhancedTuple(*term_args, int_separator=[" ", self.infix, " "])
            return EnhancedTuple("(", infix, ")")

        return EnhancedTuple(
            "r.",
            self.statement,
            "(",
            EnhancedTuple(*term_args, int_separator=", "),
            ")",
        )


class RqlBiOperQuery(RqlQuery):
    """
    RethinkDB binary query operation.
    """

    def compose(self, args, kwargs):  # pylint: disable=unused-argument
        term_args = [
            EnhancedTuple("r.expr(", args[i], ")")
            if needs_wrap(self._args[i])
            else args[i]
            for i in range(len(args))
        ]

        return EnhancedTuple(
            "(",
            EnhancedTuple(*term_args, int_separator=[" ", self.statement, " "]),
            ")",
        )


class RqlBiCompareOperQuery(RqlBiOperQuery):
    """
    RethinkDB comparison operator query.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)

        for arg in args:
            if hasattr(arg, "infix"):
                raise ReqlDriverCompileError(
                    f"""
                    Calling '{self.statement}' on result of infix bitwise operator:
                    {QueryPrinter(self).query}\n
                    This is almost always a precedence error.
                    Note that `a < b | b < c` <==> `a < (b | b) < c`.
                    If you really want this behavior, use `.or_` or `.and_` instead.
                    """
                )


class RqlTopLevelQuery(RqlQuery):
    def compose(self, args, kwargs):
        args.extend([EnhancedTuple(key, "=", value) for key, value in kwargs.items()])
        return EnhancedTuple(
            "r.", self.statement, "(", EnhancedTuple(*(args), int_separator=", "), ")"
        )


class RqlMethodQuery(RqlQuery):
    def compose(self, args, kwargs):
        if len(args) == 0:
            return EnhancedTuple("r.", self.statement, "()")

        if needs_wrap(self._args[0]):
            args[0] = EnhancedTuple("r.expr(", args[0], ")")

        restargs = args[1:]
        restargs.extend([EnhancedTuple(k, "=", v) for k, v in kwargs.items()])
        restargs = EnhancedTuple(*restargs, int_separator=", ")

        return EnhancedTuple(args[0], ".", self.statement, "(", restargs, ")")


class RqlBracketQuery(RqlMethodQuery):
    def __init__(self, *args, **kwargs):
        self.bracket_operator = False

        if "bracket_operator" in kwargs:
            self.bracket_operator = kwargs["bracket_operator"]
            del kwargs["bracket_operator"]

        super().__init__(self, *args, **kwargs)

    def compose(self, args, kwargs):
        if self.bracket_operator:
            if needs_wrap(self._args[0]):
                args[0] = EnhancedTuple("r.expr(", args[0], ")")
            return EnhancedTuple(
                args[0], "[", EnhancedTuple(*args[1:], int_separator=[","]), "]"
            )

        return super().compose(args, kwargs)


class RqlTzinfo(datetime.tzinfo):
    """
    RethinkDB timezone information.
    """

    def __init__(self, offsetstr):
        super().__init__()

        hours, minutes = map(int, offsetstr.split(":"))

        self.offsetstr = offsetstr
        self.delta = datetime.timedelta(hours=hours, minutes=minutes)

    def __getinitargs__(self):
        # Consciously return a tuple
        return (self.offsetstr,)

    def __copy__(self):
        return RqlTzinfo(self.offsetstr)

    def __deepcopy__(self, memo):
        return RqlTzinfo(self.offsetstr)

    def utcoffset(self, dt):
        return self.delta

    def tzname(self, dt):
        return self.offsetstr

    def dst(self, dt):
        return datetime.timedelta(0)


class Datum(RqlQuery):
    """
    RethinkDB datum query.

    This class handles the conversion of RQL terminal types in both directions
    Going to the server though it does not support R_ARRAY or R_OBJECT as those
    are alternately handled by the MakeArray and MakeObject nodes. Why do this?
    MakeArray and MakeObject are more flexible, allowing us to construct array
    and object expressions from nested RQL expressions. Constructing pure
    R_ARRAYs and R_OBJECTs would require verifying that at all nested levels
    our arrays and objects are composed only of basic types.
    """

    def __init__(self, val):
        super().__init__()
        self.data = val

    def build(self):
        return self.data

    def compose(self, args, kwargs):  # pylint: disable=unused-argument
        return repr(self.data)


class MakeArray(RqlQuery):
    """
    RethinkDB array composer query.
    """

    term_type = P_TERM.MAKE_ARRAY

    # pylint: disable=unused-argument,no-self-use
    def compose(self, args, kwargs):
        return EnhancedTuple("[", EnhancedTuple(*args, int_separator=", "), "]")


class MakeObj(RqlQuery):
    term_type = P_TERM.MAKE_OBJ

    def __init__(self, obj_dict):
        super().__init__()

        for key, value in obj_dict.items():
            if not isinstance(key, str):
                raise ReqlDriverCompileError("Object keys must be strings.")

            self.kwargs[key] = expr(value)

    def build(self):
        return self.kwargs

    # pylint: disable=unused-argument,no-self-use
    def compose(self, args, kwargs):
        return EnhancedTuple(
            "r.expr({",
            EnhancedTuple(
                *[
                    EnhancedTuple(repr(key), ": ", value)
                    for key, value in kwargs.items()
                ],
                int_separator=", ",
            ),
            "})",
        )


class Var(RqlQuery):
    term_type = P_TERM.VAR

    # pylint: disable=unused-argument,no-self-use
    def compose(self, args, kwargs):
        return "var_" + args[0]


class JavaScript(RqlTopLevelQuery):
    term_type = P_TERM.JAVASCRIPT
    statement = "js"


class Http(RqlTopLevelQuery):
    term_type = P_TERM.HTTP
    statement = "http"


class UserError(RqlTopLevelQuery):
    term_type = P_TERM.ERROR
    statement = "error"


class Random(RqlTopLevelQuery):
    term_type = P_TERM.RANDOM
    statement = "random"


class Changes(RqlMethodQuery):
    term_type = P_TERM.CHANGES
    statement = "changes"


class Default(RqlMethodQuery):
    term_type = P_TERM.DEFAULT
    statement = "default"


class ImplicitVar(RqlQuery):
    term_type = P_TERM.IMPLICIT_VAR

    def __call__(self, *args, **kwargs):
        raise TypeError("'r.row' is not callable, use 'r.row[...]' instead")

    # pylint: disable=unused-argument,no-self-use
    def compose(self, args, kwargs):
        return "r.row"


class Eq(RqlBiCompareOperQuery):
    term_type = P_TERM.EQ
    statement = "=="


class Ne(RqlBiCompareOperQuery):
    term_type = P_TERM.NE
    statement = "!="


class Lt(RqlBiCompareOperQuery):
    term_type = P_TERM.LT
    statement = "<"


class Le(RqlBiCompareOperQuery):
    term_type = P_TERM.LE
    statement = "<="


class Gt(RqlBiCompareOperQuery):
    term_type = P_TERM.GT
    statement = ">"


class Ge(RqlBiCompareOperQuery):
    term_type = P_TERM.GE
    statement = ">="


class Not(RqlQuery):
    term_type = P_TERM.NOT

    def compose(self, args, kwargs):  # pylint: disable=unused-argument
        if isinstance(self._args[0], Datum):
            args[0] = EnhancedTuple("r.expr(", args[0], ")")

        return EnhancedTuple("(~", args[0], ")")


class Add(RqlBiOperQuery):
    term_type = P_TERM.ADD
    statement = "+"


class Sub(RqlBiOperQuery):
    term_type = P_TERM.SUB
    statement = "-"


class Mul(RqlBiOperQuery):
    term_type = P_TERM.MUL
    statement = "*"


class Div(RqlBiOperQuery):
    term_type = P_TERM.DIV
    statement = "/"


class Mod(RqlBiOperQuery):
    term_type = P_TERM.MOD
    statement = "%"


class BitAnd(RqlBoolOperQuery):
    term_type = P_TERM.BIT_AND
    statement = "bit_and"


class BitOr(RqlBoolOperQuery):
    term_type = P_TERM.BIT_OR
    statement = "bit_or"


class BitXor(RqlBoolOperQuery):
    term_type = P_TERM.BIT_XOR
    statement = "bit_xor"


class BitNot(RqlMethodQuery):
    term_type = P_TERM.BIT_NOT
    statement = "bit_not"


class BitSal(RqlBoolOperQuery):
    term_type = P_TERM.BIT_SAL
    statement = "bit_sal"


class BitSar(RqlBoolOperQuery):
    term_type = P_TERM.BIT_SAR
    statement = "bit_sar"


class Floor(RqlMethodQuery):
    term_type = P_TERM.FLOOR
    statement = "floor"


class Ceil(RqlMethodQuery):
    term_type = P_TERM.CEIL
    statement = "ceil"


class Round(RqlMethodQuery):
    term_type = P_TERM.ROUND
    statement = "round"


class Append(RqlMethodQuery):
    term_type = P_TERM.APPEND
    statement = "append"


class Prepend(RqlMethodQuery):
    term_type = P_TERM.PREPEND
    statement = "prepend"


class Difference(RqlMethodQuery):
    term_type = P_TERM.DIFFERENCE
    statement = "difference"


class SetInsert(RqlMethodQuery):
    term_type = P_TERM.SET_INSERT
    statement = "set_insert"


class SetUnion(RqlMethodQuery):
    term_type = P_TERM.SET_UNION
    statement = "set_union"


class SetIntersection(RqlMethodQuery):
    term_type = P_TERM.SET_INTERSECTION
    statement = "set_intersection"


class SetDifference(RqlMethodQuery):
    term_type = P_TERM.SET_DIFFERENCE
    statement = "set_difference"


class Slice(RqlBracketQuery):
    term_type = P_TERM.SLICE
    statement = "slice"

    # Slice has a special bracket syntax, implemented here
    def compose(self, args, kwargs):
        if self.bracket_operator:
            if needs_wrap(self._args[0]):
                args[0] = EnhancedTuple("r.expr(", args[0], ")")

            return EnhancedTuple(args[0], "[", args[1], ":", args[2], "]")

        return RqlBracketQuery.compose(self, args, kwargs)


class Skip(RqlMethodQuery):
    term_type = P_TERM.SKIP
    statement = "skip"


class Limit(RqlMethodQuery):
    term_type = P_TERM.LIMIT
    statement = "limit"


class GetField(RqlBracketQuery):
    term_type = P_TERM.GET_FIELD
    statement = "get_field"


class Bracket(RqlBracketQuery):
    term_type = P_TERM.BRACKET
    statement = "bracket"


class Contains(RqlMethodQuery):
    term_type = P_TERM.CONTAINS
    statement = "contains"


class HasFields(RqlMethodQuery):
    term_type = P_TERM.HAS_FIELDS
    statement = "has_fields"


class WithFields(RqlMethodQuery):
    term_type = P_TERM.WITH_FIELDS
    statement = "with_fields"


class Keys(RqlMethodQuery):
    term_type = P_TERM.KEYS
    statement = "keys"


class Values(RqlMethodQuery):
    term_type = P_TERM.VALUES
    statement = "values"


class Object(RqlMethodQuery):
    term_type = P_TERM.OBJECT
    statement = "object"


class Pluck(RqlMethodQuery):
    term_type = P_TERM.PLUCK
    statement = "pluck"


class Without(RqlMethodQuery):
    term_type = P_TERM.WITHOUT
    statement = "without"


class Merge(RqlMethodQuery):
    term_type = P_TERM.MERGE
    statement = "merge"


class Between(RqlMethodQuery):
    term_type = P_TERM.BETWEEN
    statement = "between"


class DB(RqlTopLevelQuery):
    term_type = P_TERM.DB
    statement = "db"

    def table_list(self, *args):
        return TableList(self, *args)

    def config(self, *args):
        return Config(self, *args)

    def wait(self, *args, **kwargs):
        return Wait(self, *args, **kwargs)

    def reconfigure(self, *args, **kwargs):
        return Reconfigure(self, *args, **kwargs)

    def rebalance(self, *args, **kwargs):
        return Rebalance(self, *args, **kwargs)

    def grant(self, *args, **kwargs):
        return Grant(self, *args, **kwargs)

    def table_create(self, *args, **kwargs):
        return TableCreate(self, *args, **kwargs)

    def table_drop(self, *args):
        return TableDrop(self, *args)

    def table(self, *args, **kwargs):
        return Table(self, *args, **kwargs)


class FunCall(RqlQuery):
    term_type = P_TERM.FUNCALL

    # This object should be constructed with arguments first, and the
    # function itself as the last parameter.  This makes it easier for
    # the places where this object is constructed.  The actual wire
    # format is function first, arguments last, so we flip them around
    # before passing it down to the base class constructor.
    def __init__(self, *args):
        if len(args) == 0:
            raise ReqlDriverCompileError("Expected 1 or more arguments but found 0.")

        args = [func_wrap(args[-1])] + list(args[:-1])
        super().__init__(self, *args)

    def compose(self, args, kwargs):  # pylint: disable=unused-argument
        if len(args) != 2:
            return EnhancedTuple(
                "r.do(",
                EnhancedTuple(
                    EnhancedTuple(*(args[1:]), int_separator=", "),
                    args[0],
                    int_separator=", ",
                ),
                ")",
            )

        if isinstance(self._args[1], Datum):
            args[1] = EnhancedTuple("r.expr(", args[1], ")")

        return EnhancedTuple(args[1], ".do(", args[0], ")")


class Table(RqlQuery):  # pylint: disable=too-many-public-methods
    term_type = P_TERM.TABLE
    statement = "table"

    def insert(self, *args, **kwargs):
        return Insert(self, *[expr(arg) for arg in args], **kwargs)

    def get(self, *args):
        return Get(self, *args)

    def get_all(self, *args, **kwargs):
        return GetAll(self, *args, **kwargs)

    def set_write_hook(self, *args, **kwargs):
        return SetWriteHook(self, *args, **kwargs)

    def get_write_hook(self, *args, **kwargs):
        return GetWriteHook(self, *args, **kwargs)

    def index_create(self, *args, **kwargs):
        if len(args) > 1:
            args = [args[0]] + [func_wrap(arg) for arg in args[1:]]

        return IndexCreate(self, *args, **kwargs)

    def index_drop(self, *args):
        return IndexDrop(self, *args)

    def index_rename(self, *args, **kwargs):
        return IndexRename(self, *args, **kwargs)

    def index_list(self, *args):
        return IndexList(self, *args)

    def index_status(self, *args):
        return IndexStatus(self, *args)

    def index_wait(self, *args):
        return IndexWait(self, *args)

    def status(self, *args):
        return Status(self, *args)

    def config(self, *args):
        return Config(self, *args)

    def wait(self, *args, **kwargs):
        return Wait(self, *args, **kwargs)

    def reconfigure(self, *args, **kwargs):
        return Reconfigure(self, *args, **kwargs)

    def rebalance(self, *args, **kwargs):
        return Rebalance(self, *args, **kwargs)

    def sync(self, *args):
        return Sync(self, *args)

    def grant(self, *args, **kwargs):
        return Grant(self, *args, **kwargs)

    def get_intersecting(self, *args, **kwargs):
        return GetIntersecting(self, *args, **kwargs)

    def get_nearest(self, *args, **kwargs):
        return GetNearest(self, *args, **kwargs)

    def uuid(self, *args, **kwargs):
        return UUID(self, *args, **kwargs)

    def compose(self, args, kwargs):
        args.extend([EnhancedTuple(k, "=", v) for k, v in kwargs.items()])

        if isinstance(self._args[0], DB):
            return EnhancedTuple(
                args[0], ".table(", EnhancedTuple(*(args[1:]), int_separator=", "), ")"
            )

        return EnhancedTuple(
            "r.table(", EnhancedTuple(*(args), int_separator=", "), ")"
        )


class Get(RqlMethodQuery):
    term_type = P_TERM.GET
    statement = "get"


class GetAll(RqlMethodQuery):
    term_type = P_TERM.GET_ALL
    statement = "get_all"


class GetIntersecting(RqlMethodQuery):
    term_type = P_TERM.GET_INTERSECTING
    statement = "get_intersecting"


class GetNearest(RqlMethodQuery):
    term_type = P_TERM.GET_NEAREST
    statement = "get_nearest"


class UUID(RqlMethodQuery):
    term_type = P_TERM.UUID
    statement = "uuid"


class Reduce(RqlMethodQuery):
    term_type = P_TERM.REDUCE
    statement = "reduce"


class Sum(RqlMethodQuery):
    term_type = P_TERM.SUM
    statement = "sum"


class Avg(RqlMethodQuery):
    term_type = P_TERM.AVG
    statement = "avg"


class Min(RqlMethodQuery):
    term_type = P_TERM.MIN
    statement = "min"


class Max(RqlMethodQuery):
    term_type = P_TERM.MAX
    statement = "max"


class Map(RqlMethodQuery):
    term_type = P_TERM.MAP
    statement = "map"


class Fold(RqlMethodQuery):
    term_type = P_TERM.FOLD
    statement = "fold"


class Filter(RqlMethodQuery):
    term_type = P_TERM.FILTER
    statement = "filter"


class ConcatMap(RqlMethodQuery):
    term_type = P_TERM.CONCAT_MAP
    statement = "concat_map"


class OrderBy(RqlMethodQuery):
    term_type = P_TERM.ORDER_BY
    statement = "order_by"


class Distinct(RqlMethodQuery):
    term_type = P_TERM.DISTINCT
    statement = "distinct"


class Count(RqlMethodQuery):
    term_type = P_TERM.COUNT
    statement = "count"


class Union(RqlMethodQuery):
    term_type = P_TERM.UNION
    statement = "union"


class Nth(RqlBracketQuery):
    term_type = P_TERM.NTH
    statement = "nth"


class Match(RqlMethodQuery):
    term_type = P_TERM.MATCH
    statement = "match"


class ToJsonString(RqlMethodQuery):
    term_type = P_TERM.TO_JSON_STRING
    statement = "to_json_string"


class Split(RqlMethodQuery):
    term_type = P_TERM.SPLIT
    statement = "split"


class Upcase(RqlMethodQuery):
    term_type = P_TERM.UPCASE
    statement = "upcase"


class Downcase(RqlMethodQuery):
    term_type = P_TERM.DOWNCASE
    statement = "downcase"


class OffsetsOf(RqlMethodQuery):
    term_type = P_TERM.OFFSETS_OF
    statement = "offsets_of"


class IsEmpty(RqlMethodQuery):
    term_type = P_TERM.IS_EMPTY
    statement = "is_empty"


class Group(RqlMethodQuery):
    term_type = P_TERM.GROUP
    statement = "group"


class InnerJoin(RqlMethodQuery):
    term_type = P_TERM.INNER_JOIN
    statement = "inner_join"


class OuterJoin(RqlMethodQuery):
    term_type = P_TERM.OUTER_JOIN
    statement = "outer_join"


class EqJoin(RqlMethodQuery):
    term_type = P_TERM.EQ_JOIN
    statement = "eq_join"


class Zip(RqlMethodQuery):
    term_type = P_TERM.ZIP
    statement = "zip"


class CoerceTo(RqlMethodQuery):
    term_type = P_TERM.COERCE_TO
    statement = "coerce_to"


class Ungroup(RqlMethodQuery):
    term_type = P_TERM.UNGROUP
    statement = "ungroup"


class TypeOf(RqlMethodQuery):
    term_type = P_TERM.TYPE_OF
    statement = "type_of"


class Update(RqlMethodQuery):
    term_type = P_TERM.UPDATE
    statement = "update"


class Delete(RqlMethodQuery):
    term_type = P_TERM.DELETE
    statement = "delete"


class Replace(RqlMethodQuery):
    term_type = P_TERM.REPLACE
    statement = "replace"


class Insert(RqlMethodQuery):
    term_type = P_TERM.INSERT
    statement = "insert"


class DbCreate(RqlTopLevelQuery):
    term_type = P_TERM.DB_CREATE
    statement = "db_create"


class DbDrop(RqlTopLevelQuery):
    term_type = P_TERM.DB_DROP
    statement = "db_drop"


class DbList(RqlTopLevelQuery):
    term_type = P_TERM.DB_LIST
    statement = "db_list"


class TableCreate(RqlMethodQuery):
    term_type = P_TERM.TABLE_CREATE
    statement = "table_create"


class TableCreateTL(RqlTopLevelQuery):
    term_type = P_TERM.TABLE_CREATE
    statement = "table_create"


class TableDrop(RqlMethodQuery):
    term_type = P_TERM.TABLE_DROP
    statement = "table_drop"


class TableDropTL(RqlTopLevelQuery):
    term_type = P_TERM.TABLE_DROP
    statement = "table_drop"


class TableList(RqlMethodQuery):
    term_type = P_TERM.TABLE_LIST
    statement = "table_list"


class TableListTL(RqlTopLevelQuery):
    term_type = P_TERM.TABLE_LIST
    statement = "table_list"


class SetWriteHook(RqlMethodQuery):
    term_type = P_TERM.SET_WRITE_HOOK
    statement = "set_write_hook"


class GetWriteHook(RqlMethodQuery):
    term_type = P_TERM.GET_WRITE_HOOK
    statement = "get_write_hook"


class IndexCreate(RqlMethodQuery):
    term_type = P_TERM.INDEX_CREATE
    statement = "index_create"


class IndexDrop(RqlMethodQuery):
    term_type = P_TERM.INDEX_DROP
    statement = "index_drop"


class IndexRename(RqlMethodQuery):
    term_type = P_TERM.INDEX_RENAME
    statement = "index_rename"


class IndexList(RqlMethodQuery):
    term_type = P_TERM.INDEX_LIST
    statement = "index_list"


class IndexStatus(RqlMethodQuery):
    term_type = P_TERM.INDEX_STATUS
    statement = "index_status"


class IndexWait(RqlMethodQuery):
    term_type = P_TERM.INDEX_WAIT
    statement = "index_wait"


class Config(RqlMethodQuery):
    term_type = P_TERM.CONFIG
    statement = "config"


class Status(RqlMethodQuery):
    term_type = P_TERM.STATUS
    statement = "status"


class Wait(RqlMethodQuery):
    term_type = P_TERM.WAIT
    statement = "wait"


class Reconfigure(RqlMethodQuery):
    term_type = P_TERM.RECONFIGURE
    statement = "reconfigure"


class Rebalance(RqlMethodQuery):
    term_type = P_TERM.REBALANCE
    statement = "rebalance"


class Sync(RqlMethodQuery):
    term_type = P_TERM.SYNC
    statement = "sync"


class Grant(RqlMethodQuery):
    term_type = P_TERM.GRANT
    statement = "grant"


class GrantTL(RqlTopLevelQuery):
    term_type = P_TERM.GRANT
    statement = "grant"


class Branch(RqlTopLevelQuery):
    term_type = P_TERM.BRANCH
    statement = "branch"


class Or(RqlBoolOperQuery):
    term_type = P_TERM.OR
    statement = "or_"
    st_infix = "|"


class And(RqlBoolOperQuery):
    term_type = P_TERM.AND
    statement = "and_"
    st_infix = "&"


class ForEach(RqlMethodQuery):
    term_type = P_TERM.FOR_EACH
    statement = "for_each"


class Info(RqlMethodQuery):
    term_type = P_TERM.INFO
    statement = "info"


class InsertAt(RqlMethodQuery):
    term_type = P_TERM.INSERT_AT
    statement = "insert_at"


class SpliceAt(RqlMethodQuery):
    term_type = P_TERM.SPLICE_AT
    statement = "splice_at"


class DeleteAt(RqlMethodQuery):
    term_type = P_TERM.DELETE_AT
    statement = "delete_at"


class ChangeAt(RqlMethodQuery):
    term_type = P_TERM.CHANGE_AT
    statement = "change_at"


class Sample(RqlMethodQuery):
    term_type = P_TERM.SAMPLE
    statement = "sample"


class Json(RqlTopLevelQuery):
    term_type = P_TERM.JSON
    statement = "json"


class Args(RqlTopLevelQuery):
    term_type = P_TERM.ARGS
    statement = "args"


# Use this class as a wrapper to 'bytes' so we can tell the difference
# in Python2 (when reusing the result of a previous query).
class RqlBinary(bytes):
    def __new__(cls, *args, **kwargs):
        return bytes.__new__(cls, *args, **kwargs)

    def __repr__(self):
        ellipsis = "..." if len(self) > 6 else ""
        excerpt = binascii.hexlify(self[0:6]).decode("utf-8")
        excerpt = " ".join([excerpt[i : i + 2] for i in range(0, len(excerpt), 2)])
        excerpt = f", '{excerpt}{ellipsis}'" if len(self) > 0 else ""

        plural = "s" if len(self) != 1 else ""
        return f"<binary, {str(len(self))} byte{plural}{excerpt}>"


class Binary(RqlTopLevelQuery):
    # Note: this term isn't actually serialized, it should exist only
    # in the client
    term_type = P_TERM.BINARY
    statement = "binary"

    def __init__(self, data):
        # We only allow 'bytes' objects to be serialized as binary
        # Python 2 - `bytes` is equivalent to `str`, either will be accepted
        # Python 3 - `unicode` is equivalent to `str`, neither will be accepted
        if isinstance(data, RqlQuery):
            RqlTopLevelQuery.__init__(self, data)
        elif isinstance(data, str):
            raise ReqlDriverCompileError(
                "Cannot convert a unicode string to binary, "
                "use `unicode.encode()` to specify the "
                "encoding."
            )
        elif not isinstance(data, bytes):
            raise ReqlDriverCompileError(
                (
                    "Cannot convert %s to binary, convert the "
                    "object to a `bytes` object first."
                )
                % type(data).__name__
            )

        self.base64_data = base64.b64encode(data)

        # Kind of a hack to get around composing
        self._args = []
        self.kwargs = {}

    def compose(self, args, kwargs):
        if len(self._args) == 0:
            return EnhancedTuple("r.", self.statement, "(bytes(<data>))")

        return RqlTopLevelQuery.compose(self, args, kwargs)

    def build(self):
        if len(self._args) == 0:
            return {"$reql_type$": "BINARY", "data": self.base64_data.decode("utf-8")}

        return RqlTopLevelQuery.build(self)


class Range(RqlTopLevelQuery):
    term_type = P_TERM.RANGE
    statement = "range"


class ToISO8601(RqlMethodQuery):
    term_type = P_TERM.TO_ISO8601
    statement = "to_iso8601"


class During(RqlMethodQuery):
    term_type = P_TERM.DURING
    statement = "during"


class Date(RqlMethodQuery):
    term_type = P_TERM.DATE
    statement = "date"


class TimeOfDay(RqlMethodQuery):
    term_type = P_TERM.TIME_OF_DAY
    statement = "time_of_day"


class Timezone(RqlMethodQuery):
    term_type = P_TERM.TIMEZONE
    statement = "timezone"


class Year(RqlMethodQuery):
    term_type = P_TERM.YEAR
    statement = "year"


class Month(RqlMethodQuery):
    term_type = P_TERM.MONTH
    statement = "month"


class Day(RqlMethodQuery):
    term_type = P_TERM.DAY
    statement = "day"


class DayOfWeek(RqlMethodQuery):
    term_type = P_TERM.DAY_OF_WEEK
    statement = "day_of_week"


class DayOfYear(RqlMethodQuery):
    term_type = P_TERM.DAY_OF_YEAR
    statement = "day_of_year"


class Hours(RqlMethodQuery):
    term_type = P_TERM.HOURS
    statement = "hours"


class Minutes(RqlMethodQuery):
    term_type = P_TERM.MINUTES
    statement = "minutes"


class Seconds(RqlMethodQuery):
    term_type = P_TERM.SECONDS
    statement = "seconds"


class Time(RqlTopLevelQuery):
    term_type = P_TERM.TIME
    statement = "time"


class ISO8601(RqlTopLevelQuery):
    term_type = P_TERM.ISO8601
    statement = "iso8601"


class EpochTime(RqlTopLevelQuery):
    term_type = P_TERM.EPOCH_TIME
    statement = "epoch_time"


class Now(RqlTopLevelQuery):
    term_type = P_TERM.NOW
    statement = "now"


class InTimezone(RqlMethodQuery):
    term_type = P_TERM.IN_TIMEZONE
    statement = "in_timezone"


class ToEpochTime(RqlMethodQuery):
    term_type = P_TERM.TO_EPOCH_TIME
    statement = "to_epoch_time"


class GeoJson(RqlTopLevelQuery):
    term_type = P_TERM.GEOJSON
    statement = "geojson"


class ToGeoJson(RqlMethodQuery):
    term_type = P_TERM.TO_GEOJSON
    statement = "to_geojson"


class Point(RqlTopLevelQuery):
    term_type = P_TERM.POINT
    statement = "point"


class Line(RqlTopLevelQuery):
    term_type = P_TERM.LINE
    statement = "line"


class Polygon(RqlTopLevelQuery):
    term_type = P_TERM.POLYGON
    statement = "polygon"


class Distance(RqlMethodQuery):
    term_type = P_TERM.DISTANCE
    statement = "distance"


class Intersects(RqlMethodQuery):
    term_type = P_TERM.INTERSECTS
    statement = "intersects"


class Includes(RqlMethodQuery):
    term_type = P_TERM.INCLUDES
    statement = "includes"


class Circle(RqlTopLevelQuery):
    term_type = P_TERM.CIRCLE
    statement = "circle"


class Fill(RqlMethodQuery):
    term_type = P_TERM.FILL
    statement = "fill"


class PolygonSub(RqlMethodQuery):
    term_type = P_TERM.POLYGON_SUB
    statement = "polygon_sub"


class Func(RqlQuery):
    term_type = P_TERM.FUNC
    lock = threading.Lock()
    nextVarId = 1

    def __init__(self, lmbd):
        super().__init__()
        vrs = []
        vrids = []

        try:
            code = lmbd.func_code
        except AttributeError:
            code = lmbd.__code__

        for _ in range(code.co_argcount):
            Func.lock.acquire()

            var_id = Func.nextVarId

            Func.nextVarId += 1
            Func.lock.release()

            vrs.append(Var(var_id))
            vrids.append(var_id)

        self.vrs = vrs
        self._args.extend([MakeArray(*vrids), expr(lmbd(*vrs))])

    def compose(self, args, kwargs):  # pylint: disable=unused-argument
        return EnhancedTuple(
            "lambda ",
            EnhancedTuple(
                *[
                    v.compose(
                        # pylint: disable=protected-access
                        [v._args[0].compose(None, None)],
                        [],
                    )
                    for v in self.vrs
                ],
                int_separator=", ",
            ),
            ": ",
            args[1],
        )


class Asc(RqlTopLevelQuery):
    term_type = P_TERM.ASC
    statement = "asc"


class Desc(RqlTopLevelQuery):
    term_type = P_TERM.DESC
    statement = "desc"


class Literal(RqlTopLevelQuery):
    term_type = P_TERM.LITERAL
    statement = "literal"


# Returns True if IMPLICIT_VAR is found in the subquery
def _ivar_scan(query) -> bool:
    if not isinstance(query, RqlQuery):
        return False

    if isinstance(query, ImplicitVar):
        return True

    if any(
        # pylint: disable=protected-access
        [_ivar_scan(arg) for arg in query._args]
    ):
        return True

    if any([_ivar_scan(arg) for k, arg in query.kwargs.items()]):
        return True

    return False


def needs_wrap(arg):
    """
    These classes define how nodes are printed by overloading `compose`.
    """

    return isinstance(arg, (Datum, MakeArray, MakeObj))


# pylint: disable=too-many-return-statements
def expr(
    val: TUnion[
        str,
        bytes,
        RqlQuery,
        RqlBinary,
        datetime.date,
        datetime.datetime,
        Mapping,
        Iterable,
        Callable,
    ],
    nesting_depth: int = 20,
):
    """
    Convert a Python primitive into a RQL primitive value.
    """

    if not isinstance(nesting_depth, int):
        raise ReqlDriverCompileError("Second argument to `r.expr` must be a number.")

    if nesting_depth <= 0:
        raise ReqlDriverCompileError("Nesting depth limit exceeded.")

    if isinstance(val, RqlQuery):
        return val

    if callable(val):
        return Func(val)

    if isinstance(val, str):  # TODO: Default is to return Datum - Remove?
        return Datum(val)

    if isinstance(val, (bytes, RqlBinary)):
        return Binary(val)

    if isinstance(val, abc.Mapping):
        return MakeObj({k: expr(v, nesting_depth - 1) for k, v in val.items()})

    if isinstance(val, abc.Iterable):
        return MakeArray(*[expr(v, nesting_depth - 1) for v in val])  # type: ignore

    if isinstance(val, (datetime.datetime, datetime.date)):
        if isinstance(val, datetime.date) or not val.tzinfo:
            raise ReqlDriverCompileError(
                f"""
            Cannot convert {type(val).__name__} to ReQL time object
            without timezone information. You can add timezone information with
            the third party module \"pytz\" or by constructing ReQL compatible
            timezone values with r.make_timezone(\"[+-]HH:MM\"). Alternatively,
            use one of ReQL's bultin time constructors, r.now, r.time,
            or r.iso8601.
            """
            )

        return ISO8601(val.isoformat())

    return Datum(val)


# Called on arguments that should be functions
# TODO
# expr may return different value types. Maybe use a base one?
def func_wrap(val: TUnion[RqlQuery, ImplicitVar, list, dict]):
    val = expr(val)
    if _ivar_scan(val):
        return Func(lambda x: val)

    return val
