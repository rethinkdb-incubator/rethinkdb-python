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
TODO: What encoder module is
"""

import base64
from datetime import datetime
import json
from typing import Callable

from rethinkdb.ast import RqlBinary, RqlQuery, RqlTzinfo
from rethinkdb.errors import ReqlDriverError

__all__ = ["ReQLEncoder", "ReQLDecoder"]


class ReQLEncoder(json.JSONEncoder):
    """
    Default JSONEncoder subclass to handle query conversion.
    """

    def __init__(
        self,
        *,
        skipkeys=False,
        ensure_ascii=False,
        check_circular=False,
        allow_nan=False,
        sort_keys=False,
        indent=None,
        separators=(",", ":"),
        default=None,
    ):
        super().__init__(
            skipkeys=skipkeys,
            ensure_ascii=ensure_ascii,
            check_circular=check_circular,
            allow_nan=allow_nan,
            sort_keys=sort_keys,
            indent=indent,
            separators=separators,
            default=default,
        )

    def default(self, o):
        if isinstance(o, RqlQuery):
            return o.build()
        return super().default(self, o)


class ReQLDecoder(json.JSONDecoder):
    """
    Default JSONDecoder subclass to handle pseudo-type conversion.
    """

    def __init__(
        self,
        object_hook=None,
        parse_float=None,
        parse_int=None,
        parse_constant=None,
        strict=True,
        object_pairs_hook=None,
        reql_format_opts=None,
    ):
        custom_object_hook = object_hook or self.convert_pseudo_type

        super().__init__(
            object_hook=custom_object_hook,
            parse_float=parse_float,
            parse_int=parse_int,
            parse_constant=parse_constant,
            strict=strict,
            object_pairs_hook=object_pairs_hook,
        )

        self.reql_format_opts = reql_format_opts or {}

    @staticmethod
    def convert_time(obj):
        if "epoch_time" not in obj:
            raise ReqlDriverError(
                f"pseudo-type TIME object {json.dumps(obj)} does not "
                'have expected field "epoch_time".'
            )

        if "timezone" in obj:
            return datetime.fromtimestamp(obj["epoch_time"], RqlTzinfo(obj["timezone"]))

        return datetime.utcfromtimestamp(obj["epoch_time"])

    @staticmethod
    def convert_grouped_data(obj):
        if "data" not in obj:
            raise ReqlDriverError(
                f"pseudo-type GROUPED_DATA object {json.dumps(obj)} does not"
                'have the expected field "data".'
            )

        return dict([(recursively_make_hashable(k), v) for k, v in obj["data"]])

    @staticmethod
    def convert_binary(obj):
        if "data" not in obj:
            raise ReqlDriverError(
                f"pseudo-type BINARY object {json.dumps(obj)} does not have "
                'the expected field "data".'
            )

        return RqlBinary(base64.b64decode(obj["data"].encode("utf-8")))

    def _convert_pseudo_type(self, obj: dict, format_name: str, converter: Callable):
        """
        Convert pseudo types.
        """

        pseudo_type_format = self.reql_format_opts.get(format_name)

        if pseudo_type_format is None or pseudo_type_format == "native":
            return converter(obj)
        elif pseudo_type_format != "raw":
            raise ReqlDriverError(
                f'Unknown {format_name} run option "{pseudo_type_format}".'
            )

    def convert_pseudo_type(self, obj: dict):
        reql_type = obj.get("$reql_type$")

        if reql_type is None:
            # If there was no pseudo_type, or the relevant format is raw, return
            # the original object
            return obj

        if reql_type == "TIME":
            self._convert_pseudo_type(obj, "time_format", self.convert_time)
        elif reql_type == "GROUPED_DATA":
            self._convert_pseudo_type(obj, "group_format", self.convert_grouped_data)
        elif reql_type == "BINARY":
            self._convert_pseudo_type(obj, "binary_format", self.convert_binary)
        elif reql_type == "GEOMETRY":
            # No special support for this, just return the raw object
            return obj

        raise ReqlDriverError(f'Unknown pseudo-type "{reql_type}"')


def recursively_make_hashable(obj):
    """
    Python only allows immutable built-in types to be hashed, such as for keys in
    a dict. This means we can't use lists or dicts as keys in grouped data objects,
    so we convert them to tuples and frozen sets, respectively. This may make it a
    little harder for users to work with converted grouped data, unless they do a
    simple iteration over the result.
    """

    if isinstance(obj, list):
        return tuple([recursively_make_hashable(i) for i in obj])

    if isinstance(obj, dict):
        return frozenset([(k, recursively_make_hashable(v)) for k, v in obj.items()])

    return obj
