import pytest

from rethinkdb.ast import RqlQuery
from rethinkdb.encoder import ReQLDecoder, ReQLEncoder


class UnknownObj:
    """"""


def test_encode():
    """
    Test encoding of standard Python objects.
    """

    obj = {"strkey": "value", "intkey": 1, "dictkey": {"strkey": "value"}}

    encoder = ReQLEncoder()
    result = encoder.encode(obj)

    assert result == '{"strkey":"value","intkey":1,"dictkey":{"strkey":"value"}}'


def test_encode_rql_query():
    """
    Test encoding RqlQuery.
    """

    query = RqlQuery(1, 2, optargs={"key": "val"})

    encoder = ReQLEncoder()
    result = encoder.encode(query)

    assert result == '[null,[1,2],{"optargs":{"key":"val"}}]'


def test_encode_unknown_object():
    """
    Test encoding objects which are unknown by the encoder.
    """

    encoder = ReQLEncoder()

    with pytest.raises(TypeError):
        encoder.encode(UnknownObj())


def test_decode():
    """
    Test decoding strings to standard Python objects.
    """

    string = '{"strkey":"value","intkey":1,"dictkey":{"strkey":"value"}}'

    decoder = ReQLDecoder()
    result = decoder.decode(string)

    assert result == {"strkey": "value", "intkey": 1, "dictkey": {"strkey": "value"}}


def test_decode_rql_query():
    """
    Test decoding RqlQuery.
    """

    query = '[null,[1,2],{"optargs":{"key":"val"}}]'

    decoder = ReQLDecoder()
    result = decoder.decode(query)

    assert result == RqlQuery(1, 2, optargs={"key": "val"})


def test_decode_unknown_object():
    """
    Test encoding objects which are unknown by the encoder.
    """

    decoder = ReQLEncoder()

    with pytest.raises(TypeError):
        decoder.encode(UnknownObj())
