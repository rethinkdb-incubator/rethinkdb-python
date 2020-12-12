from rethinkdb.utilities import chain_to_bytes


def test_string_chaining():
    """
    Test strings can be chained into bytes.
    """

    expected_string = b"iron man"
    result = chain_to_bytes("iron", " ", "man")
    assert result == expected_string


def test_byte_chaining():
    """
    Test multiple bytes can be chained into one byte string.
    """

    expected_string = b"iron man"
    result = chain_to_bytes(b"iron", b" ", b"man")
    assert result == expected_string


def test_mixed_chaining():
    """
    Test both strings and bytes can be chained together.
    """

    expected_string = b"iron man"
    result = chain_to_bytes("iron", " ", b"man")
    assert result == expected_string
