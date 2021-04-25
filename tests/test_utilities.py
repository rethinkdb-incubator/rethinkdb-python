from rethinkdb.utilities import EnhancedTuple, chain_to_bytes


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


def test_enhanced_tuple_simple_iteration():
    """
    Test EnhancedTuple iterates on array.
    """

    expected_sequence = [1, 2, 3]
    enhanced_tuple = EnhancedTuple(expected_sequence)

    assert list(enhanced_tuple) == expected_sequence


def test_enhanced_tuple_simple_query():
    """
    Test EnhancedTuple iterates on array.
    """

    expected_sequence = ["r", ".", "e", "x", "p", "r", "(", 1, 2, 3, ")"]
    enhanced_tuple = EnhancedTuple("r.expr(", [1, 2, 3], ")")

    assert list(enhanced_tuple) == expected_sequence


def test_enhanced_tuple_recursive_iteration():
    """
    Test EnhancedTuple iterates recursively.
    """

    expected_sequence = [
        "r",
        ".",
        "e",
        "x",
        "p",
        "r",
        "(",
        "r",
        ".",
        "e",
        "x",
        "p",
        "r",
        "(",
        1,
        2,
        3,
        ")",
        ")",
    ]

    enhanced_tuple = EnhancedTuple(
        "r.expr(", EnhancedTuple("r.expr(", [1, 2, 3], ")"), ")"
    )

    assert list(enhanced_tuple) == expected_sequence
