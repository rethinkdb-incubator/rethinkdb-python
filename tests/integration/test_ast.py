import pytest

from rethinkdb.ast import Repl


@pytest.fixture
def repl():
    """
    Fixture returning a Repl object instance for testing purpose.
    """

    return Repl()


def test_repl_get(repl: Repl):  # pylint: disable=redefined-outer-name

    res = repl.get_connection()
    assert res == None
