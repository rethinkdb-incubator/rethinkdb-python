import threading

import pytest

from rethinkdb.ast import dict_items, needs_wrap, Repl


@pytest.fixture
def repl():
    """
    Fixture returning a Repl object instance for testing purpose.
    """
    return Repl()


def test_repl_get(repl):

    res = repl.get()
    assert res == None
