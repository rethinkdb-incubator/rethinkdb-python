import threading

import pytest

from rethinkdb.ast import Repl, dict_items, needs_wrap


@pytest.fixture
def repl():
    """
    Fixture returning a Repl object instance for testing purpose.
    """
    return Repl()


def test_repl_get(repl):

    res = repl.get()
    assert res == None
