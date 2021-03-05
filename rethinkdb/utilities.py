"""
Utility functions to make it easier to work with other parts of the code.
"""

from typing import Union

__all__ = ["EnhancedTuple", "chain_to_bytes"]


class EnhancedTuple:  # pylint: disable=too-few-public-methods
    """
    This 'enhanced' tuple recursively iterates over it's elements allowing us to
    construct nested hierarchies that insert subsequences into tree. It's used
    to construct the query representation used by the pretty printer.
    """

    # N.B Python 2.x doesn't allow keyword default arguments after *seq
    #     In Python 3.x we can rewrite this as `__init__(self, *seq, intsp=''`
    def __init__(self, *seq, **opts):
        self.seq = seq
        self.intsp = opts.pop("intsp", "")

    def __iter__(self):
        itr = iter(self.seq)

        try:
            for sub in next(itr):
                yield sub
        except StopIteration:
            return

        for token in itr:
            for sub in self.intsp:
                yield sub

            for sub in token:
                yield sub


def chain_to_bytes(*strings: Union[bytes, str]) -> bytes:
    """
    Ensure the bytes and/or strings are chained as bytes.
    """

    return b"".join(
        [
            string.encode("latin-1") if isinstance(string, str) else string
            for string in strings
        ]
    )
