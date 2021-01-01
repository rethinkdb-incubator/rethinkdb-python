"""
Utility functions to make it easier to work with other parts of the code.
"""

from typing import Union


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
