CHANGELOG
=========

All notable changes to this project will be documented in this file.
The format is based on `Keep a Changelog`_, and this project adheres to
`Semantic Versioning`_.

.. _Keep a Changelog: https://keepachangelog.com/en/1.0.0/
.. _Semantic Versioning: https://semver.org/spec/v2.0.0.html

.. Hyperlinks for releases

.. _Unreleased: https://github.com/rethinkdb/rethinkdb-python/compare/master...master
.. .. _2.5.0: https://github.com/rethinkdb/rethinkdb-python/releases/tag/v2.5.0

Unreleased_
-----------

Added
~~~~~

* `ValueError` raised by `ReqlTimeoutError` and `ReqlAuthError` if only host or port set

Changed
~~~~~~~

* QueryPrinter's `print_query` became a property and renamed to `query`
* QueryPrinter's `print_carrots` became a property and renamed to `carrots`
* Renamed `ReqlAvailabilityError` to `ReqlOperationError`
* Extract REPL helper class to a separate file

Removed
~~~~~~~

* Removed `Rql*` aliases for `Reql*` exceptions

.. EXAMPLE CHANGELOG ENTRY

    0.1.0_ - 2020-01-xx
    --------------------

    Added
    ~~~~~

    * TODO.

    Changed
    ~~~~~~~

    * TODO.

    Fixed
    ~~~~~

    * TODO.

    Removed
    ~~~~~~~

    * TODO
