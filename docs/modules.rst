rethinkdb
=========

.. toctree::
   :maxdepth: 4

   rethinkdb
   handshake.py deals with performing the initial handshake with the database. Classes respresenting the connection are declared there.
   ast.py represents at a database level the possible queries.
   query.py is the interface between the ast.py module and the users using the library
   repl.py is the base for establishing a connection with the database.
   errors.py custom exceptions that may be thrown while using the library.
   
