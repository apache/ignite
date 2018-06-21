=================
Examples of usage
=================

Open connection
---------------

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :lines: 16-23

Create cache
------------

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :lines: 25-28

Put value in cache
------------------

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :lines: 30-31

Get value from cache
--------------------

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :lines: 33-37

List keys in cache
------------------

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :lines: 39-40

Type hints usage
----------------

.. literalinclude:: ../examples/type_hints.py
  :language: python
  :lines: 30-49

Scan queries
------------

Scan queries allows you to browse cache contents with pagination.

.. literalinclude:: ../examples/scans.py
  :language: python
  :lines: 30-31, 45-58

Subsequent scans could be made using cursor ID.

.. literalinclude:: ../examples/scans.py
  :language: python
  :lines: 60-73

When cursor have no more data, it automatically destroys.

.. literalinclude:: ../examples/scans.py
  :language: python
  :lines: 75-77

If your cursor still holds some data, but you have no use of it anymore,
you may destroy it manually.

.. literalinclude:: ../examples/scans.py
  :language: python
  :lines: 79

Inspect cache configuration
---------------------------

.. literalinclude:: ../examples/cache_config.py
  :language: python
  :lines: 30-63

Create cache with a certain configuration
-----------------------------------------
You must supply at least cache name.

.. literalinclude:: ../examples/cache_config.py
  :language: python
  :lines: 66-78

Do cleanup
----------

Destroy created cache and close connection.

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :lines: 42-43
