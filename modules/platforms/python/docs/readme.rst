=================
Basic Information
=================

What it is?
-----------

This is an Apache Ignite lightweight (binary protocol) client library,
written in Python.

`Apache Ignite`_ is a memory-centric distributed database, caching,
and processing platform for transactional, analytical, and streaming
workloads delivering in-memory speeds at petabyte scale.

Ignite `binary client protocol`_ provides user applications the ability
to communicate with an existing Ignite cluster without starting
a full-fledged Ignite node. An application can connect to the cluster
through a raw TCP socket.

Prerequisites
-------------

- *Python 3.4+*
- attrs_
- *Sphinx* for documentation
- *pytest* for tests

Licensing
---------

This is a free software, brought to you on terms of the `Apache License v2`_.


Installation
------------

While this is not an official PyPi package, you can just `git clone` it
from the repository and use it anywhere.

Testing
-------

Create and activate virtualenv_ environment. Run

`$ pip install -r requirements.txt`

Then just run

`$ pytest`

*NB!* Some tests require Apache Ignite node running on localhost:10800.
To override the default parameters, use command line options
`--ignite-host` and `--ignite-port`:

`$ pytest --ignite-host=example.com --ignite-port=19840`

You can use each of these options multiple times. All combinations
of given host and port will be tested.

Documentation
-------------
To recompile this documentation, do

::

$ cd ./docs
$ make html

Then open `docs/_build/html/index.html` in your browser.

.. _Apache Ignite: https://apacheignite.readme.io/docs/what-is-ignite
.. _binary client protocol: https://apacheignite.readme.io/docs/binary-client-protocol
.. _Apache License v2: http://www.apache.org/licenses/LICENSE-2.0
.. _attrs: http://www.attrs.org/
.. _virtualenv: https://virtualenv.pypa.io/
