=================
Basic Information
=================

What it is
----------

This is an Apache Ignite lightweight (binary protocol) client library,
written in Python, abbreviated as *pyignite*.

`Apache Ignite`_ is a memory-centric distributed database, caching,
and processing platform for transactional, analytical, and streaming
workloads delivering in-memory speeds at petabyte scale.

Ignite `binary client protocol`_ provides user applications the ability
to communicate with an existing Ignite cluster without starting
a full-fledged Ignite node. An application can connect to the cluster
through a raw TCP socket.

Prerequisites
-------------

- *Python 3.4* or above (3.6 is tested),
- Access to *Apache Ignite 2.5* node, local or remote. Higher versions
  of Ignite may or may not work.


Installation
------------

for end user
""""""""""""

If you want to use *pyignite* in your project, you may install it from PyPI:

::

$ pip install pyignite

for developer
"""""""""""""

If you want to run tests, examples or build documentation, clone
the whole repository:

::

$ git clone git@github.com:nobitlost/ignite.git
$ git checkout ignite-7782
$ cd ignite/modules/platforms/python

Then run through the contents of `requirements` folder to install
the necessary prerequisites into your working Python environment using

::

$ pip install -r requirements/install.txt

You may also want to consult the `setuptools`_ manual about using `setup.py`.

Examples
--------

Some examples of using pyignite are provided in
`ignite/modules/platforms/python/examples` folder.

This code implies that it is run in the environment with `pyignite` package
installed. If you want to play with the repository clone of `pyignite`, run

::

$ cd ignite/modules/platforms/python
$ pip install -e .

to install the package code in your environment without actually copying it
to `site-packages` folder.

Testing
-------

Create and activate virtualenv_ environment. Run

::

$ cd ignite/modules/platforms/python
$ python ./setup.py pytest

This does not require `pytest` and other test dependencies to be installed
in your environment.

*NB!* Some or all tests require Apache Ignite node running on localhost:10800.
To override the default parameters, use command line options
`--ignite-host` and `--ignite-port`:

::

$ python ./setup.py pytest --addopts "--ignite-host=example.com --ignite-port=19840"

You can use each of these options multiple times. All combinations
of given host and port will be tested.

Documentation
-------------
To recompile this documentation, do this from your virtualenv_ environment:

::

$ cd ignite/modules/platforms/python
$ pip install -r requirements/docs.txt
$ cd docs
$ make html

Then open `ignite/modules/platforms/python/docs/generated/html/index.html`
in your browser.

If you feel that old version is stuck, do

::

$ cd ignite/modules/platforms/python/docs
$ make clean
$ sphinx-apidoc -M -o source/ ../pyignite
$ make html

And that should be it.

Licensing
---------

This is a free software, brought to you on terms of the `Apache License v2`_.

.. _Apache Ignite: https://apacheignite.readme.io/docs/what-is-ignite
.. _binary client protocol: https://apacheignite.readme.io/docs/binary-client-protocol
.. _Apache License v2: http://www.apache.org/licenses/LICENSE-2.0
.. _virtualenv: https://virtualenv.pypa.io/
.. _setuptools: https://setuptools.readthedocs.io/
