..  Copyright 2019 GridGain Systems, Inc. and Contributors.

..  Licensed under the GridGain Community Edition License (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

..      https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license

..  Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

=================
Basic Information
=================

What it is
----------

This is a GridGain Community Edition thin (binary protocol) client library,
written in Python 3, abbreviated as *pygridgain*.

`GridGain`_ is a memory-centric distributed database, caching,
and processing platform for transactional, analytical, and streaming
workloads delivering in-memory speeds at petabyte scale.

GridGain `binary client protocol`_ provides user applications the ability
to communicate with an existing GridGain cluster without starting
a full-fledged GridGain node. An application can connect to the cluster
through a raw TCP socket.

Prerequisites
-------------

- *Python 3.4* or above (3.6 is tested),
- Access to *GridGain* node, local or remote. The current thin client
  version was tested on *GridGain CE 8.7* (binary client protocols 1.2.0
  to 1.4.0).

Installation
------------

for end user
""""""""""""

If you want to use *pygridgain* in your project, you may install it from PyPI:

::

$ pip install pygridgain

for developer
"""""""""""""

If you want to run tests, examples or build documentation, clone
the whole repository:

::

$ git clone git@github.com:gridgain/gridgain.git
$ cd gridgain/modules/platforms/python
$ pip install -e .

This will install the repository version of `pygridgain` into your environment
in so-called “develop” or “editable” mode. You may read more about
`editable installs`_ in the `pip` manual.

Then run through the contents of `requirements` folder to install
the the additional requirements into your working Python environment using

::

$ pip install -r requirements/<your task>.txt

You may also want to consult the `setuptools`_ manual about using `setup.py`.

Examples
--------

Some examples of using pygridgain are provided in
`gridgain/modules/platforms/python/examples` folder. They are extensively
commented in the :ref:`examples_of_usage` section of the documentation.

This code implies that it is run in the environment with `pygridgain` package
installed, and GridGain node is running on localhost:10800, unless
otherwise noted.

There is also a possibility to run examples alone with tests. For
the explanation of testing, look up the `Testing`_ section.

Testing
-------

Create and activate virtualenv_ environment. Run

::

$ cd gridgain/modules/platforms/python
$ python ./setup.py pytest

This does not require `pytest` and other test dependencies to be installed
in your environment.

Some or all tests require GridGain node running on localhost:10800.
To override the default parameters, use command line option ``--node``:

::

$ python ./setup.py pytest --addopts "--node=example.com:19840"

You can use each of these two options multiple times to connect to multiple
GridGain nodes.

You can also test client against a server with SSL-encrypted connection.
SSL-related `pytest` parameters are:

``--use-ssl`` − use SSL encryption,

``--ssl-certfile`` − a path to ssl certificate file to identify local party,

``--ssl-ca-certfile`` − a path to a trusted certificate or a certificate chain,

``--ssl-cert-reqs`` − determines how the remote side certificate is treated:

- ``NONE`` (ignore, default),
- ``OPTIONAL`` (validate, if provided),
- ``REQUIRED`` (valid remote certificate is required),

``--ssl-ciphers`` − ciphers to use,

``--ssl-version`` − SSL version:

- ``TLSV1_1`` (default),
- ``TLSV1_2``.

Other `pytest` parameters:

``--timeout`` − timeout (in seconds) for each socket operation, including
`connect`. Accepts integer or float value. Default is None (blocking mode),

``--affinity-aware`` − experimental; off by default; turns on the affinity
awareness: a way for the thin client to calculate a data placement for the
given key.

``--username`` and ``--password`` − credentials to authenticate to GridGain
cluster. Used in conjunction with `authenticationEnabled` property in cluster
configuration.

``--examples`` − run the examples as one test. If you wish to run *only*
the examples, supply also the name of the test function to `pytest` launcher:

::

$ pytest --examples ../tests/test_examples.py::test_examples

In this test assertion fails if any of the examples' processes ends with
non-zero exit code.

Examples are not parameterized for the sake of simplicity. They always run
with default parameters (host and port) regardless of any other
`pytest` option.

Since failover, SSL and authentication examples are meant to be controlled
by user or depend on special configuration of the GridGain cluster, they
can not be automated.

Documentation
-------------
To recompile this documentation, do this from your virtualenv_ environment:

::

$ cd gridgain/modules/platforms/python
$ pip install -r requirements/docs.txt
$ cd docs
$ make clean
$ sphinx-apidoc -feM -o source/ ../ ../setup.py
$ make html

Then open `gridgain/modules/platforms/python/docs/generated/html/index.html`_
in your browser.

Licensing
---------

This is a free software, brought to you on terms of the
`GridGain Community Edition License`_.

.. _GridGain: https://docs.gridgain.com/docs
.. _binary client protocol: https://apacheignite.readme.io/docs/binary-client-protocol
.. _GridGain Community Edition License: https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
.. _virtualenv: https://virtualenv.pypa.io/
.. _setuptools: https://setuptools.readthedocs.io/
.. _gridgain/modules/platforms/python/docs/generated/html/index.html: .
.. _editable installs: https://pip.pypa.io/en/stable/reference/pip_install/#editable-installs
