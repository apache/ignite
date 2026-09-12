..  Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

..      http://www.apache.org/licenses/LICENSE-2.0

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

This is an Apache Ignite thin (binary protocol) client library,
written in Python 3, abbreviated as *pyignite*.

`Apache Ignite`_ is a memory-centric distributed database, caching,
and processing platform for transactional, analytical, and streaming
workloads delivering in-memory speeds at petabyte scale.

Ignite `binary client protocol`_ provides user applications the ability
to communicate with an existing Ignite cluster without starting
a full-fledged Ignite node. An application can connect to the cluster
through a raw TCP socket.

Prerequisites
-------------

- *Python 3.7* or above (3.7, 3.8, 3.9 and 3.10 are tested),
- Access to *Apache Ignite* node, local or remote. The current thin client
  version was tested on *Apache Ignite 2.13.0* (binary client protocol 1.7.0).

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

$ git clone git@github.com:apache/ignite-python-thin-client.git
$ pip install -e .

This will install the repository version of `pyignite` into your environment
in so-called “develop” or “editable” mode. You may read more about
`editable installs`_ in the `pip` manual.

Then run through the contents of `requirements` folder to install
the the additional requirements into your working Python environment using

::

$ pip install -r requirements/<your task>.txt


For development, it is recommended to install `tests` requirements

::

$ pip install -r requirements/tests.txt

For checking codestyle run:

::

$ flake8

You may also want to consult the `setuptools`_ manual about using `setup.py`.

Examples
--------

Some examples of using pyignite are provided in
`examples` folder. They are extensively
commented in the :ref:`examples_of_usage` section of the documentation.

This code implies that it is run in the environment with `pyignite` package
installed, and Apache Ignite node is running on localhost:10800, unless
otherwise noted.

There is also a possibility to run examples alone with tests. For
the explanation of testing, look up the `Testing`_ section.

Testing
-------

Create and activate virtualenv_ environment.

Install a binary release of Ignite with `log4j2` enabled and set `IGNITE_HOME` environment variable.

::

$ cd <ignite_binary_release>
$ export IGNITE_HOME=$(pwd)
$ cp -r $IGNITE_HOME/libs/optional/ignite-log4j2 $IGNITE_HOME/libs/


Run

::

$ pip install -e .
$ pytest

Other `pytest` parameters:

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
by user or depend on special configuration of the Ignite cluster, they
can not be automated.

Using tox
"""""""""
For automate running tests against different python version, it is recommended to use tox_

::

$ pip install tox
$ tox


Documentation
-------------
To recompile this documentation, do this from your virtualenv_ environment:

::

$ pip install -r requirements/docs.txt
$ cd docs
$ make html

Then open `docs/generated/html/index.html`_
in your browser.

If you feel that old version is stuck, do

::

$ make clean
$ sphinx-apidoc -feM -o source/ ../ ../setup.py
$ make html

And that should be it.

Licensing
---------

This is a free software, brought to you on terms of the `Apache License v2`_.

.. _Apache Ignite: https://ignite.apache.org
.. _binary client protocol: https://ignite.apache.org/docs/latest/binary-client-protocol/binary-client-protocol
.. _Apache License v2: http://www.apache.org/licenses/LICENSE-2.0
.. _virtualenv: https://virtualenv.pypa.io/
.. _tox: https://tox.readthedocs.io/en/latest/
.. _setuptools: https://setuptools.readthedocs.io/
.. _docs/generated/html/index.html: .
.. _editable installs: https://pip.pypa.io/en/stable/reference/pip_install/#editable-installs
