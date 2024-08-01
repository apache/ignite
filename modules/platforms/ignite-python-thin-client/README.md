# ignite-python-client
Apache Ignite thin (binary protocol) client, written in Python 3.

<a href="https://ignite.apache.org/"><img src="https://github.com/apache/ignite-website/blob/master/assets/images/apache_ignite_logo.svg" hspace="20"/></a>

![Build Status](https://github.com/apache/ignite-python-thin-client/actions/workflows/pr_check.yml/badge.svg)
[![License](https://img.shields.io/github/license/apache/ignite-python-thin-client?color=blue)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Pypi](https://img.shields.io/pypi/v/pyignite)](https://pypi.org/project/pyignite/)
[![Downloads](https://static.pepy.tech/badge/pyignite/month)](https://pepy.tech/project/pyignite)


## Prerequisites

- Python 3.7 or above (3.7, 3.8, 3.9, 3.10 and 3.11 are tested),
- Access to Apache Ignite node, local or remote. The current thin client
  version was tested on Apache Ignite 2.14 (binary client protocol 1.7.0).

## Installation

### *for end user*
If you only want to use the `pyignite` module in your project, do:
```bash
$ pip install pyignite
```

### *for developer*
If you want to run tests, examples or build documentation, clone
the whole repository:
```bash
$ git clone git@github.com:apache/ignite-python-thin-client.git
$ pip install -e .
```

This will install the repository version of `pyignite` into your environment
in so-called “develop” or “editable” mode. You may read more about
[editable installs](https://pip.pypa.io/en/stable/reference/pip_install/#editable-installs)
in the `pip` manual.

Then run through the contents of `requirements` folder to install
the additional requirements into your working Python environment using
```bash
$ pip install -r requirements/<your task>.txt
```

You may also want to consult the `setuptools` manual about using `setup.py`.

### *optional C extension*
There is an optional C extension to speedup some computational intensive tasks. If it's compilation fails
(missing compiler or CPython headers), `pyignite` will be installed without this module.

- On Linux or MacOS X only C compiler is required (`gcc` or `clang`). It compiles during standard setup process.
- For building universal `wheels` (binary packages) for Linux, just invoke script `./scripts/create_distr.sh`. 
  
  ***NB!* Docker is required.**
  
- On Windows MSVC 14.x required, and it should be in path, also python versions 3.7, 3.8, 3.9, 3.10 and 3.11 both for x86 and
  x86-64 should be installed. You can disable some of these versions but you'd need to edit script for that.
- For building `wheels` for Windows, invoke script `.\scripts\BuildWheels.ps1` using PowerShell. Just make sure that
  your execution policy allows execution of scripts in your environment.
  
  Ready wheels for `x86` and `x86-64` for different python versions (3.7, 3.8, 3.9, 3.10 and 3.11) will be
  located in `distr` directory.

### Updating from older version

To upgrade an existing package, use the following command:
```bash
pip install --upgrade pyignite
```

To install the latest version of a package:
```bash
pip install pyignite
```

To install a specific version:
```bash
pip install pyignite==0.6.1
```

## Documentation
[The package documentation](https://apache-ignite-binary-protocol-client.readthedocs.io)
is available at *RTD* for your convenience.

If you want to build the documentation from source, do the developer
installation as described above, then run the following commands from the
client's root directory:
```bash
$ pip install -r requirements/docs.txt
$ cd docs
$ make html
```

Then open `docs/generated/html/index.html` in your browser.

## Examples
Some examples of using pyignite are provided in `examples` folder. They are
extensively commented in the
“[Examples of usage](https://apache-ignite-binary-protocol-client.readthedocs.io/en/latest/examples.html)”
section of the documentation.

This code implies that it is run in the environment with `pyignite` package
installed, and Apache Ignite node is running on localhost:10800.

## Testing
*NB!* It is recommended installing `pyignite` in development mode.
Refer to [this section](#for-developer) for instructions.

Do not forget to install test requirements: 
```bash
$ pip install -r requirements/install.txt -r requirements/tests.txt
```

Also, you'll need to have a binary release of Ignite with `log4j2` enabled and to set
`IGNITE_HOME` environment variable: 
```bash
$ cd <ignite_binary_release>
$ export IGNITE_HOME=$(pwd)
$ cp -r $IGNITE_HOME/libs/optional/ignite-log4j2 $IGNITE_HOME/libs/
```
### Run basic tests
```bash
$ pytest
```
### Run with examples
```bash
$ pytest --examples 
```

If you need to change the connection parameters, see the documentation on
[testing](https://apache-ignite-binary-protocol-client.readthedocs.io/en/latest/readme.html#testing).
