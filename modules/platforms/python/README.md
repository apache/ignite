# ignite-python-client
Apache Ignite binary protocol client, written in Python 3.

## Prerequisites

- Python 3.4 or above (3.6 is tested),
- Access to Apache Ignite 2.5 node, local or remote. Higher versions
  of Ignite may or may not work.

## Installation

#### *for end user*
If you only want to use the `pyignite` module in your project, do:
```
$ pip install pyignite
```

#### *for developer*
If you want to run tests, examples or build documentation, clone
the whole repository:
```
$ git clone git@github.com:nobitlost/ignite.git
$ git checkout ignite-7782
$ cd ignite/modules/platforms/python
```

Then run through the contents of `requirements` folder to install
the necessary prerequisites into your working Python environment using
```
$ pip install -r requirements/install.txt
```

You may also want to consult the `setuptools` manual about using `setup.py`.

## Documentation
In your virtualenv environment run
```
$ cd ignite/modules/platforms/python
$ pip install -r requirements/docs.txt
$ cd docs
$ make html
```

Then open
[ignite/modules/platforms/python/docs/generated/html/index.html](ignite/modules/platforms/python/docs/generated/html/index.html)
in your browser.

## Examples
Some examples of using pyignite are provided in
`ignite/modules/platforms/python/examples` folder.

This code implies that it is run in the environment with `pyignite` package
installed. If you want to play with the repository clone of `pyignite`, run
```
$ cd ignite/modules/platforms/python
$ pip install -e .
``` 

to install the package code in your environment without actually copying it
to `site-packages` folder.

## Testing
Run
```
$ cd ignite/modules/platforms/python
$ python setup.py pytest
```

*NB!* All tests require Apache Ignite node running on localhost:10800.
To override the default parameters, use command line options
`--ignite-host` and `--ignite-port`:
```
$ python setup.py pytest --addopts "--ignite-host=example.com --ignite-port=19840"
```

You can use each of these options multiple times. All combinations
of given host and port will be tested.
