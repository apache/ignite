# ignite-python-client
Apache Ignite binary protocol client, written in Python 3.

## Requirements

### *for using:*
- Python 3.4+
- attrs

### *for testing:*
- pytest
- pytest-runner
- pytest-cov

### *for building documentation:*
- Sphinx

## Installation
If you only want to use the `pyignite` module in your project, do:
```
$ pip install pyignite
```

If you want also run tests and build documentation, clone the whole
repository:
```
$ git clone git@github.com:nobitlost/ignite.git
$ git checkout ignite-7782
$ cd ignite/modules/platforms/python
```

You may consult `distutils` manual about using `setup.py`.

## Documentation
Run
```
$ cd ignite/modules/platforms/python/docs
$ make html
```

Then open `docs/_build/html/index.html` in your browser.

## Tests
Run
```
$ cd ignite/modules/platforms/python
$ python setup.py pytest
```

*NB!* Some tests require Apache Ignite node running on localhost:10800.
To override the default parameters, use command line options
`--ignite-host` and `--ignite-port`:
```
$ python setup.py pytest --addopts "--ignite-host=example.com --ignite-port=19840"
```

You can use each of these options multiple times. All combinations
of given host and port will be tested.
