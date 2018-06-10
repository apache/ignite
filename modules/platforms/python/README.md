# ignite-python-client
Apache Ignite binary protocol client, written in Python 3.

## Requirements
- Python 3.4+
- pytest
- Sphinx
- attrs

## Documentation
Run
```
$ cd ./docs
$ make html
```
Then open `docs/_build/html/index.html` in your browser.

## Tests
Create and activate virtualenv environment. Run

`$ pip install -r requirements.txt`

Then just run

`$ pytest`

*NB!* Some tests require Apache Ignite node running on localhost:10900.
