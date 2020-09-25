## Overview
The `ignitetest` framework provides basic functionality and services
to write integration tests for Apache Ignite. This framework bases on 
the `ducktape` test framework, for information about it check the links:
- https://github.com/confluentinc/ducktape - source code of the `ducktape`.
- http://ducktape-docs.readthedocs.io - documentation to the `ducktape`.

Structure of the `ignitetest` directory is:
- `./ignitetest/services` contains basic services functionality.
- `./ignitetest/utils` contains utils for testing.
- `./ignitetest/tests` contains tests.

Docker is used to emulate distributed environment. Single container represents 
a running node.

## Requirements
To just start tests locally the only requirement is preinstalled `docker`. 
For development process requirements are `python` >= 3.6.

## Run tests locally
1. Change a current directory to`${IGNITE_HOME}`
2. Build Apache IGNITE invoking `${IGNITE_HOME}/scripts/build.sh`
4. Change a current directory to `${IGNITE_HOME}/modules/ducktests/tests`
3. Run tests in docker containers using a following command:
```
./docker/run_tests.sh
```
4. For detailed help and instructions, use a following command:
```
./docker/run_tests.sh --help
```

## Preparing development environment.
1. Create a virtual environment and activate it using following commands:
```
python3 -m venv ~/.virtualenvs/ignite-ducktests-dev
source ~/.virtualenvs/ignite-ducktests-dev/bin/activate
```
2. Change a current directory to `${IGNITE_HOME}/modules/ducktests/tests`. We refer to it as `${DUCKTESTS_DIR}`
3. Install requirements and `ignitetests` as editable using following commands:
```
pip install -r docker/requirements-dev.txt
pip install -e .
```
---

- For running unit tests invoke `pytest` in `${DUCKTESTS_DIR}`.
- For checking codestyle invoke `flake8` in `${DUCKTESTS_DIR}`.
- For running linter invoke `pylint --rcfile=tox.ini ignitetests checks` in `${DUCKTESTS_DIR}`.

#### Run checks over multiple python's versions using tox (optional).
All commits and PR's are checked against multiple python's version, namely 3.6, 3.7 and 3.8
If you want to check your PR as it will be checked on Travis CI, you should do following steps:
1. Install `pyenv`, see installation instruction [here](https://github.com/pyenv/pyenv#installation).
2. Install different versions of python (recommended versions are `3.6.12`, `3.7.9`, `3.8.5`)
3. Activate them with a command `pyenv shell 3.6.12 3.7.9 3.8.5`
4. Install `tox` by invoking a command `pip install tox`
5. Change a current directory to `${DUCKTESTS_DIR}` and invoke `tox`

