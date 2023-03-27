# Overview
The `ignitetest` framework provides basic functionality and services
to write integration tests for Apache Ignite. This framework bases on 
the `ducktape` test framework, for information about it check the links:
- https://github.com/confluentinc/ducktape - source code of the `ducktape`.
- http://ducktape-docs.readthedocs.io - documentation to the `ducktape`.

Structure of the `tests` directory is:
- `./ignitetest/services` contains basic services functionality.
- `./ignitetest/utils` contains utils for testing.
- `./ignitetest/tests` contains tests.
- `./checks` contains unit tests of utils, tests' decorators etc. 

# Local run
Docker is used to emulate distributed environment. Single container represents 
a running node.

## Requirements
To just start tests locally the only requirement is preinstalled `docker`. 
For development process requirements are `python` >= 3.7.

## Run tests
- Change a current directory to`${IGNITE_HOME}`
- Build Apache IGNITE invoking `${IGNITE_HOME}/scripts/build-module.sh ducktests`
- Change a current directory to `${IGNITE_HOME}/modules/ducktests/tests`
- Run tests in docker containers using a following command:
```
./docker/run_tests.sh
```
- For detailed help and instructions, use a following command:
```
./docker/run_tests.sh --help
```
- Test reports, including service logs, are located in the `${IGNITE_HOME}/results` directory.

## Runned tests management
- Tear down all the currently active ducker-ignite nodes using a following command:
```
./docker/clean_up.sh
```

# Real environment run
[Ducktape](https://ducktape-docs.readthedocs.io/en/latest/index.html) allow runs on 
Custom cluster, Vagrant, K8s, Mesos, Docker, cloud providers, etc.

## Requirements
- Set up the cluster.
  See `./docker/Dockerfile` for servers setup hints.

## Run tests
- Change a current directory to`${IGNITE_HOME}`
- Build Apache IGNITE invoking `${IGNITE_HOME}/scripts/build-module.sh ducktests`
- Run tests using [Ducktape](https://ducktape-docs.readthedocs.io/en/latest/run_tests.html). \
  For example:
  ```
  ducktape --results-root=./results --cluster-file=./cluster.json --repeat 1 --max-parallel 16 ./modules/ducktests/tests/ignitetest
  ```
# Custom Ignites (forks) testing
## Run all tests
### Setup
Any version of Apache Ignite, or it's fork, can be tested. 
Binary releases supported as well as compiled sources. 

- Binary releases should be located at `/opt` directory, eg. `/opt/ignite-2.11.0`.
- Source releases also should be located at `/opt` directory, but should be compiled before the first use.\
  Use the following command to compile sources properly:
  ```
  ./scripts/build-module.sh ducktests
  ```
You may replace `/opt` with custom directory by setting `install_root` globals param. \
For example, `--globals-json, eg: {"install_root": "/dir42"}`

### Execution
You may set versions (products) using `@ignite_versions` decorator at code
```
@ignite_versions(str(DEV_BRANCH), str(LATEST))
```
or passing versions set via globals during the execution
```
--globals-json, eg: {"ignite_versions":["2.8.1", "dev"]}
```
You may also specify product prefix by `project` param at globals, for example:
```
--globals-json, eg: {"project": "fork" ,"ignite_versions": ["ignite-2.8.1", "2.8.1", "dev"]}
```
will execute tests on `ignite-2.8.1, fork-2.8.1, fork-dev`

## Run tests from the external source/repository
TBD

# Special runs
## Run with enabled security
### Run with SSL enabled
TBD

### Run with build-in authentication enabled
TBD

## Run with metrics export enabled

To let Ignite nodes export metrics in different formats during test run use the `metrics` globals parameter
as described in following sections.  

You would need to install and configure metrics collecting software (like prometheus or zabbix) by yourselves. 

Below is the sample `prometheus.yml` config file which may be used to collect metrics from tests running in docker:

```yaml
global:
  scrape_interval: 1s
  evaluation_interval: 5s

scrape_configs:
  - job_name: 'ducktests'
    static_configs:
      - targets:
        - 172.20.0.2:8082
        - 172.20.0.3:8082
        - 172.20.0.4:8082
        - 172.20.0.5:8082
        - 172.20.0.6:8082
        - 172.20.0.7:8082
        - 172.20.0.8:8082
        - 172.20.0.9:8082
        - 172.20.0.10:8082
        - 172.20.0.11:8082
        - 172.20.0.12:8082
        - 172.20.0.13:8082
        - 172.20.0.14:8082
```

In case of docker you might want ask it to assign the same IP addresses to containers each time it is invoked. 
This way you will be able to create a static config file for your metrics scraper. To do so use
the `--subnet` parameter for the `./docker/run_tests.sh` script as

```
./docker/run_tests.sh --subnet 170.20.0.0/16
```

### Run with the Prometheus metrics exporter

To export metrics in the Prometheus format via the HTTP use the following `--globals-json` parameters:

```json
{
  "metrics": { 
    "opencensus": { 
      "enabled":true, 
      "port": 8082
    }
  }
}
```

To separate metrics from different tests the `iin` label is used. It may be used in software like grafana for filtration.

Label contains the *test id* string: dot concatenated test package name, test class name, test method name and set of
name/value pairs for parameters, like: 

`discovery_test.DiscoveryTest.test_nodes_fail_not_sequential_zk.nodes_to_kill.2.load_type.ClusterLoad.ATOMIC.ignite_version.ignite-2.11.0`

**Implementation note.** The `ignite-opencensus` metrics module uses the `iin` label to expose the `IgniteInstanceName`
cluster configuration parameter. So the `ducktests` engine exploits this fact and puts the *test id* to 
configuration of each ignite node started in scope of particular test.

### Run with the JMX metrics exporter

To have Ignite nodes export metrics via the JMX use below global parameters. Note that you also might want to enable the 
JMХ remote access (running on 1098 port by default) to let external tools like zabbix collect metrics. 
```json
{
  "jmx_remote": {
    "enabled": true,
    "port": 1098
  },
  "metrics": {
    "jmx": {
      "enabled":true
    }
  }
}
```
  
# Development
## Preparing development environment
- Create a virtual environment and activate it using following commands:
```
python3 -m venv ~/.virtualenvs/ignite-ducktests-dev
source ~/.virtualenvs/ignite-ducktests-dev/bin/activate
```
- Change a current directory to `${IGNITE_HOME}/modules/ducktests/tests`. We refer to it as `${DUCKTESTS_DIR}`.
- Install requirements and `ignitetests` as editable using following commands:
```
pip install -r docker/requirements-dev.txt
pip install -e .
```
---

- For running unit tests invoke `pytest` in `${DUCKTESTS_DIR}`.
- For checking codestyle invoke `flake8` in `${DUCKTESTS_DIR}`.

#### Run checks over multiple python's versions using tox (optional)
All commits and PR's are checked against multiple python's version, namely 3.6, 3.7 and 3.8.
If you want to check your PR as it will be checked on Travis CI, you should do following steps:

- Install `pyenv`, see installation instruction [here](https://github.com/pyenv/pyenv#installation).
- Install different versions of python (recommended versions are `3.7.9` and `3.8.5`)
- Activate them with a command `pyenv shell 3.7.9 3.8.5`
- Install `tox` by invoking a command `pip install tox`
- Change a current directory to `${DUCKTESTS_DIR}` and invoke `tox`
