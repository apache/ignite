# Apache Ignite Integration Test Framework - Ducktests

The `ignitetest` framework provides basic functionality and services to write integration tests for Apache Ignite. This framework is built on top of the **ducktape** test framework.
* For core concepts, see the [ducktape source code](https://github.com/confluentinc/ducktape).
* For framework details, see the [ducktape documentation](https://ducktape.readthedocs.io/en/latest/index.html).

### Repository Structure
All paths below are relative to `${IGNITE_HOME}/modules/ducktests/tests`:
* `./ignitetest/services`: Contains basic services and cluster orchestration logic.
* `./ignitetest/utils`: Contains testing helper utilities.
* `./ignitetest/tests`: Contains the actual integration test scenarios.
* `./checks`: Contains internal framework unit tests and style decorators.

---

## Quick Start (Local Docker Run)

Docker is used to emulate a distributed multi-node cluster environment where each individual container acts as a running cluster node.

### 1. Prerequisites
* **Docker** installed and running on your host system.
* **Python >= 3.8** installed on your host system (required only for local environment scripts and development).

### 2. Prepare the Environment & Code
Execute these preparation steps from the root directory of your project:
```bash
# 1. Change your current directory to the Ignite root
cd ${IGNITE_HOME}

# 2. Build the Apache Ignite ducktests modules
./scripts/build-module.sh ducktests

# 3. Navigate into the ducktests directory
cd modules/ducktests/tests
```

### 3. Preparing the Local Environment
Run the following commands from your host system's shell inside `${IGNITE_HOME}/modules/ducktests/tests`:
```bash
# Create and activate an isolated development virtual environment
python3 -m venv ~/.virtualenvs/ignite-ducktests-dev
source ~/.virtualenvs/ignite-ducktests-dev/bin/activate

# Install core framework testing requirements and editable dependencies
pip install -r docker/requirements-dev.txt
pip install -e .
```

> If your environment is configured to look only at internal or restricted artifact registries, `pip install` may fail to find specific package versions.
>
> To resolve this, append the public PyPI mirror to your installation command:

```bash
pip install -r docker/requirements-dev.txt --extra-index-url https://pypi.org/simple
```

### 4. Run a Smoke Test
Run the test runner script by pointing it directly to a specific smoke test target. The script will automatically build the required container images, bring up the necessary nodes, and run the test scenario:

```bash
./docker/run_tests.sh -t ./ignitetest/tests/smoke_test.py::SmokeServicesTest.test_ignite_start_stop -n 3 --global-json '{"cluster_size": 2}'
```

### 5. What a Successful Run Looks Like
When the test runs successfully, your terminal output will display discovery logs followed by a clean execution matrix report:

```text
[INFO]: Discovered 1 tests to run
[INFO]: starting test run with session id 2026-06-25--004...
[INFO]: running 1 tests...
[INFO]: RunnerClient: ... SmokeServicesTest.test_ignite_start_stop: Running...
[INFO]: RunnerClient: ... SmokeServicesTest.test_ignite_start_stop: PASS
================================================================================
SESSION REPORT (ALL TESTS)
ducktape version: 0.13.0
run time:         6.554 seconds
tests run:        1
passed:           1
failed:           0
================================================================================
test_id:    ...SmokeServicesTest.test_ignite_start_stop
status:     PASS
```

### 6. Managing and Cleaning Containers
Always clean up and tear down active background nodes after your test runs finish to free up local resources:
```bash
# Display detailed help options and arguments
./docker/run_tests.sh --help

# Stop and remove all currently active ducker-ignite cluster nodes
./docker/clean_up.sh
```

---

## Local Development & Code Checks

To modify framework logic or contribute features locally without depending on Docker container environments, isolate your development runtime dependencies.

### 1. Activate the Local Environment
Run the following commands from your host system's shell inside `${IGNITE_HOME}/modules/ducktests/tests`:
```bash
# Activate an isolated development virtual environment
source ~/.virtualenvs/ignite-ducktests-dev/bin/activate
```

### 2. Running Linters and Unit Tests
```bash
# Execute local framework utility unit tests
pytest

# Enforce uniform codebase style rules
flake8
```

### 3. Testing Across Multiple Python Versions (Optional)
To locally simulate validation matrices across distinct target runtimes (e.g., Python 3.8, 3.9) using `tox`:
1. Install [pyenv](https://github.com/pyenv/pyenv#installation) onto your machine.
2. Download target runtimes and initialize your runtime shell profile context:
   ```bash
   pyenv install 3.8
   pyenv install 3.9
   pyenv shell 3.8 3.9
   ```
3. Install `tox` and run the validation suite:
   ```bash
   pip install tox
   tox
   tox -r -e codestyle,py3
   ```

---

## Testing with Ignite Extensions

Some integration tests (such as CDC replication scenarios) require additional modules maintained in the separate [ignite-extensions](https://github.com/apache/ignite-extensions) repository.

### Setup Directory Structure
To run these tests, the `ignite-extensions` working directory **must** be checked out at the exact same filesystem level as your main `ignite` repository:
```text
 your-development-folder/
├──  ignite/            <- Checked out from the main ignite repository (${IGNITE_HOME})
└──  ignite-extensions/ <- Checked out from the ignite-extensions repository
```

### Building the Extensions
The target extension module must be compiled on your host before running the test suite. For example, to prepare the `cdc-ext` module:
```bash
cd ${IGNITE_HOME}/../ignite-extensions
mvn clean package -pl :ignite-cdc-ext -Pskip-docs -DskipTests
```
*Note: The local docker startup script automatically handles binding this directory inside the containers if the folder structure matches.*

---

## Global Parameters & Special Runs

You can modify test environments at execution time using global flags injected through the `--global-json` parameter.

#### Test Configuration

| Global Parameter Key | Definition | Example Configuration |
|---------------------|------------|----------------------|
| **cluster_size** | Controls the cluster size for tests. Overrides the default num_nodes value passed in the @cluster annotation. Default value is determined by the test framework. | ```{"cluster_size": 13}``` |
| **ignite_versions** | List of Ignite versions for testing. Tests run for each version in the list. Values are folder names in /opt/ (e.g., "ignite-dev" for master branch, "ignite-2.17.0" for release). Default is determined by test annotations. | ```{"ignite_versions": ["ignite-dev", "ignite-2.17.0"]}``` |
| **failure_detection_timeout** | Timeout in milliseconds for failure detection in discovery. Used to detect node failures in the cluster. Default value is 10000 ms (10 seconds). | ```{"failure_detection_timeout": 20000}``` |

#### Security & Authentication

| Global Parameter Key | Definition | Example Configuration |
|---------------------|------------|----------------------|
| **ssl** | SSL configuration with nested parameters. Enabled flag controls SSL globally, params contains keystore configurations for different aliases (server, client, admin). *Paths beginning with a `/` resolve as absolute paths. Otherwise, files are read relative to `/mnt/service/shared/`.* | ```{"ssl": {"enabled": true, "params": {"client": {"key_store_jks": "client.jks", "key_store_password": "pwd", "trust_store_jks": "truststore.jks", "trust_store_password": "pwd"}}}}``` |
| **authentication** | Authentication configuration with nested parameters. Enabled flag controls authentication globally, username and password specify credentials. Default username is "ignite" and password is "ignite". | ```{"authentication": {"enabled": true, "username": "admin", "password": "secret"}}``` |

#### Performance & Monitoring

| Global Parameter Key | Definition | Example Configuration |
|---------------------|------------|----------------------|
| **jfr_enabled** | Boolean flag to enable Java Flight Recorder for performance profiling. Default is False. | ```{"jfr_enabled": true}``` |
| **safepoint_log_enabled** | Boolean flag to enable safepoint logging for debugging JVM behavior. Default is False. | ```{"safepoint_log_enabled": true}``` |
| **jmx_remote** | JMX remote monitoring configuration with nested parameters. Enabled flag controls remote JMX access, port specifies the listening port (default is 1098). | ```{"jmx_remote": {"enabled": true, "port": 1099}}``` |
| **metrics** | Metrics configuration with nested parameters. JMX metrics can be enabled, and OpenCensus metrics can be configured with period and port settings. Default period is 1000ms and default port is 8082. | ```{"metrics": {"jmx": {"enabled": true}, "opencensus": {"enabled": true, "period": 2000, "port": 8083}}}``` |

*Note on OpenCensus Telemetry:* To separate metrics from different tests, the `iin` label is injected into the configuration of each Ignite node. It can be used in downstream software like Grafana for precise filtration. The label contains a detailed dot-concatenated string representing the full namespace of the current test runner context (e.g., `discovery_test.DiscoveryTest.test_nodes_fail_not_sequential_zk.nodes_to_kill.2.load_type.ClusterLoad.ATOMIC.ignite_version.ignite-2.11.0`).

#### Framework Configuration

| Global Parameter Key | Definition | Example Configuration |
|---------------------|------------|----------------------|
| **NodeSpec** | Specifies the class to use for node specifications in Ignite services. Controls how Ignite nodes are configured and started. | ```{"NodeSpec": "myapp.services.MyNodeSpec"}``` |
| **AppSpec** | Specifies the class to use for application specifications in Ignite applications. Controls how Ignite applications are configured and started. | ```{"AppSpec": "myapp.services.MyAppSpec"}``` |
| **IgniteTestContext** | Class name for the test context implementation. Allows customization of test context behavior. | ```{"IgniteTestContext": "myapp.context.CustomTestContext"}``` |
| **project** | Project/fork name for version handling (e.g., "ignite", "fork"). Used to distinguish between different Ignite variants. Default is "ignite". | ```{"project": "fork"}``` |

#### Paths & Directories

| Global Parameter Key | Definition | Example Configuration |
|---------------------|------------|----------------------|
| **persistent_root** | Root directory for persistent storage in tests. Used for storing test data and logs. Default is typically the test working directory. | ```{"persistent_root": "/opt/ignite/test-data"}``` |
| **install_root** | Root directory for Ignite installation. Points to the base directory where Ignite is installed for testing. Default is typically "/opt/ignite". | ```{"install_root": "/opt/ignite-testing"}``` |

### Custom Ignites & Forks Testing
You can test arbitrary binary releases or custom compiled forks by overriding the execution path:
* **Binary releases:** Must be extracted to the `/opt` directory (e.g., `/opt/ignite-2.11.0`).
* **Source forks:** Must be located at `/opt` and compiled using `./scripts/build-module.sh ducktests`.

To use a custom directory path entirely, pass the `install_root` global configuration variable:
```bash
--global-json '{"install_root": "/custom_directory_path"}'
```

You can target specific cross-product version compatibility combinations inside test suites using the `@ignite_versions` decorator, or override them dynamically during running execution:

```bash
# Runs tests only against versions '2.8.1' and 'dev'
--global-json '{"ignite_versions":["2.8.1", "dev"]}'

# Specifies a custom product prefix ('fork') to execute tests across 'ignite-2.8.1', 'fork-2.8.1', and 'fork-dev'
--global-json '{"project": "fork", "ignite_versions": ["ignite-2.8.1", "2.8.1", "dev"]}'
```


### Diagnostics & Performance Utilities
```bash
# Enable Java Flight Recorder (JFR) tracing
--global-json '{"jfr_enabled": true}'

# Enable JVM Safepoints performance logging
--global-json '{"safepoint_log_enabled": true}'
```

### Security Settings
```bash
# Enable built-in authentication overrides
--global-json '{"authentication": {"enabled": true, "username": "custom_user", "password": "custom_password"}}'

# Enable default SSL/TLS configurations
--global-json '{"ssl": {"enabled": true}}'
```

To supply precise keystores for explicit communication roles, use detailed layout arrays:
```json
{
  "ssl": {
    "enabled": true,
    "params": {
      "server": { "key_store_jks": "server.jks", "key_store_password": "pwd", "trust_store_jks": "truststore.jks", "trust_store_password": "pwd" }
    }
  }
}
```
*Paths beginning with a `/` resolve as absolute paths. Otherwise, files are read relative to `/mnt/service/shared/`.*

For more information about ssl in ignite you can check this link: [SSL in ignite](https://ignite.apache.org/docs/latest/security/ssl-tls)

### Prometheus & JMX Metrics Exporters
To configure external scraping agents (like Prometheus or Zabbix) to systematically track container telemetry, apply tracking labels.

In case of docker you might want ask it to assign the same IP addresses to containers each time it is invoked. This way you will be able to create a static config file for your metrics scraper. To do so use the `--subnet` parameter for the `./docker/run_tests.sh` script as:

```bash
./docker/run_tests.sh --subnet 170.20.0.0/16
```


Pass the following telemetry properties to open standard scrape ports:
```json
{
  "jmx_remote": { "enabled": true, "port": 1098 },
  "metrics": {
    "jmx": { "enabled": true },
    "opencensus": { "enabled": true, "port": 8082 }
  }
}
```
To separate metrics from different tests, the `iin` label is used. It can be utilized in downstream software like Grafana for precise filtration.

The label contains a detailed **test id** string composed of the dot-concatenated test package name, test class name, test method name, and name/value pairs for all parameters:

`discovery_test.DiscoveryTest.test_nodes_fail_not_sequential_zk.nodes_to_kill.2.load_type.ClusterLoad.ATOMIC.ignite_version.ignite-2.11.0`

**Implementation note:** The `ignite-opencensus` metrics module uses the `iin` label to expose the `IgniteInstanceName` cluster configuration parameter. The `ducktests` engine exploits this behavior by injecting the exact *test id* directly into the configuration of each Ignite node started within that test scope.

```yaml
# Sample host prometheus.yml scrape job configuration
scrape_configs:
  - job_name: 'ducktests'
    static_configs:
      - targets: ['172.20.0.2:8082', '172.20.0.3:8082', '172.20.0.4:8082']
```

---

## Distributed Remote Environments

The underlying `ducktape` execution orchestrator allows running these exact suites on production clusters, managed infrastructure, Kubernetes (K8s), Vagrant, Mesos, or Cloud environments.

### Requirements
* Ensure target remote cluster node configurations align with base OS images. Refer to `./docker/Dockerfile` for underlying software, storage routing setup, and platform software prerequisites.

### Execution
Trigger remote integration test execution loops via explicit command wrappers:
```bash
ducktape --results-root=./results --cluster-file=./cluster.json --repeat 1 --max-parallel 16 ./modules/ducktests/tests/ignitetest
```
