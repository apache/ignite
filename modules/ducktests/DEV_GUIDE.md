<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# How to Write a New DuckTest

The ignite-ducktests framework is a bilingual integration testing framework:

    ┌──────────────────────┬──────────┬───────────────────────────────────────────────────────────────────┐
    │ Layer                │ Language │ Purpose                                                           │
    ├──────────────────────┼──────────┼───────────────────────────────────────────────────────────────────┤
    │ Test orchestration   │ Python   │ Manages Docker containers, starts/stops nodes, asserts results    │
    │ In-cluster workloads │ Java     │ Runs inside Ignite nodes (cache ops, transactions, queries, etc.) │
    └──────────────────────┴──────────┴───────────────────────────────────────────────────────────────────┘

Each Ignite node runs in a separate Docker container. The Python layer manages container lifecycle and simulates network failures via iptables.

---
## Write a Java Application (if needed)

All Java applications extend IgniteAwareApplication. Create a new file under:
modules/ducktests/src/main/java/org/apache/ignite/internal/ducktest/tests/<your_package>/<YourApp>.java

### Example:
```java
package org.apache.ignite.internal.ducktest.tests.mytest;
      
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

public class MyTestApp extends IgniteAwareApplication {
    @Override
    protected void run(JsonNode params) throws Exception {
        // 1. Signal initialization — Python side waits for this
        markInitialized();
        // 2. Parse parameters passed from Python
        String cacheName = params.get("cacheName").asText();
        int range = params.get("range").asInt(1000);
     
        // 3. Use the cluster (depends on service type)
        //    NODE mode       → this.ignite        (full Ignite node)
        //    THIN_CLIENT     → this.client         (IgniteClient)
        //    THIN_JDBC       → this.thinJdbcDataSource
        IgniteCache<Integer, String> cache = ignite.cache(cacheName);
        for (int i = 0; i < range; i++)
            cache.put(i, "val-" + i);
        // 4. Record a result back to Python (readable via extract_result())
        recordResult("putCount", String.valueOf(range));
        
        // 5. For long-running apps, loop until SIGTERM:
        //    while (!terminated()) { U.sleep(100); }
     
        // 6. Signal completion — Python side waits for this on stop()
        markFinished();
    }
}
```
---
## Application Lifecycle Methods
    ┌─────────────────────────────┬─────────────────────────────────────┬───────────────────────────────────────────┐
    │ Method                      │ What it does                        │ Python effect                             │
    ├─────────────────────────────┼─────────────────────────────────────┼───────────────────────────────────────────┤
    │ markInitialized()           │ Prints IGNITE_APPLICATION_INITIALIZED │ Required before start() returns
    │
    │ markFinished()              │ Prints IGNITE_APPLICATION_FINISHED    │ Required before stop() returns
    │
    │ markBroken(Throwable)       │ Prints IGNITE_APPLICATION_BROKEN      │ Raises IgniteExecutionException in Python
    │
    │ recordResult(name, value)   │ Prints name->value<-                │ Read via app.extract_result(name)         │
    │ markSyncExecutionComplete() │ Run-to-completion shortcut          │ Use instead of init + finish pair         │
    └─────────────────────────────┴─────────────────────────────────────┴───────────────────────────────────────────┘

---
## Service Types (what connection the app gets)

    ┌──────────────────┬───────────────────────────────┬─────────────────────────┐
    │ Service Type     │ Python config class           │ Java field              │
    ├──────────────────┼───────────────────────────────┼─────────────────────────┤
    │ Full server node │ IgniteConfiguration           │ this.ignite             │
    │ Thin client      │ IgniteThinClientConfiguration │ this.client             │
    │ Thin JDBC        │ (same, mode=THIN_JDBC)        │ this.thinJdbcDataSource │
    │ No connection    │ service_type = NONE           │ nothing                 │
    └──────────────────┴───────────────────────────────┴─────────────────────────┘

---
## Write a Python Test

Create a new file under:
modules/ducktests/tests/ignitetest/tests/<your_test>.py
```python
from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.ignite_configuration import (
    IgniteConfiguration, IgniteThinClientConfiguration
)
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST, IgniteVersion

class MyTest(IgniteTest):
    @cluster(num_nodes=3)                          # max containers needed
    @ignite_versions(str(DEV_BRANCH), str(LATEST))  # parameterize by version
    def test_cache_operations(self, ignite_version):
        # ── 1. Start server nodes ──────────────────────────────
        server_config = IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            caches=[CacheConfiguration(name='test-cache', backups=1)]
        )
        servers = IgniteService(self.test_context, server_config, num_nodes=2)
        servers.start()
        # ── 2. Start a client application ──────────────────────
        client_config = IgniteThinClientConfiguration(
            version=IgniteVersion(ignite_version),
            discovery_spi=from_ignite_cluster(servers)
        )
        app = IgniteApplicationService(
            self.test_context,
            client_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.mytest.MyTestApp",
            params={"cacheName": "test-cache", "range": 1000}
        )
        app.start()  # blocks until IGNITE_APPLICATION_INITIALIZED
        # ── 3. Verify results ──────────────────────────────────
        result = app.extract_result("putCount")
        assert result == "1000", f"Expected 1000, got {result}"
        # ── 4. Teardown ────────────────────────────────────────
        app.stop()
        servers.stop()
```
---
## Decorators Reference

### @cluster Annotation
@cluster is a mandatory test method decorator. It tells the ducktape framework how many Docker containers the test will consume and how exactly.

#### Basic Usage
```python
from ignitetest.utils import cluster
class MyTest(IgniteTest):
    @cluster(num_nodes=3)
    # Here num_nodes=3 means the test will consume up to 3
    # containers from the available pool (default 13, set via -n in
    # run_tests.sh).
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    def test_something(self, ignite_version):
        servers = IgniteService(self.test_context, config, num_nodes=2)
        servers.start()
        # ... test logic ...
        servers.stop()
```
#### Key Points:

1. The decorator wraps the test in before()/after()
   This means self.test_context is guaranteed to be initialized before the test runs.

2. num_nodes is the peak requirement
   Specify the maximum number of containers the test may consume simultaneously. For example, if the test launches 2
   server nodes + 1 client app:
```python 
@cluster(num_nodes=3)  # 2 servers + 1 client = 3
def test_with_client(self, ignite_version):
    servers = IgniteService(self.test_context, config, num_nodes=2)
    client = IgniteService(self.test_context, client_config, num_nodes=1)
    servers.start()
    client.start()
```
3. Global cluster_size parameter. You can override num_nodes at runtime via a global parameter:
   ./docker/run_tests.sh -g cluster_size=5 -t ./ignitetest/tests/my_test.py
4. Instead of num_nodes, you can specify a ClusterSpec from ducktape for typed node specifications:
5. One @cluster per method. The decorator must appear once per test method. It sets cluster metadata in ctx.cluster_use_metadata. If the context already has metadata, it is not overwritten.

#### Errors and Limitations

    ┌────────────────────────────────────────┬───────────────────────────────────────────────────────────────┐
    │ Situation                              │ Result                                                        │
    ├────────────────────────────────────────┼───────────────────────────────────────────────────────────────┤
    │ num_nodes exceeds available containers │ Test will not start (ducktape reports insufficient resources) │
    │ num_nodes <= 0                         │ Parsing error when reading global cluster_size                │
    │ Multiple @cluster on one method        │ Only the top one applies                                      │
    │ Missing @cluster                       │ before()/after() not executed — context is not initialized    │
    └────────────────────────────────────────┴───────────────────────────────────────────────────────────────┘

---
### @ignite_versions Annotation

Purpose: @ignite_versions is a test method decorator that parameterizes tests across multiple Ignite versions. It automatically generates separate test executions for each specified version, injecting the version string into the test method as an argument.

#### Basic Usage
``` python
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.version import DEV_BRANCH, LATEST
class MyTest(IgniteTest):
    @cluster(num_nodes=2)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    def test_cache_ops(self, ignite_version):
        # Runs twice: once for "dev", once for "2.17.0" (or whatever LATEST is)
        config = IgniteConfiguration(version=IgniteVersion(ignite_version))
        servers = IgniteService(self.test_context, config, num_nodes=1)
        servers.start()
        # ... test logic ...
        servers.stop()
```
#### Key Points
1. The decorator injects the version string into the test method using the version_prefix. By default, the prefix is ignite_version, so the method receives ignite_version="ignite-2.17.0".
2. Versions specified in the decorator can be completely overridden at runtime using the ignite_versions global
   parameter:
   `./docker/run_tests.sh -gj '{"ignite_versions": ["2.15.0", "dev"]}' -t ./ignitetest/tests/my_test.py`
   If ignite_versions is present in globals, the decorator ignores *args and uses the global list instead. This
   allows CI/CD pipelines to control test versions without code changes.

3. You can stack multiple @ignite_versions decorators with different prefixes to test cross-version compatibility
   (e.g., server vs. client versions):
``` python
@cluster(num_nodes=2)
@ignite_versions(str(DEV_BRANCH), str(LATEST), version_prefix="server_version")
@ignite_versions(str(DEV_BRANCH), str(LATEST), version_prefix="client_version")
def test_cross_version(self, server_version, client_version):
   # Generates a cartesian product of server/client versions
```
4. If a single test needs to spin up nodes with different versions simultaneously, pass a tuple/list of size ≥ 2 as
   one argument: `@ignite_versions(("dev", "2.17.0"))`
5. @ignite_versions must be placed below @cluster and above @matrix (or the method definition):

#### Errors and Limitations

     ┌────────────────────────────────────────────────────┬──────────────────────────────────────────────────────────┐
     │ Situation                                          │ Result                                                   │
     ├────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────┤
     │ Duplicate version_prefix on stacked decorators     │ Second decorator is skipped 								│
     │ Global ignite_versions is not a string or iterable │ AssertionError during test collection                    │
     │ Version string doesn't match to installed          │ Test fails at runtime                                    │
     │ Mixing single-string and tuple                     │ Unpredictable argument names; use distinct prefixes      │
     └────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────┘
#### Examples:
```python
#Minimal (Single Version Parameter)
@cluster(num_nodes=1)
@ignite_versions(str(DEV_BRANCH))
def test_basic(self, ignite_version):
   config = IgniteConfiguration(version=IgniteVersion(ignite_version))
   # ...
```
``` python
#Cross-Version Compatibility
@cluster(num_nodes=3)
@ignite_versions(str(DEV_BRANCH), str(LATEST), version_prefix="server_ver")
@ignite_versions(str(DEV_BRANCH), str(LATEST), version_prefix="client_ver")
```
``` python
   Mixed-Version Cluster
@cluster(num_nodes=2)
@ignite_versions(("dev", "2.17.0"))
```
---
### @matrix Annotation

Purpose: @matrix is a standard ducktape decorator that parameterizes test methods by creating a cartesian product of all provided parameters. Each unique combination of parameters results in a separate test execution.

#### Basic Usage
``` python
from ducktape.tests.test import matrix
from ignitetest.utils import cluster, ignite_versions
class MyTest(IgniteTest):
   @cluster(num_nodes=4)
   @ignite_versions(str(DEV_BRANCH), str(LATEST))
   @matrix(nodes_to_kill=[1, 2], load_type=["atomic", "transactional"])
   def test_failure(self, ignite_version, nodes_to_kill, load_type):
       # Generates 2 versions × 2 nodes_to_kill × 2 load_type = 8 test executions
```
#### Key Points
1.The decorator creates all possible combinations of the provided parameters. For example:
@matrix(a=[1, 2], b=["x", "y"])
Produces 4 test cases:
- a=1, b="x"
- a=1, b="y"
- a=2, b="x"
- a=2, b="y"
2. Each parameter from @matrix is injected as a keyword argument into the test method. The method signature must include all injected parameters:
``` python
@matrix(nodes_to_kill=[1, 2], net_partition=True)
def test_something(self, ignite_version, nodes_to_kill, net_partition):
   # nodes_to_kill and net_partition are injected by @matrix
   # ignite_version is injected by @ignite_versions
```
3. @matrix and @ignite_versions work together. The total number of test executions is:
   `(versions count) × (matrix combinations count)`
4. Any Python object can be used as a parameter value:
```python
@matrix(
num_backups=[0, 1, 2],
cache_mode=[CacheMode.PARTITIONED, CacheMode.REPLICATED],
enabled=[True, False],
timeout_ms=[5000, 10000]
)
def test_config(self, num_backups, cache_mode, enabled, timeout_ms):
```
5. If any parameter list is empty, the entire parameter combination is skipped — no test cases are generated for that decorator.

6. @matrix must be placed below @cluster and @ignite_versions, and above the method definition:

#### Errors and Limitations:

#### Runtime Errors

     ┌───────────────────────────────────────────────────────────────────────────────┬───────────────────────────────────────────────────────────────────────────────────────┐
     │ Situation                                                                     │ Error / Result                                                                        │
     ├───────────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────┤
     │ Test method signature does not include all injected parameters                │ TypeError: test_failure() missing X required positional arguments                     │
     │ Test method signature includes extra parameters not injected by any decorator │ TypeError: test_failure() got an unexpected keyword argument                          │
     │ Parameter value is not a list or iterable                                     │ TypeError: 'int' object is not iterable (ducktape internal error)                     │
     │ Using @matrix on a non-method function                                        │ Parameters are still injected, but self.test_context is None — runtime AttributeError │
     └───────────────────────────────────────────────────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────┘

#### Behavior Limitations

    ┌──────────────────────────────────────────────────────────┬────────────────────────────────────────────────────────────────────────────────────────────┐
    │ Situation                                                │ Behavior                                                                                   │
    ├──────────────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────┤
    │ One parameter list is empty                              │ The entire @matrix produces zero test cases — the test is silently skipped                 │
    │ Multiple @matrix decorators on one method                │ Each @matrix is applied independently, creating nested cartesian products                  │
    │ Large number of combinations                             │ Exponential test growth — e.g., 3 params × 5 values each = 125 test executions per version │
    │ Non-hashable parameter values (e.g., lists, dicts)       │ Ducktape may fail to deduplicate test cases — unexpected duplicates or crashes             │
    │ Parameter names conflict with @ignite_versions injection │ Name collision in ctx.injected_args — last writer wins, unpredictable behavior             │
    │ Mutable default values in parameter lists                │ Shared mutable state across test executions — potential side effects                       │
    └──────────────────────────────────────────────────────────┴────────────────────────────────────────────────────────────────────────────────────────────┘


#### Test Reporting Limitations

    ┌───────────────────────────────────────────────────────┬──────────────────────────────────────────────────────────────────────────────────────────────┐
    │ Situation                                             │ Result                                                                                       │
    ├───────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────┤
    │ Many matrix combinations produce identical test names │ Ducktape appends parameter values to the test ID for uniqueness, but logs may become verbose │
    │ One matrix combination fails                          │ Other combinations still run — they are independent test executions                          │
    │ Long-running matrix combinations                      │ Total test suite time = (combinations count) × (single test time) — can be very slow         │
    └───────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────────────────────────────────────────┘
---
### @ignore_if Annotation

Purpose: @ignore_if marks a test method as IGNORED if a specific condition evaluates to True. It is used to skip tests for certain Ignite versions or global parameter configurations without removing the test code.

#### Basic Usage
```python
from ignitetest.utils import cluster, ignite_versions, ignore_if
from ignitetest.utils.version import V_2_11_0
class MyTest(IgniteTest):
    @cluster(num_nodes=2)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @ignore_if(lambda version, globals: version <= V_2_11_0)
    def test_new_feature(self, ignite_version):
        # Skipped for Ignite 2.11.0 and older
        config = IgniteConfiguration(version=IgniteVersion(ignite_version))
        # ...
```
#### Key Points

1. The condition parameter is a callable (usually a lambda) that receives two arguments:
    - version: An IgniteVersion object representing the injected version.
    - globals: A dict of global parameters passed to ducktape.
2. By default, @ignore_if looks for the injected argument named ignite_version. If you used a different version_prefix in @ignite_versions, you must specify it:
```python
@ignite_versions(str(DEV_BRANCH), version_prefix="server_version")
@ignore_if(lambda ver, g: ver.is_dev, variable_name='server_version')
def test_something(self, server_version):
    ...
```
3. The condition function can also inspect global parameters:
```python
@ignore_if(lambda ver, g: not g.get('ssl', {}).get('enabled', False))
def test_ssl_feature(self, ignite_version):
    # Skipped if SSL is not enabled in globals
```
4. @ignore_if is evaluated after @cluster and @ignite_versions have set up the test contexts. If a test is ignored, @cluster's before()/after() wrapper is still present, but the test body is marked as skipped.
5. @ignore_if must be placed below @cluster and @ignite_versions:

#### Errors and Limitations

#### Runtime Errors

    ┌─────────────────────────────────────────────────┬────────────────────────────────────────────────────────────────────┐
    │ Situation                                       │ Error / Result                                                     │
    ├─────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────┤
    │ variable_name does not exist in ctx.injected_args │ KeyError during test collection                                    │
    │ Injected value for variable_name is not a str   │ AssertionError: "'<variable_name>' injected args must be a string" │
    │ condition is not callable                       │ TypeError: '<type>' object is not callable                         │
    │ condition raises an exception                   │ The exception propagates — test collection fails                   │
    └─────────────────────────────────────────────────┴────────────────────────────────────────────────────────────────────┘

#### Behavior Limitations

    ┌───────────────────────────────────┬───────────────────────────────────────────────────────────────────────────────────────────────┐
    │ Situation                         │ Behavior                                                                                      │
    ├───────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────┤
    │ Multiple @ignore_if on one method │ Each is applied independently; if any returns True, the test is skipped                       │
    │ Used without @ignite_versions     │ variable_name will not be injected — KeyError unless variable_name comes from another decorator │
    │ Condition logic is complex        │ Debugging skipped tests becomes harder — use logging or simple predicates                     │
    │ Ignored tests in CI/CD reports    │ Reported as IGNORED, not PASSED — may affect coverage or pass-rate metrics                    │
    └───────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────────┘

#### Examples
```python
#Skip by Version
@cluster(num_nodes=2)
@ignite_versions(str(DEV_BRANCH), str(LATEST))
@ignore_if(lambda ver, g: ver < V_2_12_0)
def test_requires_212(self, ignite_version):
    ...
```
```python
#Skip by Global Parameter
@cluster(num_nodes=2)
@ignite_versions(str(DEV_BRANCH))
@ignore_if(lambda ver, g: not g.get('authentication', {}).get('enabled'))
def test_auth_feature(self, ignite_version):
    # Skipped if authentication is not enabled
```
```python
#Skip Multiple Versions
SKIP_VERSIONS = {V_2_8_0, V_2_10_0}
@ignore_if(lambda ver, g: ver in SKIP_VERSIONS)
def test_unstable_in_old_versions(self, ignite_version):
    ...
```
---
## IgniteConfiguration Reference

Key fields for IgniteConfiguration (NamedTuple):

    ┌───────────────────────────┬──────────────────────────┬─────────────────────────────────────────┐
    │ Field                     │ Type                     │ Description                             │
    ├───────────────────────────┼──────────────────────────┼─────────────────────────────────────────┤
    │ version                   │ IgniteVersion            │ Required — Ignite version               │
    │ client_mode               │ bool                     │ True for client node                    │
    │ discovery_spi             │ DiscoverySpi             │ TcpDiscoverySpi / ZookeeperDiscoverySpi │
    │ caches                    │ list[CacheConfiguration] │ Cache configs                           │
    │ data_storage              │ DataStorageConfiguration │ Persistent store settings               │
    │ auth_enabled              │ bool                     │ Enable authentication                   │
    │ cluster_state             │ str                      │ "ACTIVE" / "INACTIVE"                   │
    │ failure_detection_timeout   │ int                      │ Timeout in ms                           │
    │ transaction_configuration │ TransactionConfiguration │ TX settings                             │
    │ sql_schemas               │ list                     │ SQL schemas                             │
    └───────────────────────────┴──────────────────────────┴─────────────────────────────────────────┘
---
## Discovery Helpers

    Purpose: Classes and utility functions to configure node discovery (TcpDiscoverySpi or ZookeeperDiscoverySpi) for Ignite services.

#### TcpDiscoverySpi
    Configures TCP-based discovery.
     - Parameters: ip_finder, port (default 47500), port_range (default 100), local_address.
     - Type: Returns 'TCP'.
     - Behavior: Calls ip_finder.prepare_on_start() during service startup.

#### TcpDiscoveryVmIpFinder
    Provides a static list of IP addresses for TCP discovery.
     - Parameters: nodes (list of cluster nodes).
     - Type: Returns 'VM'.
     - Behavior: Extracts externally routable IPs from the provided nodes.

#### ZookeeperDiscoverySpi
    Configures Zookeeper-based discovery.
     - Parameters: zoo_service (ZookeeperService instance), root_path (ZNode path).
     - Type: Returns 'ZOOKEEPER'.
     - Behavior: Extracts connection string, client port, and session timeout from the Zookeeper service.

#### Helper Functions

    from_ignite_cluster(cluster, subset=None)
    Creates a TcpDiscoverySpi with a TcpDiscoveryVmIpFinder populated from an existing IgniteService cluster.
     - `subset`: Optional slice object to use only a portion of the cluster's nodes (e.g., slice(0, 2)).

    from_ignite_services(ignite_service_list)
    Creates a TcpDiscoverySpi combining static IPs from a list of multiple IgniteService objects.

    from_zookeeper_cluster(cluster, root_path="/apacheIgnite")
    Creates a ZookeeperDiscoverySpi configured from a ZookeeperService cluster.
     - `root_path`: The root ZNode path for Ignite (default: /apacheIgnite).

#### Usage Example
```python
# TCP Discovery from existing servers
discovery = from_ignite_cluster(servers)
# TCP Discovery from multiple service groups
discovery = from_ignite_services([servers1, servers2])
# Zookeeper Discovery
discovery = from_zookeeper_cluster(zk_quorum, root_path="/myIgnite")
config = IgniteConfiguration(
    version=IgniteVersion(ignite_version),
    discovery_spi=discovery
)
```
---
## Service Utility Methods

All IgniteAwareService subclasses (including IgniteService and IgniteApplicationService) expose:


    ┌─────────────────────────────────────────────┬─────────────────────────────────────────┐
    │ Method                                      │ Description                             │
    ├─────────────────────────────────────────────┼─────────────────────────────────────────┤
    │ await_event(pattern, timeout_sec, nodes=None) │ Wait for a log pattern                  │
    │ drop_network(nodes, net_part=NetPart.ALL)     │ Simulate network partition via iptables │
    │ exec_command(node, cmd)                     │ Run a shell command on a node           │
    │ node_id(node)                               │ Get UUID of a node from logs            │
    │ alive(node)                                 │ Check if a node process is alive        │
    │ thread_dump(node)                           │ Dump JVM threads                        │
    └─────────────────────────────────────────────┴─────────────────────────────────────────┘
---
## Key Files to Reference

    ┌────────────────────────────────────────────────────────────┬───────────────────────────────────────────┐
    │ File                                                       │ Purpose                                   │
    ├────────────────────────────────────────────────────────────┼───────────────────────────────────────────┤
    │ .../ducktest/utils/IgniteAwareApplication.java             │ Base class for Java apps                  │
    │ .../ducktest/utils/IgniteAwareApplicationService.java      │ Main runner (NODE/THIN_CLIENT/JDBC modes) │
    │ .../ignitetest/utils/ignite_test.py                        │ Base IgniteTest class                     │
    │ .../ignitetest/utils/_mark.py                              │ @cluster, @ignite_versions, @ignore_if      │
    │ .../ignitetest/utils/version.py                            │ DEV_BRANCH, LATEST, all version constants │
    │ .../ignitetest/services/ignite.py                          │ IgniteService                             │
    │ .../ignitetest/services/ignite_app.py                      │ IgniteApplicationService                  │
    │ .../ignitetest/services/utils/ignite_aware.py              │ IgniteAwareService base                   │
    │ .../ignitetest/services/utils/ignite_configuration/__init__.py │ All config NamedTuples                    │
    │ .../ignitetest/services/utils/control_utility.py           │ ControlUtility wrapper                    │
    │ .../ignitetest/tests/smoke_test.py                         │ Simplest test example                     │
    │ .../ignitetest/tests/discovery_test.py                     │ Complex parameterized test                │
    │ .../ignitetest/tests/self_test.py                          │ Config variation examples                 │
    └────────────────────────────────────────────────────────────┴───────────────────────────────────────────┘
