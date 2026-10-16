# Apache Ignite benchmarks

JMH benchmarks for Apache Ignite. They show how a change affects throughput, latency or heap
allocation: run a benchmark before and after the change on the same machine and compare the results.

Most benchmarks start Ignite nodes inside the benchmark JVM and call the public API: cache
operations, SQL, the thin client. The rest measure internal structures and algorithms without
starting a node.

## What's inside

Packages under `org.apache.ignite.internal.benchmarks`:

| Package | What it measures | Run it after changing |
|---|---|---|
| `jmh.cache` | Cache put and get, locks, expiry, async listeners, allocation per operation | Cache operations, transactions, cache messages |
| `jmh.sql`, `jmh.sql.tpch` | SQL scans, joins, aggregates, sorting, DML, user-defined functions and TPC-H queries, on the H2 and Calcite engines | Query engines, indexing |
| `jmh.thin` | Cache put and get through the thin client | The thin client and its protocol |
| `jmh.binary`, `jmh.direct` | Serialization of strings and maps, reading of communication messages | Binary marshalling, message serialization |
| `jmh.streamer`, `jmh.sequence` | Data streamer loading, atomic sequence | The data streamer, atomic data structures |
| `jmh.tree`, `jmh.misc`, `jmh.collections`, `jmh.diagnostic` | B+ tree and index lookups, partition update counters, page replacement lists, page lock tracking, small collections | B+ trees and indexes, partition counters, page memory |
| `jmh.algo`, `jmh.encryption`, `jmh.future`, `jmh.notify` | CRC, keystore encryption, futures, thread wake-up strategies | Checksums, encryption, futures, thread hand-off |
| `jol` | Heap size of internal structures, measured with JOL (Java Object Layout); plain Java programs, not JMH | The memory layout of these structures |

## Before you run

- JDK 17.
- Don't run a benchmark and Ignite tests at the same time on one machine: test nodes look for other
  nodes on the default discovery ports and can join the benchmark's cluster.
- Don't run two benchmarks at the same time either: they slow each other down.
- Results depend on the machine and on what else runs on it. Compare runs made on the same machine
  with nothing heavy running next to them.

## Build

From the repository root, build the module and write its runtime classpath to
`modules/benchmarks/target/cp.txt`. The module is part of the build only with the `benchmarks` or
`all-java` profile:

```bash
./mvnw package dependency:build-classpath -pl modules/benchmarks -am -Pbenchmarks -DskipTests \
    -Dmaven.javadoc.skip=true -Dmaven.source.skip=true \
    -Dmdep.outputFile=target/cp.txt -Dmdep.includeScope=runtime
```

## Run

Run the commands from `modules/benchmarks`. On Windows, the classpath separator is `;` instead of `:`.
JMH runs each benchmark in a fork, a separate JVM that it starts for the run. Ignite needs the flags
from `org.apache.ignite.internal.util.FeatureChecker#JAVA_17_OPTIONS` in that JVM to start on JDK 17.

There are two ways to run a benchmark:

- Run a benchmark class that has a `main()` method. It sets the JMH options and passes the flags to
  the fork:

  ```bash
  java -cp "target/classes:$(cat target/cp.txt)" org.apache.ignite.internal.benchmarks.jmh.sql.JmhSqlScanBenchmark
  ```

- Run `org.openjdk.jmh.Main` to choose the benchmarks and the JMH options yourself. Pass the flags
  through `-jvmArgs`:

  ```bash
  java -cp "target/classes:$(cat target/cp.txt)" org.openjdk.jmh.Main 'JmhSqlScanBenchmark' \
      -p engine=CALCITE -rf json -rff result.json -jvmArgs "<flags from JAVA_17_OPTIONS>"
  ```

  `-l` lists the benchmarks, `-lp` lists them with their parameters, `-h` prints all options.

Benchmark settings:

- JMH parameters (`@Param`), such as the SQL engine, are set with `-p <name>=<value>`.
- `JmhCacheAbstractBenchmark` reads the cache setup from system properties with the
  `ignite.jmh.cache.` prefix: `backups`, `atomicityMode`, `writeSynchronizationMode`, `dataNodes` and
  `clientMode`. Pass them as `-D` options in `-jvmArgs`. A subclass can fix the setup in code instead,
  as `JmhCacheAllocationBenchmark` does.

## Measuring allocation

Bytes allocated per operation show whether a change makes Ignite create more or fewer objects. With
escape analysis off (see below), they repeat closely enough that one extra small object per
operation shows up, on the node that allocates it.

- `-prof gc`, built into JMH, reports bytes per operation for the whole JVM (`gc.alloc.rate.norm`).
  When a benchmark runs client and server nodes in its JVM, this one number covers all of them.
- `JmhAllocationProfiler` splits the bytes per operation by node and thread pool. It also reports
  communication messages and bytes sent, and discovery messages processed, per operation and node.
  Use it to see on which node and in which pool a change adds or removes allocation.

```bash
java -cp "target/classes:$(cat target/cp.txt)" org.openjdk.jmh.Main 'JmhCacheAllocationBenchmark.put$' \
    -f 1 -wi 5 -w 3s -i 5 -r 3s \
    -prof 'org.apache.ignite.internal.benchmarks.jmh.profile.JmhAllocationProfiler:dir=alloc' \
    -rf json -rff result.json -jvmArgs "<flags from JAVA_17_OPTIONS> -XX:-DoEscapeAnalysis"
```

`JmhCacheAllocationBenchmark.main()` runs put and get on an atomic and a transactional cache with
the profiler and writes the results to `JmhCacheAllocationBenchmark.json`.

The profiler's results, per operation:

| Result | What it counts |
|---|---|
| `alloc.norm:benchmark` | The benchmark threads, which call the Ignite API |
| `alloc.norm:client.<node>.<pool>` | A thread pool of a client node, e.g. `client.client.sys-stripe` |
| `alloc.norm:server.<node>.<pool>` | A thread pool of a server node, e.g. `server.node0.grid-nio-worker-tcp-comm` |
| `alloc.norm:thin.<pool>` | The thin client's network threads |
| `alloc.norm:other.<node>.<pool>`, `alloc.norm:other.<thread>` | Threads of a node the profiler doesn't know, such as a stopped one, and other JVM threads |
| `alloc.norm:total` | All of the above |
| `comm.msgs.norm:<node>`, `comm.bytes.norm:<node>` | Communication messages and bytes sent |
| `disco.msgs.norm:<node>` | Discovery messages processed |

The threads of one pool on one node add up to one result. With `dir=<path>`, the profiler also writes
the JDK and OS of the run to `<path>/run.properties`.

The profiler works with the benchmarks that extend `JmhCacheAbstractBenchmark` and keep its teardown.
To use it with another benchmark, see [Writing a benchmark](#writing-a-benchmark).

To compare two builds:

- Add `-XX:-DoEscapeAnalysis`. Escape analysis lets the JIT remove some allocations. Which ones it
  removes differs from one fork to the next, so the results jump between forks. With escape analysis
  off, every allocation in the code counts and forks agree.
- Compare forks, not iterations: iterations of one fork share its JIT decisions. With escape analysis
  on, run at least four forks.

## Writing a benchmark

- Extend the base class of the area: `JmhCacheAbstractBenchmark`, `JmhSqlAbstractBenchmark`,
  `JmhThinClientAbstractBenchmark`, or `JmhAbstractBenchmark` for the rest.
- Start nodes in the benchmark JVM and give each one a name: the allocation profiler finds a node's
  threads by its name.
- Call `JmhNodeStopHook.run()` first in the teardown that stops the nodes. JMH runs this teardown
  before profilers read the last iteration, and the hook lets them read it while the nodes still run.
- Create keys and values outside the measured method when they aren't part of the operation.
  Otherwise their cost and allocation count as Ignite's.
- Add a `main()` that runs the benchmark through `JmhIdeBenchmarkRunner`. The runner passes the JVM
  flags Ignite needs, so the benchmark runs from the IDE and from the command line.

For writing JMH benchmarks in general, start with the
[JMH samples](https://github.com/openjdk/jmh/tree/master/jmh-samples/src/main/java/org/openjdk/jmh/samples).
