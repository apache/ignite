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
| `jmh.sql`, `jmh.sql.tpch` | SQL scans, joins, aggregates, sorting, DML, user-defined functions and TPC-H queries, on the H2 and Calcite engines; allocation per query | Query engines, indexing |
| `jmh.thin` | Cache operations, transactions and SQL through the thin client and the JDBC thin driver, allocation per operation | The thin client, JDBC thin and their protocol |
| `jmh.binary`, `jmh.direct` | Serialization of strings and maps, reading of communication messages | Binary marshalling, message serialization |
| `jmh.streamer`, `jmh.sequence` | Data streamer loading, atomic sequence; allocation per operation of the streamer and the data structures | The data streamer, data structures |
| `jmh.compute` | Allocation per call of compute jobs and tasks, services and messaging | The compute grid, services, messaging |
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
  `clientMode`. Pass them as `-D` options in `-jvmArgs`. The allocation benchmarks ignore them and
  take their setup as JMH parameters instead, see [Allocation benchmarks](#allocation-benchmarks).

## Measuring allocation

Bytes allocated per operation show whether a change makes Ignite create more or fewer objects. With
escape analysis off (see below), they repeat closely enough that one extra small object per
operation shows up, on the node that allocates it.

- `-prof gc`, built into JMH, reports bytes per operation for the whole JVM (`gc.alloc.rate.norm`).
  When a benchmark runs client and server nodes in its JVM, this one number covers all of them.
- `JmhAllocationProfiler` splits the bytes per operation by node and thread pool. Per operation and
  node, it also counts communication messages sent (in total and by message class) and received,
  tasks completed by each thread pool, and discovery messages processed. Use it to see on which node
  and in which pool a change adds or removes allocation, and whether it changes the messages and
  tasks of an operation.
- `JmhAllocationHistogramProfiler` counts objects and bytes per operation for each class, by node and
  thread pool. Use it to see which objects a change adds or removes. It is slower, see
  [Objects per class](#objects-per-class).

```bash
java -cp "target/classes:$(cat target/cp.txt)" org.openjdk.jmh.Main 'JmhCacheAllocationBenchmark.put$' \
    -f 1 -wi 5 -w 3s -i 5 -r 3s \
    -prof 'org.apache.ignite.internal.benchmarks.jmh.profile.JmhAllocationProfiler:dir=alloc' \
    -rf json -rff result.json -jvmArgs "<flags from JAVA_17_OPTIONS> -Xms2g -Xmx2g -XX:-DoEscapeAnalysis"
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
| `comm.type.norm:<node>:<class>` | Communication messages sent, by message class, e.g. `comm.type.norm:client:GridNearAtomicSingleUpdateRequest` |
| `comm.rcvd.norm:<node>` | Communication messages received |
| `pool.tasks.norm:<node>.<registry>` | Tasks completed by a thread pool, named by its metric registry, e.g. `pool.tasks.norm:node0.StripedExecutor` |
| `disco.msgs.norm:<node>` | Discovery messages processed |

The threads of one pool on one node add up to one result. A node knows hundreds of message classes
and about 20 pools, so `comm.type.norm` and `pool.tasks.norm` list only those that moved: each
appears from the first iteration, warmup included, in which the node sent such a message or the
pool completed a task, and stays in every iteration after it. JMH counts the measurement iterations
before that as 0 in the result, and leaves them out of `rawData` in the JSON. The registry names a
pool as Ignite's metrics do (`threadPools.<registry>`), not as its threads: `StripedExecutor` runs
the `sys-stripe` threads, `GridSystemExecutor` the `sys`
threads, `GridExecutionExecutor` `pub`, `GridServicesExecutor` `svc`, `GridQueryExecutor` `query`
and `GridThinClientExecutor` `client-connector`. With `dir=<path>`, the profiler also writes
the JDK and OS of the run to `<path>/run.properties`.

Both allocation profilers work with the benchmarks that extend `JmhCacheAbstractBenchmark`,
`JmhSqlAbstractBenchmark` or `JmhThinClientAbstractBenchmark` and keep their teardown. To use them
with another benchmark, see [Writing a benchmark](#writing-a-benchmark).

To compare two builds:

- Add `-XX:-DoEscapeAnalysis`. Escape analysis lets the JIT remove some allocations. Which ones it
  removes differs from one fork to the next, so the results jump between forks. With escape analysis
  off, every allocation in the code counts and forks agree.
- Compare forks, not iterations: iterations of one fork share its JIT decisions. With escape analysis
  on, run at least four forks.
- Some allocation follows time rather than operations: threads such as discovery and timeout workers
  allocate on a timer, and atomic caches send deferred update responses on a timer too. When two
  runs differ much in operations per second, these parts differ per operation as well. The same
  goes for messages and tasks: a fractional part that changes with operations per second is
  time-driven work, not the operation's code.

### Objects per class

`JmhAllocationHistogramProfiler` records allocations with JDK Flight Recorder (JFR) and counts objects
and bytes per operation for each class, in the same groups as `JmhAllocationProfiler`. It counts the
JFR event `jdk.ObjectAllocationOutsideTLAB`, which covers every allocation only when TLABs
(thread-local allocation buffers) are off: `-XX:-UseTLAB`. Without it, the benchmark fails.

```bash
java -cp "target/classes:$(cat target/cp.txt)" org.openjdk.jmh.Main 'JmhCacheAllocationBenchmark.put$' \
    -f 1 -wi 5 -w 3s -i 5 -r 3s -foe true \
    -prof 'org.apache.ignite.internal.benchmarks.jmh.profile.JmhAllocationHistogramProfiler:dir=alloc' \
    -prof 'org.apache.ignite.internal.benchmarks.jmh.profile.JmhAllocationProfiler' \
    -rf json -rff result.json -jvmArgs "<flags from JAVA_17_OPTIONS> -Xms2g -Xmx2g -XX:-UseTLAB -XX:-DoEscapeAnalysis \
    -XX:FlightRecorderOptions:memorysize=512m"
```

`JmhCacheTxAllocationBenchmark.main()` runs a transaction of two puts with both profilers and these
flags. It writes the results to `JmhCacheTxAllocationBenchmark.json` and the per-class tables to the
`JmhCacheTxAllocationBenchmark` directory.

- `-XX:FlightRecorderOptions:memorysize=512m` gives JFR room for the events it hasn't written to disk
  yet. With less, JFR drops events when disk writes slow down, and the benchmark fails: run it again
  with a larger value. The default is 10 MB, and the scan query from a client with a near cache
  dropped events with 256 MB. With `-foe true`, the run stops with an error and writes no
  results; without it, JMH prints `<failure>`, keeps the iterations before it and goes on.
- When you run both profilers, list the histogram profiler first. JMH stops profilers in reverse
  order, so the time the histogram profiler spends reading its recording stays out of `alloc.norm`.
  Before the benchmark stops its nodes, the profilers act in the order listed, so the histogram
  profiler stops recording before the counter profiler reads the nodes' metrics, and that reading
  stays out of the histogram's last iteration.
- With TLABs off, the benchmark runs fewer operations per second, and after each iteration the
  profiler reads the whole recording, so the run takes much longer.

The histogram profiler's results:

| Result | What it counts |
|---|---|
| `jfr.objs.norm:<group>`, `jfr.bytes.norm:<group>` | Objects and bytes per operation allocated by a group, named as in `alloc.norm` |
| `jfr.objs.norm:total`, `jfr.bytes.norm:total` | All groups |

The profiler needs `dir=<path>`. It writes the JDK and OS of the run to `<path>/run.properties`, and
the table per class to a file there, one per benchmark, parameter set and fork:
`<full benchmark name>-<param>=<value>-<process ID>.tsv`, with the parameters whose value differs
from the default. Use a new `dir` for each run, since the files of earlier runs stay there. The
file's columns, separated by tabs:

| Column | What it holds |
|---|---|
| `group` | The group, named as in `alloc.norm` |
| `class` | The class as Java writes it: `byte[]`, `java.lang.Object[]` |
| `size` | For arrays, the range of their size in bytes, header included, by powers of two: `<=16`, `<=32`, `<=64`, ... Empty for other classes |
| `frames` | Only with `site`, see below: where the objects were allocated |
| `iteration` | `warmup-1`, `warmup-2`, ... for warmup iterations, then `1`, `2`, ... |
| `objects/op`, `bytes/op` | Objects and bytes per operation |

A lambda's class name, in `class` and in `frames`, carries a number that differs from one JVM to the
next.

To see where objects are allocated, add `site=<n>` to the profiler's options, separated by `;`. In
measurement iterations the profiler then records the stack of each allocation, and the `frames`
column holds its top `n` frames, from the allocating method outwards:
`Class.method:line < Class.method:line`. A class allocated in several places has a row per place.
The first warmup iteration also starts the nodes, which is too much to record with stacks, so no
warmup iteration records them, and their `frames` are empty.

```bash
java -cp "target/classes:$(cat target/cp.txt)" org.openjdk.jmh.Main 'JmhCacheAllocationBenchmark.put$' \
    -f 1 -wi 5 -w 3s -i 3 -r 3s -foe true \
    -prof 'org.apache.ignite.internal.benchmarks.jmh.profile.JmhAllocationHistogramProfiler:dir=alloc;site=12' \
    -prof 'org.apache.ignite.internal.benchmarks.jmh.profile.JmhAllocationProfiler' \
    -rf json -rff result.json -jvmArgs "<flags from JAVA_17_OPTIONS> -Xms2g -Xmx2g -XX:-UseTLAB -XX:-DoEscapeAnalysis \
    -XX:FlightRecorderOptions:memorysize=1g,stackdepth=12"
```

- With stacks, the benchmark runs far fewer operations per second and JFR's data grows.
  `memorysize=1g` gives JFR room for it, and `stackdepth` keeps only the frames the profiler writes
  (JFR records 64 by default).
- Use `site` to find where objects come from, not to compare totals between runs: at a different
  speed, the parts that follow time differ per operation (see [To compare two
  builds](#measuring-allocation)).

### Allocation benchmarks

These benchmarks in `jmh.cache`, `jmh.streamer`, `jmh.sql`, `jmh.thin`, `jmh.compute` and
`jmh.sequence` are made for the allocation profilers. Each starts two server nodes and calls Ignite
from a client node, from a server with `client=SERVER`, or, in the thin client and JDBC thin
benchmarks, from a client connected to the servers' client connectors. Each creates the keys, values
and other arguments of its calls before measuring. The benchmarks of a cache write every key first,
so their operations work on existing entries.

| Class | Methods | What it measures |
|---|---|---|
| `JmhCacheAllocationBenchmark` | `put`, `get`, `getAndPut`, `putIfAbsent`, `replace`, `putRemove`, `invoke`, `putAsync`, `getAsync` | Single-key operations. `putIfAbsent` finds the key and writes nothing; `putRemove` inserts a key the setup didn't write and removes it; `invoke` runs an entry processor that writes the value back |
| `JmhCacheBatchAllocationBenchmark` | `putAll`, `getAll` | Operations on a batch of keys |
| `JmhCacheTypesAllocationBenchmark` | `put`, `get`, `getAll`, `invoke` | Put, get, `getAll` of 10 consecutive keys and an entry processor that writes the value back, by key type, value type and value size. With the histogram profiler, the copies of a `byte[]` value show as rows of its size range |
| `JmhCacheTxAllocationBenchmark` | `txPut` | Puts `txKeys` consecutive keys in an explicit transaction |
| `JmhCacheQueryAllocationBenchmark` | `scanQuery`, `indexQuery` | A scan query of one partition, about a hundred entries, and an index query of one entry by its value's indexed field |
| `JmhStreamerAllocationBenchmark` | `addData` | A data streamer adds 10 000 keys the cache doesn't have, then flushes. Each iteration is one such call (single shot), results are per key, and the cache grows by 10 000 keys per iteration. The client's streamer threads and, with `allowOverwrite=true` and `PRIMARY_SYNC`, the backups finish a batch after the flush returns, so each iteration ends with a 500 ms wait outside the timed call |
| `JmhSqlAllocationBenchmark` | `selectByKey`, `selectRange`, `update`, `insertDelete` | SQL through a node's cache API: a select of one row by key, a select of 10 rows by a range of the indexed field `id`, an update of `id` to the value it has, and an insert of a row the setup didn't write followed by its delete. The queries and their arguments are created in setup |
| `JmhThinClientAllocationBenchmark` | `put`, `get`, `putAll`, `getAll`, `sqlSelect` | Cache operations through the thin client, `putAll` and `getAll` of 10 consecutive keys, and an SQL select of one row by key |
| `JmhThinClientTxAllocationBenchmark` | `txPut` | Puts of two consecutive keys in a pessimistic repeatable-read thin client transaction |
| `JmhJdbcThinAllocationBenchmark` | `selectByKey`, `selectRange`, `update`, `insertDelete` | The queries of `JmhSqlAllocationBenchmark` through the JDBC thin driver, with statements prepared in setup, on the default engine, H2 |
| `JmhContinuousQueryAllocationBenchmark` | `putWithListener`, `putWithFilter` | A put to a cache with a continuous query of the calling node: with a local listener, which gets every update, or with a remote filter that drops every update on the servers |
| `JmhComputeAllocationBenchmark` | `run`, `call`, `affinityCall`, `execute`, `broadcast`, `thinExecute` | Compute to the servers: a job on one server, a job that returns a value, a job on the primary server of a key, a task of one job, a job on each server, and the task started through the thin client. The jobs only carry their argument and return it. The thin client's task also allocates in the JDK's common pool, the group `other.ForkJoinPool.commonPool-worker` |
| `JmhServiceAllocationBenchmark` | `call`, `thinCall` | A call of a service deployed on each server, through the client node's proxy and through the thin client's |
| `JmhMessagingAllocationBenchmark` | `send`, `sendOrdered` | A user message and an ordered one from the client node to one server. Each call waits until the server's listener has the message: sends that don't wait pile up in the queues and break the connection |
| `JmhDataStructuresAllocationBenchmark` | `atomicLongIncrement`, `atomicSequenceIncrement`, `atomicReferenceCompareAndSet`, `queueOfferPoll`, `setAddRemove`, `semaphoreAcquireRelease`, `lockUnlock` | Operations of the data structures, called from the client node. The atomic sequence reserves 1 000 values at a time by default, so one increment in 1 000 goes to the servers |

Each parameter has one default value, and a run without `-p` uses it. `-p <name>=<value>` sets
another value, `-p <name>=<value>,<value>` runs several. The parameters of a base class, such as
`JmhCacheAllocationAbstractBenchmark` or `JmhThinClientAllocationAbstractBenchmark`, apply to every
class that extends it.

| Parameter | Classes | Default | Other values | What it changes |
|---|---|---|---|---|
| `client` | `JmhCacheAllocationAbstractBenchmark` | `THICK` | `SERVER`, `NEAR` | The node that calls the cache: a client node, a server node, or a client node with a near cache |
| `atomicity` | `JmhCacheAllocationBenchmark`, `JmhCacheBatchAllocationBenchmark`, `JmhCacheTypesAllocationBenchmark` | `ATOMIC` | `TRANSACTIONAL` | Atomicity mode. `JmhCacheTxAllocationBenchmark` always uses `TRANSACTIONAL` |
| `backups` | `JmhCacheAllocationAbstractBenchmark` | `1` | `0` | Backups |
| `writeSync` | `JmhCacheAllocationAbstractBenchmark` | `PRIMARY_SYNC` | `FULL_SYNC`, `FULL_ASYNC` | Write synchronization mode |
| `cacheMode` | `JmhCacheAllocationAbstractBenchmark` | `PARTITIONED` | `REPLICATED` | Cache mode. With two servers and `backups=1`, both modes keep every key on both servers |
| `copyOnRead` | `JmhCacheAllocationAbstractBenchmark` | `true` | `false` | Whether reads copy a value kept on heap, used with `onHeap=true` |
| `onHeap` | `JmhCacheAllocationAbstractBenchmark` | `false` | `true` | On-heap cache |
| `storage` | `JmhCacheAllocationAbstractBenchmark` | `IN_MEMORY` | `PERSISTENT` | Ignite Persistence on the servers |
| `walMode` | `JmhCacheAllocationAbstractBenchmark` | `LOG_ONLY` | `FSYNC`, `BACKGROUND` | WAL mode, used with `storage=PERSISTENT` or `cdc=true` |
| `cdc` | `JmhCacheAllocationAbstractBenchmark` | `false` | `true` | Change data capture on the data region |
| `sqlIdx` | `JmhCacheAllocationBenchmark` | `false` | `true` | An SQL index on the value's field |
| `batch` | `JmhCacheBatchAllocationBenchmark` | `10` | `1`, `100`, `1000` | Keys in a batch |
| `keyType` | `JmhCacheTypesAllocationBenchmark` | `INT` | `STRING`, `UUID`, `AFFINITY_POJO`, `BYTES` | Key class: `Integer`, `String`, `UUID`, a class with an affinity key field, `byte[16]` |
| `valType` | `JmhCacheTypesAllocationBenchmark` | `POJO` | `BYTE`, `SHORT`, `INT`, `LONG`, `FLOAT`, `DOUBLE`, `CHAR`, `BOOLEAN`, `STRING`, `STRING_CYRILLIC`, `UUID`, `BIG_DECIMAL`, `DATE`, `TIMESTAMP`, `BYTES`, `INTS`, `LONGS`, `OBJECTS`, `LIST`, `MAP`, `POJO8`, `NESTED_POJO`, `BINARYLIZABLE`, `EXTERNALIZABLE`, `ENUM`, `BINARY_OBJECT` | Value class, see `JmhCacheTypesAllocationBenchmark.ValueType` |
| `valSize` | `JmhCacheTypesAllocationBenchmark` | `100` | `10`, `1000`, `10000`, `100000` | Length of string, array and collection values. For every value type, it also sets how many keys the setup writes |
| `concurrency` | `JmhCacheTxAllocationBenchmark` | `PESSIMISTIC` | `OPTIMISTIC` | Transaction concurrency |
| `isolation` | `JmhCacheTxAllocationBenchmark` | `REPEATABLE_READ` | `READ_COMMITTED`, `SERIALIZABLE` | Transaction isolation |
| `txKeys` | `JmhCacheTxAllocationBenchmark` | `2` | `1`, `10` | Keys per transaction |
| `allowOverwrite` | `JmhStreamerAllocationBenchmark` | `false` | `true` | Whether the streamer writes every key as a put does, or only the keys the cache doesn't have |
| `engine` | `JmhSqlAllocationBenchmark` | `H2` | `CALCITE` | SQL query engine |
| `valType` | `JmhSqlAllocationBenchmark`, `JmhThinClientAllocationBenchmark`, `JmhThinClientTxAllocationBenchmark`, `JmhJdbcThinAllocationBenchmark` | `POJO` | `POJO8` | Value class, and so the table's columns: one `int` field, or eight fields of different types, see `SqlValueType` |
| `partitionAwareness` | `JmhThinClientAllocationAbstractBenchmark` | `true` | `false` | Whether the thin client or the JDBC thin driver sends a key's request to the key's primary server. With `true` it connects to both servers, with `false` to the first one. The driver's own default is `false` |
| `client` | `JmhComputeAllocationBenchmark` | `THICK` | `SERVER` | The node that calls compute: a client node or a server. A server sends its jobs to both servers, so about half of them run on the calling server, without messages. `thinExecute` doesn't depend on it |
| `argSize` | `JmhComputeAllocationBenchmark` | `0` | `100`, `10000`, `100000` | Length of a `byte[]` in each job's argument and, in the calls that return a value, in the result. With `0` the jobs carry `null` |

- With `storage=PERSISTENT` or `cdc=true`, the servers write a WAL; each node preallocates several
  hundred megabytes of WAL segments. The nodes keep their data, WAL and change data capture files in
  a new temporary directory, `ignite-jmh-*`, which the benchmark deletes when it stops the nodes,
  so a persistent run never starts on the data of an earlier one. A run that fails before its nodes
  stop leaves the directory: delete it by hand. Binary type metadata, a few hundred bytes per node,
  stays in the nodes' work directory with their logs.
- With `storage=PERSISTENT`, the first write to a page after a checkpoint logs a copy of the whole
  page to the WAL, and later writes log only the change. Writes allocate a few KB more per server
  until most pages have been written since the last checkpoint. With the default `walMode` this
  takes under a second for a key-value put and about 4 seconds for the SQL `update` of
  `JmhSqlAllocationBenchmark`. With `walMode=FSYNC`, at a few hundred writes per second, it takes
  tens of seconds or more, and the next checkpoint, 3 minutes later by default, starts it again: warm
  up and measure for minutes. With so few operations per iteration, the last one also shows the
  start of the teardown, about 20 KB in the benchmark thread.
- `JmhCacheTypesAllocationBenchmark` writes fewer keys when `valSize` is large, so the data the
  setup writes stays about the same size.

## Writing a benchmark

- Extend the base class of the area: `JmhCacheAbstractBenchmark`, `JmhSqlAbstractBenchmark`,
  `JmhThinClientAbstractBenchmark`, or `JmhAbstractBenchmark` for the rest. A cache benchmark for
  the allocation profilers extends `JmhCacheAllocationAbstractBenchmark`, a thin client or JDBC thin
  one `JmhThinClientAllocationAbstractBenchmark`, which bring the parameters above. Give each new
  parameter one default value in `@Param` and list the others in this README.
- Start nodes in the benchmark JVM and give each one a name: the allocation profilers find a node's
  threads by its name.
- Call `JmhNodeStopHook.run()` first in the teardown that stops the nodes. JMH runs this teardown
  before profilers read the last iteration, and the hook lets them read it while the nodes still run.
  Without the hook, both allocation profilers fail the benchmark.
- Close thin clients and JDBC connections in that teardown too, after the hook. JMH runs the teardown
  of a helper `@State` before it, and a thin client closed there stops its threads before the
  counter profiler reads them, which drops their last iteration.
- Create keys and values outside the measured method when they aren't part of the operation.
  Otherwise their cost and allocation count as Ignite's.
- Bring the cache to the state of a cluster that has run for a while before measuring. Ignite boxes
  partition update counters, and `Long.valueOf` returns cached objects up to 127, so in a freshly
  loaded cache an update allocates less, and its allocation grows during the run.
  `JmhCacheAbstractBenchmark#populate` writes the keys as many times as that takes.
- To run a benchmark from the IDE, give it a `main()` that runs it through `JmhIdeBenchmarkRunner`,
  which passes the JVM flags Ignite needs.

For writing JMH benchmarks in general, start with the
[JMH samples](https://github.com/openjdk/jmh/tree/master/jmh-samples/src/main/java/org/openjdk/jmh/samples).
