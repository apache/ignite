Yardstick Ignite Benchmarks
===========================
Yardstick Ignite is a set of Ignite Grid (http://ignite.apache.org/) benchmarks written on top of Yardstick framework.

Yardstick Framework
===================
Visit Yardstick Repository (https://github.com/gridgain/yardstick) for detailed information on how to run Yardstick benchmarks and how to generate graphs.

The documentation below describes configuration parameters in addition to standard Yardstick parameters.

Installation
============
1. Create a local clone of Ignite repository
2. Run

mvn package

command for Yardstick Ignite POM

Provided Benchmarks
===================
The following benchmarks are provided:

1. `GetBenchmark` - benchmarks atomic distributed cache get operation
2. `PutBenchmark` - benchmarks atomic distributed cache put operation
3. `PutGetBenchmark` - benchmarks atomic distributed cache put and get operations together
4. `PutTxBenchmark` - benchmarks transactional distributed cache put operation
5. `PutGetTxBenchmark` - benchmarks transactional distributed cache put and get operations together
6. `SqlQueryBenchmark` - benchmarks distributed SQL query over cached data
7. `SqlQueryJoinBenchmark` - benchmarks distributed SQL query with a Join over cached data
8. `SqlQueryPutBenchmark` - benchmarks distributed SQL query with simultaneous cache updates

Writing Ignite Benchmarks
=========================
All benchmarks extend `AbstractBenchmark` class. A new benchmark should also extend this abstract class
and implement `test` method. This is the method that is actually benchmarked.

Running Ignite Benchmarks
=========================
Before running Ignite benchmarks, run:

mvn package

command. This command will compile the project and also will unpack scripts from `yardstick-resources.zip` file to `bin` directory.

Properties And Command Line Arguments
=====================================
Note that this section only describes configuration parameters specific to Ignite benchmarks,
and not for Yardstick framework. To run Ignite benchmarks and generate graphs, you will need to run them using
Yardstick framework scripts in `bin` folder.

Refer to Yardstick Documentation (https://github.com/gridgain/yardstick) for common Yardstick properties
and command line arguments for running Yardstick scripts.

The following Ignite benchmark properties can be defined in the benchmark configuration:

* `-nn <num>` or `--nodeNumber <num>` - Number of nodes (automatically set in `benchmark.properties`), used to wait for the specified number of nodes to start
* `-b <num>` or `--backups <num>` - Number of backups for every key
* `-cfg <path>` or `--Config <path>` - Path to Ignite configuration file
* `-sm <mode>` or `-syncMode <mode>` - Synchronization mode (defined in `CacheWriteSynchronizationMode`)
* `-cl` or `--client` - Client flag
* `-nc` or `--nearCache` - Near cache flag
* `-wom <mode>` or `--writeOrderMode <mode>` - Write order mode for ATOMIC caches (defined in `CacheAtomicWriteOrderMode`)
* `-txc <value>` or `--txConcurrency <value>` - Cache transaction concurrency control, either `OPTIMISTIC` or `PESSIMISTIC` (defined in `CacheTxConcurrency`)
* `-txi <value>` or `--txIsolation <value>` - Cache transaction isolation (defined in `CacheTxIsolation`)
* `-ot` or `--offheapTiered` - Flag indicating whether tiered off-heap mode is on
* `-ov` or `--offheapValuesOnly` - Flag indicating whether off-heap mode is on and only cache values are stored off-heap
* `-rtp <num>`  or `--restPort <num>` - REST TCP port, indicates that a Ignite node is ready to process Ignite Clients
* `-rth <host>` or `--restHost <host>` - REST TCP host
* `-ss` or `--syncSend` - Flag indicating whether synchronous send is used in `TcpCommunicationSpi`
* `-r <num>` or `--range` - Range of keys that are randomly generated for cache operations

For example if we need to run 2 `IgniteNode` servers on localhost with `PutBenchmark` benchmark on localhost,
with number of backups set to 1, synchronization mode set to `PRIMARY_SYNC`, then the following configuration
should be specified in `benchmark.properties` file:

```
SERVER_HOSTS=localhost,localhost
...

Note that -dn and -sn, which stand for data node and server node, are native Yardstick parameters and are documented in Yardstick framework.
===========================================================================================================================================

CONFIGS="-b 1 -sm PRIMARY_SYNC -dn PutBenchmark -sn IgniteNode"
```

Issues
======
Use Ignite Apache JIRA (https://issues.apache.org/jira/browse/IGNITE) to file bugs.

License
=======
Yardstick Ignite is available under Apache 2.0 (http://www.apache.org/licenses/LICENSE-2.0.html) Open Source license.
