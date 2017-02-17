Yardstick Ignite Benchmarks
===========================
Apache Ignite benchmarks are written on top of Yardstick Framework (https://github.com/gridgain/yardstick) and allow
you to measure performance of various Apache Ignite components and modules.

The documentation below describes how to execute and configure already assembled benchmarks. If you need to add new
benchmarks or build existing one then refer to instruction from `DEVNOTES.txt` in source directory.

Visit Yardstick Repository (https://github.com/gridgain/yardstick) for detailed information in regards resulting graphs
generation and how the frameworks works.


Running Ignite Benchmarks Locally
==========================================

The simplest way to start with benchmarking is to use one of the executable scripts available under `benchmarks\bin` directory:

./bin/benchmark-run-all.sh config/benchmark-atomic-put.properties

The command above will benchmark the cache put operation for a distributed atomic cache. The results of the benchmark will be added
to auto-generated `results-{DATE-TIME}` directory.

If `./bin/benchmark-run-all.sh` command is executed as is without any parameters and modifications in configurations files then
all the available benchmarks will be executed on a local machine using `config/benchmark.properties` configuration.

To get more information about available benchmarks and configuration parameters refer to “Provided Benchmarks” and
“Properties And Command Line Arguments” sections below.


Running Ignite Benchmarks Remotely
=========================================

For running Ignite benchmarks on remote hosts you need to upload Ignite-Yardstick to each one of your remote hosts.

NOTE: The path to the uploaded Ignite-Yardstick should be exactly the same on each host.

Then you need to make some changes in config files:

1. You need to comment or delete the
        <property name="localHost" value="127.0.0.1"/>
line in `config/ignite-localhost-config.xml` file.

2. You need to replace all the `127.0.0.1` addresses in `ignite-localhost-config.xml` file by actual IPs of your remote
servers. You can add or delete lines with IP addresses if you want to run benchmarks on different number of hosts.
There must be at least one IP address in the list.

3. You need to replace the `localhost` strings by actual IP of your servers in the lines
SERVERS='localhost,localhost'
DRIVERS='localhost'
in `config/benchmark-atomic-put.properties` file.

Then use the following command:

./bin/benchmark-run-all.sh config/benchmark-atomic-put.properties


It is recommended to create some copies of original config files, edit these copies and then use as a
parameter for `./bin/benchmark-run-all.sh` script.


Provided Benchmarks
===================
The following benchmarks are provided:

1.  `GetBenchmark` - benchmarks atomic distributed cache get operation
2.  `PutBenchmark` - benchmarks atomic distributed cache put operation
3.  `PutGetBenchmark` - benchmarks atomic distributed cache put and get operations together
4.  `PutTxBenchmark` - benchmarks transactional distributed cache put operation
5.  `PutGetTxBenchmark` - benchmarks transactional distributed cache put and get operations together
6.  `SqlQueryBenchmark` - benchmarks distributed SQL query over cached data
7.  `SqlQueryJoinBenchmark` - benchmarks distributed SQL query with a Join over cached data
8.  `SqlQueryPutBenchmark` - benchmarks distributed SQL query with simultaneous cache updates
9.  `AffinityCallBenchmark` - benchmarks affinity call operation
10. `ApplyBenchmark` - benchmarks apply operation
11. `BroadcastBenchmark` - benchmarks broadcast operations
12. `ExecuteBenchmark` - benchmarks execute operations
13. `RunBenchmark` - benchmarks running task operations
14. `PutGetOffHeapBenchmark` - benchmarks atomic distributed cache put and get operations together off heap
15. `PutGetOffHeapValuesBenchmark` - benchmarks atomic distributed cache put value operations off heap
16. `PutOffHeapBenchmark` - benchmarks atomic distributed cache put operations off heap
17. `PutOffHeapValuesBenchmark` - benchmarks atomic distributed cache get value operations off heap
18. `PutTxOffHeapBenchmark` - benchmarks transactional distributed cache put operation off heap
19. `PutTxOffHeapValuesBenchmark` - benchmarks transactional distributed cache put value operation off heap
20. `SqlQueryOffHeapBenchmark` -benchmarks distributed SQL query over cached data off heap
21. `SqlQueryJoinOffHeapBenchmark` - benchmarks distributed SQL query with a Join over cached data off heap
22. `SqlQueryPutOffHeapBenchmark` - benchmarks distributed SQL query with simultaneous cache updates off heap
23. `PutAllBenchmark` - benchmarks atomic distributed cache batch put operation
24. `PutAllTxBenchmark` - benchmarks transactional distributed cache batch put operation


Properties And Command Line Arguments
=====================================
Note that this section only describes configuration parameters specific to Ignite benchmarks,
and not for Yardstick framework. To run Ignite benchmarks and generate graphs, you will need to run them using
Yardstick framework scripts in `bin` folder.

Refer to Yardstick Documentation (https://github.com/gridgain/yardstick) for common Yardstick properties
and command line arguments for running Yardstick scripts.

The following Ignite benchmark properties can be defined in the benchmark configuration:

* `-b <num>` or `--backups <num>` - Number of backups for every key
* `-cfg <path>` or `--Config <path>` - Path to Ignite configuration file
* `-cs` or `--cacheStore` - Enable or disable cache store readThrough, writeThrough
* `-cl` or `--client` - Client flag
* `-nc` or `--nearCache` - Near cache flag
* `-nn <num>` or `--nodeNumber <num>` - Number of nodes (automatically set in `benchmark.properties`), used to wait for
    the specified number of nodes to start
* `-sm <mode>` or `-syncMode <mode>` - Synchronization mode (defined in `CacheWriteSynchronizationMode`)
* `-ot` or `--offheapTiered` - Flag indicating whether tiered off-heap mode is on
* `-ov` or `--offheapValuesOnly` - Flag indicating whether off-heap mode is on and only cache values are stored off-heap
* `-r <num>` or `--range` - Range of keys that are randomly generated for cache operations
* `-rd or --restartdelay` - Restart delay in seconds
* `-rs or --restartsleep` - Restart sleep in seconds
* `-rth <host>` or `--restHost <host>` - REST TCP host
* `-rtp <num>` or `--restPort <num>` - REST TCP port, indicates that a Ignite node is ready to process Ignite Clients
* `-ss` or `--syncSend` - Flag indicating whether synchronous send is used in `TcpCommunicationSpi`
* `-txc <value>` or `--txConcurrency <value>` - Cache transaction concurrency control, either `OPTIMISTIC` or
    `PESSIMISTIC` (defined in `CacheTxConcurrency`)
* `-txi <value>` or `--txIsolation <value>` - Cache transaction isolation (defined in `CacheTxIsolation`)
* `-wb` or `--writeBehind` - Enable or disable writeBehind for cache store
* `-wom <mode>` or `--writeOrderMode <mode>` - Write order mode for ATOMIC caches (defined in `CacheAtomicWriteOrderMode`)

For example if we need to run 2 `IgniteNode` servers on localhost with `PutBenchmark` benchmark on localhost,
with number of backups set to 1, synchronization mode set to `PRIMARY_SYNC`, then the following configuration
should be specified in `benchmark.properties` file:

```
SERVER_HOSTS=localhost,localhost
...

Note that -dn and -sn, which stand for data node and server node, are native Yardstick parameters and are documented in Yardstick framework.
===========================================================================================================================================

CONFIGS="-b 1 -sm PRIMARY_SYNC -dn PutBenchmark`IgniteNode"
```

Issues
======
Use Ignite Apache JIRA (https://issues.apache.org/jira/browse/IGNITE) to file bugs.

License
=======
Yardstick Ignite is available under Apache 2.0 (http://www.apache.org/licenses/LICENSE-2.0.html) Open Source license.
