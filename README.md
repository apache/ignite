# Apache Ignite In-Memory Computing Platform

<a href="https://ignite.apache.org/"><img src="https://ignite.apache.org/images/logo3.png" hspace="20"/></a>

[![Join the chat at https://gitter.im/apacheignite/ignite](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/apacheignite/ignite?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

<a href="http://ci.ignite.apache.org/viewType.html?buildTypeId=IgniteTests_IgniteBasic&branch_IgniteTests=%3Cdefault%3E"><img src="http://ci.ignite.apache.org/app/rest/builds/buildType:(id:IgniteTests_IgniteBasic)/statusIcon" /></a>

[Apache Ignite][apache-homepage] is the <b>in-memory</b> computing platform
that is <b>durable</b>, <b>strongly consistent</b>, and <b>highly available</b>
with powerful <b>SQL</b>, <b>key-value</b> and <b>processing</b> APIs.

<p align="center">
    <a href="https://apacheignite.readme.io/docs">
        <img src="https://ignite.apache.org/images/durable-memory.png" width="900px"/>
    </a>
</p>

## Durable Memory
Ignite's durable memory component treats RAM not just as a caching layer but as a complete fully functional storage layer. This means that users can turn the persistence on and off as needed. If the persistence is off, then Ignite can act as a distributed **in-memory database** or **in-memory data grid**, depending on whether you prefer to use SQL or key-value APIs. If the persistence is turned on, then Ignite becomes a distributed, **horizontally scalable database** that guarantees full data consistency and is resilient to full cluster failures.

## ACID Compliance
Data stored in Ignite is ACID-compliant both in memory and on disk, making Ignite a **strongly consistent** system. Ignite transactions work across the network and can span multiple servers.

## Complete SQL Support
Ignite provides full support for SQL, DDL and DML, allowing users to interact with Ignite using pure SQL without writing any code. This means that users can create tables and indexes as well as insert, update, and query data using only SQL. Having such complete SQL support makes Ignite a one-of-a-kind **distributed SQL database**.

## Key-Value
The in-memory data grid component in Ignite is a fully transactional **distributed key-value store** that can scale horizontally across 100s of servers in the cluster. When persistence is enabled, Ignite can also store more data than fits in memory and survive full cluster restarts.

## Collocated Processing
Most traditional databases work in a client-server fashion, meaning that data must be brought to the client side for processing. This approach requires lots of data movement from servers to clients and generally does not scale. Ignite, on the other hand, allows for sending light-weight computations to the data, i.e. **collocating** computations with data. As a result, Ignite scales better and minimizes data movement.

## Scalability and Durability
Ignite is an elastic, horizontally scalable distributed system that supports adding and removing cluster nodes on demand. Ignite also allows for storing multiple copies of the data, making it resilient to partial cluster failures. If the persistence is enabled, then data stored in Ignite will also survive full cluster failures. Cluster restarts in Ignite can be very fast, as the data becomes operational instantaneously directly from disk. As a result, the data does not need to be preloaded in-memory to begin processing, and Ignite caches will lazily warm up resuming the in memory performance.

## Ignite Persistence

Ignite Native Persistence is a distributed ACID and SQL-compliant disk store that transparently integrates with Ignite's Durable Memory as an optional disk layer storing data and indexes on SSD, Flash, 3D XPoint, and other types of non-volatile storages.

With the Ignite Persistence enabled, you no longer need to keep all the data and indexes in memory or warm it up after a node or cluster restart because the Durable Memory is tightly coupled with persistence and treats it as a secondary memory tier. This implies that if a subset of data or an index is missing in RAM, the Durable Memory will take it from the disk.

<p align="center">
    <a href="https://apacheignite.readme.io/docs/distributed-persistent-store">
        <img src="https://ignite.apache.org/images/check_pointing-2.png" width="400px"/>
    </a>
</p>

## Ignite Facts

<b>Is Ignite a persistent or pure in-memory storage?</b><br/>
**Both**. Native persistence in Ignite can be turned on and off. This allows Ignite to store data sets bigger than can fit in the available memory. Essentially, the smaller operational data sets can be stored in-memory only, and larger data sets that do not fit in memory can be stored on disk, using memory as a caching layer for better performance.

[Read More](https://apacheignite.readme.io/docs/distributed-persistent-store)

<b>Is Ignite a distributed database?</b><br/>
**Yes**. Data in Ignite is either *partitioned* or *replicated* across a cluster of multiple nodes. This provides scalability and adds resilience to the system. Ignite automatically controls how data is partitioned, however, users can plug in their own distribution (affinity) functions and collocate various pieces of data together for efficiency.

[Read More](https://apacheignite.readme.io/docs/distributed-sql)

<b>Is Ignite a relational SQL database?</b><br/>
**Not fully**. Although Ignite aims to behave like any other relational SQL database, there are differences in how Ignite handles constraints and indexes. Ignite supports *primary* and *secondary* indexes, however, the *uniqueness* can only be enforced for the *primary* indexes. Ignite also does not support *foreign key* constraints.

Essentially, Ignite purposely does not support any constraints that would entail a cluster broadcast message for each update and significantly hurt performance and scalability of the system.

[Read More](https://apacheignite.readme.io/docs/indexes)

<b>Is Ignite an in-memory database?</b><br/>
**Yes**. Even though Ignite *durable memory* works well in-memory and on-disk, the disk persistence can be disabled and Ignite can act as a pure *in-memory database*.

[Read More](https://apacheignite.readme.io/docs/distributed-sql)

<b>Is Ignite a transactional database?</b><br/>
**Not fully**. ACID Transactions are supported, but only at *key-value* API level. Ignite also supports *cross-partition* transactions, which means that transactions can span keys residing in different partitions on different servers.

At *SQL* level Ignite supports *atomic*, but not yet *transactional* consistency. Ignite community plans to implement SQL transactions in version 2.2.

[Read More](https://apacheignite.readme.io/docs/sql-queries#known-limitations)

<b>Is Ignite a key-value store?</b><br/>
**Yes**. Ignite provides a feature rich key-value API, that is JCache (JSR-107) compliant and supports Java, C++, and .NET.

[Read More](https://apacheignite.readme.io/docs/data-grid)

<b>Is Ignite an in-memory data grid?</b><br/>
**Yes**. Ignite is a full-featured data grid, which can be used either in pure in-memory mode or with Ignite native persistence. It can also integrate with any 3rd party database, including any RDBMS or NoSQL store.

[Read More](https://apacheignite.readme.io/docs/data-grid)

<b>What is durable memory?</b><br/>
Ignite *durable memory* architecture allows Ignite to extend in-memory computing to disk. It is based on a paged-based off-heap memory allocator which becomes durable by persisting to the *write-ahead-log (WAL)* and, then, to main Ignite persistent storage. When persistence is disabled, durable memory acts like a pure in-memory storage.

[Read More](https://apacheignite.readme.io/docs/durable-memory)

<b>What is collocated processing?</b><br/>
Ignite is a distributed system and, therefore, it is important to be able to collocate data with data and compute with data to avoid distributed data noise. Data collocation becomes especially important when performing distributed SQL joins. Ignite also supports sending user logic (functions, lambdas, etc.) directly to the nodes where the data resides and computing on the data locally.

[Read More](https://apacheignite.readme.io/docs/collocate-compute-and-data)

## Ignite On Other Platforms

<a href="modules/platforms/dotnet">Ignite.NET</a>

<a href="modules/platforms/cpp">Ignite C++</a>

## Getting Started

For information on how to get started with Apache Ignite please visit: [Getting Started][getting-started].

## Full Documentation

You can find the full Apache Ignite documentation here: [Full documentation][docs].

[apache-homepage]: https://ignite.apache.org/
[getting-started]: https://apacheignite.readme.io/docs/getting-started
[docs]: https://apacheignite.readme.io/docs