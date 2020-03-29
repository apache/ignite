# Apache Ignite

<a href="https://ignite.apache.org/"><img src="https://ignite.apache.org/images/logo3.png" hspace="20"/></a>

<a href="https://ci.ignite.apache.org/viewType.html?buildTypeId=IgniteTests24Java8_RunAll&branch_IgniteTests24Java8=%3Cdefault%3E"><img src="https://ci.ignite.apache.org/app/rest/builds/buildType:(id:IgniteTests24Java8_RunAll)/statusIcon.svg"/></a>


## Getting Started

For information on how to get started with Apache Ignite, please visit: [Getting Started][getting-started].

## Full Documentation

You can find the full Apache Ignite documentation here: [Full documentation][docs].


## What is Apache Ignite?

[Apache Ignite][apache-ignite-homepage] is a memory-centric distributed <strong>database</strong>, <strong>caching</strong>,
 and <strong>processing</strong> platform for transactional, analytical, and streaming workloads delivering in-memory
 speeds at petabyte scale.

<p align="center">
    <a href="https://ignite.apache.org/whatisignite.html">
        <img src="https://ignite.apache.org/images/ignite_architecture.png" width="400px"/>
    </a>
</p>

## Memory-Centric Storage
Apache Ignite is based on distributed memory-centric architecture that combines the performance and scale of in-memory
computing together with the disk durability and strong consistency in one system.

The main difference between the memory-centric approach and the traditional disk-centric approach is that the memory
is treated as a fully functional storage, not just as a caching layer, like most databases do.
For example, Apache Ignite can function in a pure in-memory mode, in which case it can be treated as an
In-Memory Database (IMDB) and In-Memory Data Grid (IMDG) in one.

On the other hand, when persistence is turned on, Ignite begins to function as a memory-centric system where most of
the processing happens in memory, but the data and indexes get persisted to disk. The main difference here
from the traditional disk-centric RDBMS or NoSQL system is that Ignite is strongly consistent, horizontally
scalable, and supports both SQL and key-value processing APIs.

[Read More](https://ignite.apache.org/arch/memorycentric.html)

## Ignite Persistence

Ignite Native Persistence is a distributed, ACID, and SQL-compliant **disk store** that transparently integrates with
Ignite memory-centric storage as an optional disk layer storing data and indexes on SSD,
 Flash, 3D XPoint, and other types of non-volatile storages.

With the Ignite Persistence enabled, you no longer need to keep all the data and indexes in memory or warm it
up after a node or cluster restart because the Durable Memory is tightly coupled with persistence and treats
it as a secondary memory tier. This implies that if a subset of data or an index is missing in RAM,
the Durable Memory will take it from the disk.

<p align="center">
    <a href="https://apacheignite.readme.io/docs/distributed-persistent-store">
        <img src="https://ignite.apache.org/images/native_persistence.png?renew" width="400px"/>
    </a>
</p>

[Read More](https://ignite.apache.org/arch/persistence.html)

## ACID Compliance
Data stored in Ignite is ACID-compliant both in memory and on disk, making Ignite a **strongly consistent** system. Ignite transactions work across the network and can span multiple servers.

[Read More](https://apacheignite.readme.io/docs/transactions)

## Complete SQL Support
Ignite provides full support for SQL, DDL and DML, allowing users to interact with Ignite using pure SQL without writing any code. This means that users can create tables and indexes as well as insert, update, and query data using only SQL. Having such complete SQL support makes Ignite a one-of-a-kind **distributed SQL database**.

[Read More](https://apacheignite.readme.io/docs/distributed-sql)

## Key-Value
The in-memory data grid component in Ignite is a fully transactional **distributed key-value store** that can scale horizontally across 100s of servers in the cluster. When persistence is enabled, Ignite can also store more data than fits in memory and survive full cluster restarts.

[Read More](https://apacheignite.readme.io/docs/data-grid)

## Collocated Processing
Most traditional databases work in a client-server fashion, meaning that data must be brought to the client side for processing. This approach requires lots of data movement from servers to clients and generally does not scale. Ignite, on the other hand, allows for sending light-weight computations to the data, i.e. **collocating** computations with data. As a result, Ignite scales better and minimizes data movement.

[Read More](https://apacheignite.readme.io/docs/collocate-compute-and-data)

## Scalability and Durability
Ignite is an elastic, horizontally scalable distributed system that supports adding and removing cluster nodes on demand. Ignite also allows for storing multiple copies of the data, making it resilient to partial cluster failures. If the persistence is enabled, then data stored in Ignite will also survive full cluster failures. Cluster restarts in Ignite can be very fast, as the data becomes operational instantaneously directly from disk. As a result, the data does not need to be preloaded in-memory to begin processing, and Ignite caches will lazily warm up resuming the in memory performance.

[Read More](https://apacheignite.readme.io/docs/clustering)