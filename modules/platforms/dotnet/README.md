# Apache Ignite.NET In-Memory Data Fabric

<img src="https://ignite.apache.org/images/logo3.png" hspace="20" /><img src="https://ptupitsyn.github.io/images/net-framework.png" hspace="20" />

[![Join the chat at https://gitter.im/apacheignite/ignite](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/apacheignite/ignite?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

<a href="https://www.nuget.org/packages?q=Apache.Ignite"><img src="https://img.shields.io/nuget/v/Apache.Ignite.svg" /></a>

<a href="https://www.myget.org/gallery/apache-ignite-net-nightly"><img src="https://img.shields.io/myget/apache-ignite-net-nightly/vpre/Apache.Ignite.svg" /></a>

<a href="http://ci.ignite.apache.org/viewType.html?buildTypeId=IgniteTests_IgnitePlatformNet&branch_IgniteTests=%3Cdefault%3E"><img src="http://ci.ignite.apache.org/app/rest/builds/buildType:(id:IgniteTests_IgnitePlatformNet)/statusIcon" /></a>

Apache Ignite In-Memory Data Fabric is designed to deliver uncompromised performance for a wide set of in-memory computing use cases from 
[high performance computing](https://ignite.apache.org/features.html), to the industry most advanced [data grid](https://ignite.apache.org/features.html), 
highly available [service grid](https://ignite.apache.org/features.html), and [streaming](https://ignite.apache.org/features.html).

## Advanced Clustering

Ignite nodes can automatically discover each other. This helps to scale the cluster when needed, without having to restart the whole cluster.
Developers can also leverage from Ignite’s hybrid cloud support that allows establishing connection between private cloud and public clouds
such as Amazon Web Services, providing them with best of both worlds.

<p align="center">
    <a href="https://apacheignite.readme.io/docs/cluster">
        <img src="https://ignite.apache.org/images/advanced-clustering.png" />
    </a>
</p>

## Data Grid (Cache)

Ignite data grid is an in-memory distributed key-value store which can be viewed as a distributed partitioned `Dictionary`, with every cluster node
owning a portion of the overall data. This way the more cluster nodes we add, the more data we can cache.

Unlike other key-value stores, Ignite determines data locality using a pluggable hashing algorithm. Every client can determine which node a key
belongs to by plugging it into a hashing function, without a need for any special mapping servers or name nodes.

Ignite data grid supports local, replicated, and partitioned data sets and allows to freely cross query between these data sets using standard SQL and LINQ syntax.
Ignite supports standard SQL and LINQ for querying in-memory data including support for distributed joins.

<p align="center">
    <a href="https://apacheignite.readme.io/docs/data-grid">
        <img src="https://ignite.apache.org/images/in-memory-data-grid.jpg" />
    </a>
</p>

Our data grid offers many features, some of which are:

* Primary & Backup Copies.
* Near Caches.
* Cache queries and SQL/LINQ queries.
* Continuous Queries.
* Transactions.
* Off-Heap Memory.
* Affinity Collocation.
* Persistent Store.
* Automatic Persistence.
* Data Loading.
* Eviction and Expiry Policies.
* Data Rebalancing
* Topology Validation.

## Streaming & CEP

Ignite streaming allows to process continuous never-ending streams of data in scalable and fault-tolerant fashion. The rates at which data can be injected into Ignite can be very high and easily exceed millions of events per second on a moderately sized cluster.

Real-time data is ingested via data streamers. We offer streamers for JMS 1.1, Apache Kafka, MQTT, Twitter, Apache Flume and Apache Camel already, and we keep adding new ones every release.

<p align="center">
    <a href="https://apacheignite.readme.io/docs/streaming--cep">
        <img src="https://ignite.apache.org/images/data-streamers.png" />
    </a>
</p>

The data can then be queried within sliding windows, if needed:

<p align="center">
    <a href="https://apacheignite.readme.io/docs/streaming--cep">
        <img src="https://ignite.apache.org/images/sliding-event-window.png" />
    </a>
</p>

## Compute Grid

Distributed computations are performed in parallel fashion to gain high performance, low latency, and linear scalability. Ignite compute grid provides a set of simple APIs that allow users distribute computations and data processing across multiple computers in the cluster. Distributed parallel processing is based on the ability to take any computation and execute it on any set of cluster nodes and return the results back.

<p align="center">
    <a href="https://apacheignite.readme.io/docs/compute-grid">
        <img src="https://ignite.apache.org/images/in_memory_compute.png" />
    </a>
</p>

We support these features, amongst others:

* Distributed Closure Execution.
* MapReduce & ForkJoin Processing.
* Clustered Executor Service.
* Collocation of Compute and Data.
* Load Balancing.
* Fault Tolerance.
* Job State Checkpointing.
* Job Scheduling.

## Service Grid

Service Grid allows for deployments of arbitrary user-defined services on the cluster. You can implement and deploy any service, such as custom counters, ID generators, hierarchical maps, etc.

Ignite allows you to control how many instances of your service should be deployed on each cluster node and will automatically ensure proper deployment and fault tolerance of all the services.

<p align="center">
    <a href="https://apacheignite.readme.io/docs/service-grid">
        <img src="https://ignite.apache.org/images/ignite_service.png" vspace="15"/>
    </a>
</p>

## Distributed Data Structures

Ignite.NET supports complex data structures in a distributed fashion: `AtomicLong`, `AtomicReference`, `AtomicSequence`.

## Distributed Messaging

Distributed messaging allows for topic based cluster-wide communication between all nodes. Messages with a specified message topic can be distributed to
all or sub-group of nodes that have subscribed to that topic.

Ignite messaging is based on publish-subscribe paradigm where publishers and subscribers are connected together by a common topic.
When one of the nodes sends a message A for topic T, it is published on all nodes that have subscribed to T.

## Distributed Events

Distributed events allow applications to receive notifications when a variety of events occur in the distributed grid environment.
You can automatically get notified for task executions, read, write or query operations occurring on local or remote nodes within the cluster.

## ASP.NET Integration

Ignite.NET provides [Output Cache](https://apacheignite-net.readme.io/docs/aspnet-output-caching) and [Session State](https://apacheignite-net.readme.io/docs/aspnet-session-state-caching) clustering: boost performance by storing cache and session data in Ignite distributed cache. 

## Entity Framework Integration

Ignite.NET [Entity Framework Second Level Cache](https://apacheignite-net.readme.io/docs/entity-framework-second-level-cache) improves Entity Framework performance by storing query results in Ignite caches. 

## Getting Started

For information on how to get started with Apache Ignite please visit: [Getting Started][getting-started].

## Full Documentation

You can find the full Apache Ignite documentation here: [Full documentation][docs].

[apache-homepage]: https://ignite.apache.org/
[getting-started]: https://apacheignite-net.readme.io/docs/getting-started-2
[docs]: https://apacheignite-net.readme.io/docs

## Contribute to Ignite.NET

See [Ignite.NET Development](https://cwiki.apache.org/confluence/display/IGNITE/Ignite.NET+Development) on wiki.