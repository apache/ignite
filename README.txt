Apache Ignite In-Memory Data Fabric
===================================

Ignite In-Memory Data Fabric is designed to deliver high performance for the widest
array of in-memory computing use cases.

The main feature set of Ignite In-Memory Data Fabric includes:
* Advanced Clustering
* Compute Grid
* Data Grid
* Service Grid
* IGFS - Ignite File System
* Distributed Data Structures
* Distributed Messaging
* Distributed Events
* Streaming & CEP

For information on how to get started with Apache Ignite please visit:

    http://apacheignite.readme.io/v1.0/docs/getting-started


You can find Apache Ignite documentation here:

    http://apacheignite.readme.io/v1.0/docs/getting-started


LGPL dependencies
=================

Ignite includes the following optional LGPL dependencies:
 - Hibernate L2 Cache Integration, http://hibernate.org/orm/
 - JTS Topology Suite for Geospatial indexing, http://tsusiatsoftware.net/jts/main.html
 - cron4j for cron-based task scheduling, http://www.sauronsoftware.it/projects/cron4j

Apache binary releases cannot include LGPL dependencies. If you would like to include
optional LGPL dependencies into your release, you should download the source release
from Ignite website and do the build with the following maven command:

mvn clean package -DskipTests -P release,lgpl
