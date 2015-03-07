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

Ignite provides three different modes of cache operation: `LOCAL`, `REPLICATED`, and `PARTITIONED`. A cache mode is configured for each cache. Cache modes are defined in `CacheMode` enumeration. 
[block:api-header]
{
  "type": "basic",
  "title": "Local Mode"
}
[/block]
`LOCAL` mode is the most light weight mode of cache operation, as no data is distributed to other cache nodes. It is ideal for scenarios where data is either read-only, or can be periodically refreshed at some expiration frequency. It also works very well with read-through behavior where data is loaded from persistent storage on misses. Other than distribution, local caches still have all the features of a distributed cache, such as automatic data eviction, expiration, disk swapping, data querying, and transactions.
[block:api-header]
{
  "type": "basic",
  "title": "Replicated Mode"
}
[/block]
In `REPLICATED` mode all data is replicated to every node in the cluster. This cache mode provides the utmost availability of data as it is available on every node. However, in this mode every data update must be propagated to all other nodes which can have an impact on performance and scalability. 

As the same data is stored on all cluster nodes, the size of a replicated cache is limited by the amount of memory available on the node with the smallest amount of RAM. This mode is ideal for scenarios where cache reads are a lot more frequent than cache writes, and data sets are small. If your system does cache lookups over 80% of the time, then you should consider using `REPLICATED` cache mode.
[block:callout]
{
  "type": "success",
  "body": "Replicated caches should be used when data sets are small and updates are infrequent."
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Partitioned Mode"
}
[/block]
`PARTITIONED` mode is the most scalable distributed cache mode. In this mode the overall data set is divided equally into partitions and all partitions are split equally between participating nodes, essentially creating one huge distributed in-memory store for caching data. This approach allows you to store as much data as can be fit in the total memory available across all nodes, hence allowing for multi-terabytes of data in cache memory across all cluster nodes. Essentially, the more nodes you have, the more data you can cache.

Unlike `REPLICATED` mode, where updates are expensive because every node in the cluster needs to be updated, with `PARTITIONED` mode, updates become cheap because only one primary node (and optionally 1 or more backup nodes) need to be updated for every key. However, reads become somewhat more expensive because only certain nodes have the data cached. 

In order to avoid extra data movement, it is important to always access the data exactly on the node that has that data cached. This approach is called *affinity colocation* and is strongly recommended when working with partitioned caches.
[block:callout]
{
  "type": "success",
  "body": "Partitioned caches are idea when working with large data sets and updates are frequent.",
  "title": ""
}
[/block]
The picture below illustrates a simple view of a partitioned cache. Essentially we have key K1 assigned to Node1, K2 assigned to Node2, and K3 assigned to Node3. 
[block:image]
{
  "images": [
    {
      "image": [
        "https://www.filepicker.io/api/file/7pGSgxCVR3OZSHqYLdJv",
        "in_memory_data_grid.png",
        "500",
        "338",
        "#d64304",
        ""
      ]
    }
  ]
}
[/block]
See [configuration](#configuration) section below for an example on how to configure cache mode.
[block:api-header]
{
  "type": "basic",
  "title": "Cache Distribution Mode"
}
[/block]
Node can operate in four different cache distribution modes when `PARTITIONED` mode is used. Cache distribution mode is defined by `CacheDistributionMode` enumeration and can be configured via `distributionMode` property of `CacheConfiguration`.
[block:parameters]
{
  "data": {
    "h-0": "Distribution Mode",
    "h-1": "Description",
    "0-0": "`PARTITIONED_ONLY`",
    "0-1": "Local node may store primary and/or backup keys, but does not cache recently accessed keys, which are neither primaries or backups, in near cache.",
    "1-0": "`CLIENT_ONLY`",
    "1-1": "Local node does not cache any data and communicates with other cache nodes via remote calls.",
    "2-0": "`NEAR_ONLY`",
    "2-1": "Local node will not be primary or backup node for any key, but will cache recently accessed keys in a smaller near cache. Amount of recently accessed keys to cache is controlled by near eviction policy.",
    "3-0": "`NEAR_PARTITIONED`",
    "3-1": "Local node may store primary and/or backup keys, and also will cache recently accessed keys in near cache. Amount of recently accessed keys to cache is controlled by near eviction policy."
  },
  "cols": 2,
  "rows": 4
}
[/block]
By default `PARTITIONED_ONLY` cache distribution mode is enabled. It can be turned on or off by setting the `distributionMode` configuration property in `CacheConfiguration`. For example:
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.IgniteConfiguration\">\n  \t...\n    <property name=\"cacheConfiguration\">\n        <bean class=\"org.apache.ignite.configuration.CacheConfiguration\">\n          \t<!-- Set a cache name. -->\n           \t<property name=\"name\" value=\"cacheName\"/>\n            \n          \t<!-- cache distribution mode. -->\n    \t\t\t\t<property name=\"distributionMode\" value=\"NEAR_ONLY\"/>\n    \t\t\t\t... \n        </bean\n    </property>\n</bean>",
      "language": "xml"
    },
    {
      "code": "CacheConfiguration cacheCfg = new CacheConfiguration();\n\ncacheCfg.setName(\"cacheName\");\n\ncacheCfg.setDistributionMode(CacheDistributionMode.NEAR_ONLY);\n\nIgniteConfiguration cfg = new IgniteConfiguration();\n\ncfg.setCacheConfiguration(cacheCfg);\n\n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Atomic Write Order Mode"
}
[/block]
When using partitioned cache in `CacheAtomicityMode.ATOMIC` mode, one can configure atomic cache write order mode. Atomic write order mode determines which node will assign write version (sender or primary node) and is defined by `CacheAtomicWriteOrderMode` enumeration. There are 2 modes, `CLOCK` and `PRIMARY`. 

In `CLOCK` write order mode, write versions are assigned on a sender node. `CLOCK` mode is automatically turned on only when `CacheWriteSynchronizationMode.FULL_SYNC` is used, as it  generally leads to better performance since write requests to primary and backups nodes are sent at the same time. 

In `PRIMARY` write order mode, cache version is assigned only on primary node. In this mode the sender will only send write requests to primary nodes, which in turn will assign write version and forward them to backups.

Atomic write order mode can be configured via `atomicWriteOrderMode` property of `CacheConfiguration`. 
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.IgniteConfiguration\">\n  \t...\n    <property name=\"cacheConfiguration\">\n        <bean class=\"org.apache.ignite.configuration.CacheConfiguration\">\n           \t<!-- Set a cache name. -->\n           \t<property name=\"name\" value=\"cacheName\"/>\n          \t\n          \t<!-- Atomic write order mode. -->\n    \t\t\t\t<property name=\"atomicWriteOrderMode\" value=\"PRIMARY\"/>\n    \t\t\t\t... \n        </bean\n    </property>\n</bean>",
      "language": "xml"
    },
    {
      "code": "CacheConfiguration cacheCfg = new CacheConfiguration();\n\ncacheCfg.setName(\"cacheName\");\n\ncacheCfg.setAtomicWriteOrderMode(CacheAtomicWriteOrderMode.CLOCK);\n\nIgniteConfiguration cfg = new IgniteConfiguration();\n\ncfg.setCacheConfiguration(cacheCfg);\n\n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]

[block:callout]
{
  "type": "info",
  "body": "For more information on `ATOMIC` mode, refer to [Transactions](/docs/transactions) section."
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Primary and Backup Nodes"
}
[/block]
In `PARTITIONED` mode, nodes to which the keys are assigned to are called primary nodes for those keys. You can also optionally configure any number of backup nodes for cached data. If the number of backups is greater than 0, then Ignite will automatically assign backup nodes for each individual key. For example if the number of backups is 1, then every key cached in the data grid will have 2 copies, 1 primary and 1 backup.
[block:callout]
{
  "type": "info",
  "body": "By default, backups are turned off for better performance."
}
[/block]
Backups can be configured by setting `backups()` property of 'CacheConfiguration`, like so:
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.IgniteConfiguration\">\n  \t...\n    <property name=\"cacheConfiguration\">\n        <bean class=\"org.apache.ignite.configuration.CacheConfiguration\">\n           \t<!-- Set a cache name. -->\n           \t<property name=\"name\" value=\"cacheName\"/>\n          \n          \t<!-- Set cache mode. -->\n    \t\t\t\t<property name=\"cacheMode\" value=\"PARTITIONED\"/>\n          \t\n          \t<!-- Number of backup nodes. -->\n    \t\t\t\t<property name=\"backups\" value=\"1\"/>\n    \t\t\t\t... \n        </bean\n    </property>\n</bean>",
      "language": "xml"
    },
    {
      "code": "CacheConfiguration cacheCfg = new CacheConfiguration();\n\ncacheCfg.setName(\"cacheName\");\n\ncacheCfg.setCacheMode(CacheMode.PARTITIONED);\n\ncacheCfg.setBackups(1);\n\nIgniteConfiguration cfg = new IgniteConfiguration();\n\ncfg.setCacheConfiguration(cacheCfg);\n\n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Near Caches"
}
[/block]
A partitioned cache can also be fronted by a `Near` cache, which is a smaller local cache that stores most recently or most frequently accessed data. Just like with a partitioned cache, the user can control the size of the near cache and its eviction policies. 

In the vast majority of use cases, whenever utilizing Ignite with affinity colocation, near caches should not be used. If computations are collocated with the proper partition cache nodes then the near cache is simply not needed because all the data is available locally in the partitioned cache.

However, there are cases when it is simply impossible to send computations to remote nodes. For cases like this near caches can significantly improve scalability and the overall performance of the application.

Following are configuration parameters related to near cache. These parameters make sense for `PARTITIONED` cache only.
[block:parameters]
{
  "data": {
    "0-0": "`setNearEvictionPolicy(CacheEvictionPolicy)`",
    "h-0": "Setter Method",
    "h-1": "Description",
    "h-2": "Default",
    "0-1": "Eviction policy for near cache.",
    "0-2": "`CacheLruEvictionPolicy` with max size of 10,000.",
    "1-0": "`setEvictNearSynchronized(boolean)`",
    "1-1": "Flag indicating whether eviction is synchronized with near caches on remote nodes.",
    "1-2": "true",
    "2-0": "`setNearStartSize(int)`",
    "3-0": "",
    "2-2": "256",
    "2-1": "Start size for near cache."
  },
  "cols": 3,
  "rows": 3
}
[/block]

[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.IgniteConfiguration\">\n  \t...\n    <property name=\"cacheConfiguration\">\n        <bean class=\"org.apache.ignite.configuration.CacheConfiguration\">\n          \t<!-- Set a cache name. -->\n           \t<property name=\"name\" value=\"cacheName\"/>\n          \n           \t<!-- Start size for near cache. -->\n    \t\t\t\t<property name=\"nearStartSize\" value=\"512\"/>\n \n            <!-- Configure LRU eviction policy for near cache. -->\n            <property name=\"nearEvictionPolicy\">\n                <bean class=\"org.apache.ignite.cache.eviction.lru.CacheLruEvictionPolicy\">\n                    <!-- Set max size to 1000. -->\n                    <property name=\"maxSize\" value=\"1000\"/>\n                </bean>\n            </property>\n    \t\t\t\t... \n        </bean\n    </property>\n</bean>",
      "language": "xml"
    },
    {
      "code": "CacheConfiguration cacheCfg = new CacheConfiguration();\n\ncacheCfg.setName(\"cacheName\");\n\ncacheCfg.setNearStartSize(512);\n\nCacheLruEvictionPolicy evctPolicy = new CacheLruEvictionPolicy();\nevctPolicy.setMaxSize(1000);\n\ncacheCfg.setNearEvictionPolicy(evctPolicy);\n\nIgniteConfiguration cfg = new IgniteConfiguration();\n\ncfg.setCacheConfiguration(cacheCfg);\n\n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Configuration"
}
[/block]

Cache modes are configured for each cache by setting the `cacheMode` property of `CacheConfiguration` like so:
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.IgniteConfiguration\">\n  \t...\n    <property name=\"cacheConfiguration\">\n        <bean class=\"org.apache.ignite.configuration.CacheConfiguration\">\n           \t<!-- Set a cache name. -->\n           \t<property name=\"name\" value=\"cacheName\"/>\n            \n          \t<!-- Set cache mode. -->\n    \t\t\t\t<property name=\"cacheMode\" value=\"PARTITIONED\"/>\n    \t\t\t\t... \n        </bean\n    </property>\n</bean>",
      "language": "xml"
    },
    {
      "code": "CacheConfiguration cacheCfg = new CacheConfiguration();\n\ncacheCfg.setName(\"cacheName\");\n\ncacheCfg.setCacheMode(CacheMode.PARTITIONED);\n\nIgniteConfiguration cfg = new IgniteConfiguration();\n\ncfg.setCacheConfiguration(cacheCfg);\n\n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]