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

Ignite In-Memory Data Fabric can be used as [Hibernate](http://hibernate.org) Second-Level cache (or L2 cache), which can significantly speed-up the persistence layer of your application.

[Hibernate](http://hibernate.org) is a well-known and widely used framework for Object-Relational Mapping (ORM). While interacting closely with an SQL database, it performs caching of retrieved data to minimize expensive database requests.
[block:image]
{
  "images": [
    {
      "image": [
        "https://www.filepicker.io/api/file/D35hL3OuQ46YA4v3BLwJ",
        "hibernate-L2-cache.png",
        "600",
        "478",
        "#b7917a",
        ""
      ]
    }
  ]
}
[/block]
All work with Hibernate database-mapped objects is done within a session, usually bound to a worker thread or a Web session. By default, Hibernate only uses per-session (L1) cache, so, objects, cached in one session, are not seen in another. However, a global second-level (L2) cache may be used, in which the cached objects are seen for all sessions that use the same L2 cache configuration. This usually gives a significantly greater performance gain, because each newly-created session can take full advantage of the data already present in L2 cache memory (which outlives any session-local L1 cache).

While L1 cache is always enabled and fully implemented by Hibernate internally, L2 cache is optional and can have multiple pluggable implementaions. Ignite can be easily plugged-in as an L2 cache implementation, and can be used in all access modes (`READ_ONLY`, `READ_WRITE`, `NONSTRICT_READ_WRITE`, and `TRANSACTIONAL`), supporting a wide range of related features:
  * caching to memory and disk, as well as off-heap memory.
  * cache transactions, that make `TRANSACTIONA`L mode possible.
  * clustering, with 2 different replication modes: `REPLICATED` and `PARTITIONED`

To start using GridGain as a Hibernate L2 cache, you need to perform 3 simple steps:
  * Add Ignite libraries to your application's classpath.
  * Enable L2 cache and specify Ignite implementation class in L2 cache configuration.
  * Configure Ignite caches for L2 cache regions and start the embedded Ignite node (and, optionally, external Ignite nodes). 
 
In the section below we cover these steps in more detail.
[block:api-header]
{
  "type": "basic",
  "title": "L2 Cache Configuration"
}
[/block]
To configure Ignite In-Memory Data Fabric as a Hibernate L2 cache, without any changes required to the existing Hibernate code, you need to:
  * Configure Hibernate itself to use Ignite as L2 cache.
  * Configure Ignite cache appropriately. 

##Hibernate Configuration Example
A typical Hibernate configuration for L2 cache with Ignite would look like the one below:
[block:code]
{
  "codes": [
    {
      "code": "<hibernate-configuration>\n    <session-factory>\n        ...\n        <!-- Enable L2 cache. -->\n        <property name=\"cache.use_second_level_cache\">true</property>\n        \n        <!-- Generate L2 cache statistics. -->\n        <property name=\"generate_statistics\">true</property>\n        \n        <!-- Specify GridGain as L2 cache provider. -->\n        <property name=\"cache.region.factory_class\">org.gridgain.grid.cache.hibernate.GridHibernateRegionFactory</property>\n        \n        <!-- Specify the name of the grid, that will be used for second level caching. -->\n        <property name=\"org.gridgain.hibernate.grid_name\">hibernate-grid</property>\n        \n        <!-- Set default L2 cache access type. -->\n        <property name=\"org.gridgain.hibernate.default_access_type\">READ_ONLY</property>\n        \n        <!-- Specify the entity classes for mapping. -->\n        <mapping class=\"com.mycompany.MyEntity1\"/>\n        <mapping class=\"com.mycompany.MyEntity2\"/>\n        \n        <!-- Per-class L2 cache settings. -->\n        <class-cache class=\"com.mycompany.MyEntity1\" usage=\"read-only\"/>\n        <class-cache class=\"com.mycompany.MyEntity2\" usage=\"read-only\"/>\n        <collection-cache collection=\"com.mycompany.MyEntity1.children\" usage=\"read-only\"/>\n        ...\n    </session-factory>\n</hibernate-configuration>",
      "language": "xml"
    }
  ]
}
[/block]
Here, we do the following:
  * Enable L2 cache (and, optionally, the L2 cache statistics generation).
  * Specify Ignite as L2 cache implementation.
  * Specify the name of the caching grid (should correspond to the one in Ignite configuration).
  * Specify the entity classes and configure caching for each class (a corresponding cache region should be configured in Ignite). 

##Ignite Configuration Example
A typical Ignite configuration for Hibernate L2 caching looks like this:
[block:code]
{
  "codes": [
    {
      "code": "<!-- Basic configuration for atomic cache. -->\n<bean id=\"atomic-cache\" class=\"org.apache.ignite.configutation.CacheConfiguration\" abstract=\"true\">\n    <property name=\"cacheMode\" value=\"PARTITIONED\"/>\n    <property name=\"atomicityMode\" value=\"ATOMIC\"/>\n    <property name=\"writeSynchronizationMode\" value=\"FULL_SYNC\"/>\n</bean>\n \n<!-- Basic configuration for transactional cache. -->\n<bean id=\"transactional-cache\" class=\"org.apache.ignite.configutation.CacheConfiguration\" abstract=\"true\">\n    <property name=\"cacheMode\" value=\"PARTITIONED\"/>\n    <property name=\"atomicityMode\" value=\"TRANSACTIONAL\"/>\n    <property name=\"writeSynchronizationMode\" value=\"FULL_SYNC\"/>\n</bean>\n \n<bean id=\"ignite.cfg\" class=\"org.apache.ignite.configuration.IgniteConfiguration\">\n    <!-- \n        Specify the name of the caching grid (should correspond to the \n        one in Hibernate configuration).\n    -->\n    <property name=\"gridName\" value=\"hibernate-grid\"/>\n    ...\n    <!-- \n        Specify cache configuration for each L2 cache region (which corresponds \n        to a full class name or a full association name).\n    -->\n    <property name=\"cacheConfiguration\">\n        <list>\n            <!--\n                Configurations for entity caches.\n            -->\n            <bean parent=\"transactional-cache\">\n                <property name=\"name\" value=\"com.mycompany.MyEntity1\"/>\n            </bean>\n            <bean parent=\"transactional-cache\">\n                <property name=\"name\" value=\"com.mycompany.MyEntity2\"/>\n            </bean>\n            <bean parent=\"transactional-cache\">\n                <property name=\"name\" value=\"com.mycompany.MyEntity1.children\"/>\n            </bean>\n \n            <!-- Configuration for update timestamps cache. -->\n            <bean parent=\"atomic-cache\">\n                <property name=\"name\" value=\"org.hibernate.cache.spi.UpdateTimestampsCache\"/>\n            </bean>\n \n            <!-- Configuration for query result cache. -->\n            <bean parent=\"atomic-cache\">\n                <property name=\"name\" value=\"org.hibernate.cache.internal.StandardQueryCache\"/>\n            </bean>\n        </list>\n    </property>\n    ...\n</bean>",
      "language": "xml"
    }
  ]
}
[/block]
Here, we specify the cache configuration for each L2 cache region:
  * We use `PARTITIONED` cache to split the data between caching nodes. Another possible strategy is to enable `REPLICATED` mode, thus replicating a full dataset between all caching nodes. See Cache Distribution Models for more information.
  * We specify the cache name that corresponds an L2 cache region name (either a full class name or a full association name).
  * We use `TRANSACTIONAL` atomicity mode to take advantage of cache transactions.
  * We enable `FULL_SYNC` to be always fully synchronized with backup nodes.

Additionally, we specify a cache for update timestamps, which may be `ATOMIC`, for better performance.

Having configured Ignite caching node, we can start it from within our code the following way:
[block:code]
{
  "codes": [
    {
      "code": "Ignition.start(\"my-config-folder/my-ignite-configuration.xml\");",
      "language": "java"
    }
  ]
}
[/block]
After the above line is executed, the internal Ignite node is started and is ready to cache the data. We can also start additional standalone nodes by running the following command from console:

[block:code]
{
  "codes": [
    {
      "code": "$IGNITE_HOME/bin/ignite.sh my-config-folder/my-ignite-configuration.xml",
      "language": "text"
    }
  ]
}
[/block]
For Windows, use the `.bat` script in the same folder.
[block:callout]
{
  "type": "success",
  "body": "The nodes may be started on other hosts as well, forming a distributed caching cluster. Be sure to specify the right network settings in GridGain configuration file for that."
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Query Cache"
}
[/block]
In addition to L2 cache, Hibernate offers a query cache. This cache stores the results of queries (either HQL or Criteria) with a given set of parameters, so, when you repeat the query with the same parameter set, it hits the cache without going to the database. 

Query cache may be useful if you have a number of queries, which may repeat with the same parameter values. Like in case of L2 cache, Hibernate relies on a 3-rd party cache implementation, and Ignite In-Memory Data Fabric can be used as such.
[block:callout]
{
  "type": "success",
  "body": "Consider using support for [SQL-based In-Memory Queries](/docs/cache-queries) in Ignite which should perform faster than going through Hibernate."
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Query Cache Configuration"
}
[/block]
The [configuration](#l2-cache-configuration) information above totally applies to query cache, but some additional configuration and code change is required.

##Hibernate Configuration
To enable query cache in Hibernate, you only need one additional line in configuration file:
[block:code]
{
  "codes": [
    {
      "code": "<!-- Enable query cache. -->\n<property name=\"cache.use_query_cache\">true</property>",
      "language": "xml"
    }
  ]
}
[/block]
Yet, a code modification is required: for each query that you want to cache, you should enable `cacheable` flag by calling `setCacheable(true)`:
[block:code]
{
  "codes": [
    {
      "code": "Session ses = ...;\n \n// Create Criteria query.\nCriteria criteria = ses.createCriteria(cls);\n \n// Enable cacheable flag.\ncriteria.setCacheable(true);\n \n...",
      "language": "java"
    }
  ]
}
[/block]
After this is done, your query results will be cached.

##Ignite Configuration
To enable Hibernate query caching in Ignite, you need to specify an additional cache configuration:
[block:code]
{
  "codes": [
    {
      "code": "\n<property name=\"cacheConfiguration\">\n    <list>\n        ...\n        <!-- Query cache (refers to atomic cache defined in above example). -->\n        <bean parent=\"atomic-cache\">\n            <property name=\"name\" value=\"org.hibernate.cache.internal.StandardQueryCache\"/>\n        </bean>\n    </list>\n</property>",
      "language": "xml"
    }
  ]
}
[/block]
Notice that the cache is made `ATOMIC` for better performance.