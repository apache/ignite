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

Ignite In-Memory Data Fabric is capable of caching web sessions of all Java Servlet containers that follow Java Servlet 3.0 Specification, including Apache Tomcat, Eclipse Jetty, Oracle WebLogic, and others.

Web sessions caching becomes useful when running a cluster of app servers. When running a web application in a servlet container, you may face performance and scalability problems. A single app server is usually not able to handle large volumes of traffic by itself. A common solution is to scale your web application across multiple clustered instances:
[block:image]
{
  "images": [
    {
      "image": [
        "https://www.filepicker.io/api/file/AlvqqQhZRym15ji5iztA",
        "web_sessions_1.png",
        "561",
        "502",
        "#7f9eaa",
        ""
      ]
    }
  ]
}
[/block]
In the architecture shown above, High Availability Proxy (Load Balancer) distributes requests between multiple Application Server instances (App Server 1, App Server 2, ...), reducing the load on each instance and providing service availability if any of the instances fails. The problem here is web session availability. A web session keeps an intermediate logical state between requests by using cookies, and is normally bound to a particular application instance. Generally this is handled using sticky connections, ensuring that requests from the same user are handled by the same app server instance. However, if that instance fails, the session is lost, and the user will have to create it anew, loosing all the current unsaved state:
[block:image]
{
  "images": [
    {
      "image": [
        "https://www.filepicker.io/api/file/KtAyyVzrQ5CwhxODgEVV",
        "web_sessions_2.png",
        "561",
        "502",
        "#fb7661",
        ""
      ]
    }
  ]
}
[/block]
A solution here is to use Ignite In-Memory Data Fabric web sessions cache - a distributed cache that maintains a copy of each created session, sharing them between all instances. If any of your application instances fails, Ignite will automatically restore the sessions, owned by the failed instance, from the distributed cache regardless of which app server the next request will be forwarded to. Moreover, with web session caching sticky connections become less important as the session is available on any app server the web request may be routed to.
[block:image]
{
  "images": [
    {
      "image": [
        "https://www.filepicker.io/api/file/8WyBbutWSm4PRYDNWRr7",
        "web_sessions_3.png",
        "561",
        "502",
        "#f73239",
        ""
      ]
    }
  ]
}
[/block]
In this chapter we give a brief architecture overview of Ignite's web session caching functionality and instructions on how to configure your web application to enable web sessions caching.
[block:api-header]
{
  "type": "basic",
  "title": "Architecture"
}
[/block]
To set up a distributed web sessions cache with Ignite, you normally configure your web application to start a Ignite node (embedded mode). When multiple application server instances are started, all Ignite nodes connect with each-other forming a distributed cache.
[block:callout]
{
  "type": "info",
  "body": "Note that not every Ignite caching node has to be running inside of application server. You can also start additional, standalone Ignite nodes and add them to the topology as well."
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Replication Strategies"
}
[/block]
There are several replication strategies you can use when storing sessions in Ignite In-Memory Data Fabric. The replication strategy is defined by the backing cache settings. In this section we briefly cover most common configurations.

##Fully Replicated Cache
This strategy stores copies of all sessions on each Ignite node, providing maximum availability. However with this approach you can only cache as many web sessions as can fit in memory on a single server. Additionally, the performance may suffer as every change of web session state now must be replicated to all other cluster nodes.

To enable fully replicated strategy, set cacheMode of your backing cache to `REPLICATED`:
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.CacheConfiguration\">\n    <!-- Cache mode. -->\n    <property name=\"cacheMode\" value=\"REPLICATED\"/>\n    ...\n</bean>",
      "language": "xml"
    }
  ]
}
[/block]
##Partitioned Cache with Backups
In partitioned mode, web sessions are split into partitions and every node is responsible for caching only partitions assigned to that node. With this approach, the more nodes you have, the more data can be cached. New nodes can always be added on the fly to add more memory.
[block:callout]
{
  "type": "info",
  "body": "With `Partitioned` mode, redundancy is addressed by configuring number of backups for every web session being cached."
}
[/block]
To enable partitioned strategy, set cacheMode of your backing cache to `PARTITIONED`, and set the number of backups with `backups` property of `CacheConfiguration`:
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.CacheConfiguration\">\n    <!-- Cache mode. -->\n    <property name=\"cacheMode\" value=\"PARTITIONED\"/>\n    <property name=\"backups\" value=\"1\"/>\n</bean>",
      "language": "xml"
    }
  ]
}
[/block]

[block:callout]
{
  "type": "info",
  "body": "See [Cache Distribution Models](doc:cache-distribution-models) for more information on different replication strategies available in Ignite."
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Expiration and Eviction"
}
[/block]
Stale sessions are cleaned up from cache automatically when they expire. However, if there are a lot of long-living sessions created, you may want to save memory by evicting dispensable sessions from cache when cache reaches a certain limit. This can be done by setting up cache eviction policy and specifying the maximum number of sessions to be stored in cache. For example, to enable automatic eviction with LRU algorithm and a limit of 10000 sessions, you will need to use the following cache configuration:

[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.CacheConfiguration\">\n    <!-- Cache name. -->\n    <property name=\"name\" value=\"session-cache\"/>\n \n    <!-- Set up LRU eviction policy with 10000 sessions limit. -->\n    <property name=\"evictionPolicy\">\n        <bean class=\"org.apache.ignite.cache.eviction.lru.CacheLruEvictionPolicy\">\n            <property name=\"maxSize\" value=\"10000\"/>\n        </bean>\n    </property>\n    ...\n</bean>",
      "language": "xml"
    }
  ]
}
[/block]

[block:callout]
{
  "type": "info",
  "body": "For more information about various eviction policies, see Eviction Policies section."
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Configuration"
}
[/block]
To enable web session caching in your application with Ignite, you need to:

1\. **Add Ignite JARs** - Download Ignite and add the following jars to your application’s classpath (`WEB_INF/libs` folder):
  * `ignite.jar`
  * `ignite-web.jar`
  * `ignite-log4j.jar`
  * `ignite-spring.jar`

Or, if you have a Maven based project, add the following to your application's pom.xml.
[block:code]
{
  "codes": [
    {
      "code": "<dependency>\n      <groupId>org.ignite</groupId>\n      <artifactId>ignite-fabric</artifactId>\n      <version> ${ignite.version}</version>\n      <type>pom</type>\n</dependency>\n\n<dependency>\n    <groupId>org.ignite</groupId>\n    <artifactId>ignite-web</artifactId>\n    <version> ${ignite.version}</version>\n</dependency>\n\n<dependency>\n    <groupId>org.ignite</groupId>\n    <artifactId>ignite-log4j</artifactId>\n    <version>${ignite.version}</version>\n</dependency>\n\n<dependency>\n    <groupId>org.ignite</groupId>\n    <artifactId>ignite-spring</artifactId>\n    <version>${ignite.version}</version>\n</dependency>",
      "language": "xml"
    }
  ]
}
[/block]
Make sure to replace ${ignite.version} with actual Ignite version.

2\. **Configure Cache Mode** - Configure Ignite cache in either `PARTITIONED` or `REPLICATED` mode (See [examples](#replication-strategies) above).

3\. **Update `web.xml`** - Declare a context listener and web session filter in `web.xml`:
[block:code]
{
  "codes": [
    {
      "code": "...\n\n<listener>\n   <listener-class>org.apache.ignite.startup.servlet.IgniteServletContextListenerStartup</listener-class>\n</listener>\n\n<filter>\n   <filter-name>IgniteWebSessionsFilter</filter-name>\n   <filter-class>org.apache.ignite.cache.websession.IgniteWebSessionFilter</filter-class>\n</filter>\n\n<!-- You can also specify a custom URL pattern. -->\n<filter-mapping>\n   <filter-name>IgniteWebSessionsFilter</filter-name>\n   <url-pattern>/*</url-pattern>\n</filter-mapping>\n\n<!-- Specify Ignite configuration (relative to META-INF folder or Ignite_HOME). -->\n<context-param>\n   <param-name>IgniteConfigurationFilePath</param-name>\n   <param-value>config/default-config.xml </param-value>\n</context-param>\n\n<!-- Specify the name of Ignite cache for web sessions. -->\n<context-param>\n   <param-name>IgniteWebSessionsCacheName</param-name>\n   <param-value>partitioned</param-value>\n</context-param>\n\n...",
      "language": "xml"
    }
  ]
}
[/block]
On application start, the listener will start a Ignite node within your application, which will connect to other nodes in the network, forming a distributed cache.

4\. **Set Eviction Policy (Optional)** - Set eviction policy for stale web sessions data lying in cache (See [example](#expiration-and-eviction) above).

##Configuration Parameters
`IgniteServletContextListenerStartup` has the following configuration parameters:
[block:parameters]
{
  "data": {
    "0-0": "`IgniteConfigurationFilePath`",
    "0-1": "Path to Ignite configuration file (relative to `META_INF` folder or `IGNITE_HOME`).",
    "0-2": "`/config/default-config.xml`",
    "h-2": "Default",
    "h-1": "Description",
    "h-0": "Parameter Name"
  },
  "cols": 3,
  "rows": 1
}
[/block]
`IgniteWebSessionFilter` has the following configuration parameters:
[block:parameters]
{
  "data": {
    "h-0": "Parameter Name",
    "h-1": "Description",
    "h-2": "Default",
    "0-0": "`IgniteWebSessionsGridName`",
    "0-1": "Grid name for a started Ignite node. Should refer to grid in configuration file (if a grid name is specified in configuration).",
    "0-2": "null",
    "1-0": "`IgniteWebSessionsCacheName`",
    "2-0": "`IgniteWebSessionsMaximumRetriesOnFail`",
    "1-1": "Name of Ignite cache to use for web sessions caching.",
    "1-2": "null",
    "2-1": "Valid only for `ATOMIC` caches. Specifies number of retries in case of primary node failures.",
    "2-2": "3"
  },
  "cols": 3,
  "rows": 3
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Supported Containers"
}
[/block]
Ignite has been officially tested with following servlet containers:
  * Apache Tomcat 7
  * Eclipse Jetty 9
  * Apache Tomcat 6
  * Oracle WebLogic >= 10.3.4