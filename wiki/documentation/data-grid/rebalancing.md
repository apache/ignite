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

When a new node joins topology, existing nodes relinquish primary or back up ownership of some keys to the new node so that keys remain equally balanced across the grid at all times.

If the new node becomes a primary or backup for some partition, it will fetch data from previous primary node for that partition or from one of the backup nodes for that partition. Once a partition is fully loaded to the new node, it will be marked obsolete on the old node and will be eventually evicted after all current transactions on that node are finished. Hence, for some short period of time, after topology changes, there can be a case when a cache will have more backup copies for a key than configured. However once rebalancing completes, extra backup copies will be removed from node caches.
[block:api-header]
{
  "type": "basic",
  "title": "Preload Modes"
}
[/block]
Following preload modes are defined in `CachePreloadMode` enum.
[block:parameters]
{
  "data": {
    "0-0": "`SYNC`",
    "h-0": "CachePreloadMode",
    "h-1": "Description",
    "0-1": "Synchronous rebalancing mode. Distributed caches will not start until all necessary data is loaded from other available grid nodes. This means that any call to cache public API will be blocked until rebalancing is finished.",
    "1-1": "Asynchronous rebalancing mode. Distributed caches will start immediately and will load all necessary data from other available grid nodes in the background.",
    "1-0": "`ASYNC`",
    "2-1": "In this mode no rebalancing will take place which means that caches will be either loaded on demand from persistent store whenever data is accessed, or will be populated explicitly.",
    "2-0": "`NONE`"
  },
  "cols": 2,
  "rows": 3
}
[/block]
By default, `ASYNC` preload mode is enabled. To use another mode, you can set the `preloadMode` property of `CacheConfiguration`, like so:
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.IgniteConfiguration\">\n    ...\n    <property name=\"cacheConfiguration\">\n        <bean class=\"org.apache.ignite.configuration.CacheConfiguration\">          \t\t\n          \t<!-- Set synchronous preloading. -->\n    \t\t\t\t<property name=\"preloadMode\" value=\"SYNC\"/>\n            ... \n        </bean\n    </property>\n</bean>",
      "language": "xml"
    },
    {
      "code": "CacheConfiguration cacheCfg = new CacheConfiguration();\n\ncacheCfg.setPreloadMode(CachePreloadMode.SYNC);\n\nIgniteConfiguration cfg = new IgniteConfiguration();\n\ncfg.setCacheConfiguration(cacheCfg);\n\n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Rebalance Message Throttling"
}
[/block]
When re-balancer transfers data from one node to another, it splits the whole data set into batches and sends each batch in a separate message. If your data sets are large and there are a lot of messages to send, the CPU or network can get over-consumed. In this case it can be reasonable to wait between rebalance messages so that negative performance impact caused by preloading process is minimized. This time interval is controlled by `preloadThrottle` configuration property of  `CacheConfiguration`. Its default value is 0, which means that there will be no pauses between messages. Note that size of a single message can be also customized by `preloadBatchSize` configuration property (default size is 512K).

For example, if you want preloader to send 2MB of data per message with 100 ms throttle interval, you should provide the following configuration: 
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.IgniteConfiguration\">\n    ...\n    <property name=\"cacheConfiguration\">\n        <bean class=\"org.apache.ignite.configuration.CacheConfiguration\">          \t\t\n          \t<!-- Set batch size. -->\n    \t\t\t\t<property name=\"preloadBatchSize\" value=\"#{2 * 1024 * 1024}\"/>\n \n    \t\t\t\t<!-- Set throttle interval. -->\n    \t\t\t\t<property name=\"preloadThrottle\" value=\"100\"/>\n            ... \n        </bean\n    </property>\n</bean> ",
      "language": "xml"
    },
    {
      "code": "CacheConfiguration cacheCfg = new CacheConfiguration();\n\ncacheCfg.setPreloadBatchSize(2 * 1024 * 1024);\n            \ncacheCfg.setPreloadThrottle(100);\n\nIgniteConfiguration cfg = new IgniteConfiguration();\n\ncfg.setCacheConfiguration(cacheCfg);\n\n// Start Ignite node.\nIgnition.start(cfg);",
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
Cache preloading behavior can be customized by optionally setting the following configuration properties:
[block:parameters]
{
  "data": {
    "h-0": "Setter Method",
    "h-1": "Description",
    "h-2": "Default",
    "0-0": "`setPreloadMode`",
    "0-1": "Preload mode for distributed cache. See Preload Modes section for details.",
    "1-0": "`setPreloadPartitionedDelay`",
    "1-1": "Preloading delay in milliseconds. See Delayed And Manual Preloading section for details.",
    "2-0": "`setPreloadBatchSize`",
    "2-1": "Size (in bytes) to be loaded within a single preload message. Preloading algorithm will split total data set on every node into multiple batches prior to sending data.",
    "3-0": "`setPreloadThreadPoolSize`",
    "3-1": "Size of preloading thread pool. Note that size serves as a hint and implementation may create more threads for preloading than specified here (but never less threads).",
    "4-0": "`setPreloadThrottle`",
    "4-1": "Time in milliseconds to wait between preload messages to avoid overloading of CPU or network. When preloading large data sets, the CPU or network can get over-consumed with preloading messages, which consecutively may slow down the application performance. This parameter helps tune the amount of time to wait between preload messages to make sure that preloading process does not have any negative performance impact. Note that application will continue to work properly while preloading is still in progress.",
    "5-0": "`setPreloadOrder`",
    "6-0": "`setPreloadTimeout`",
    "5-1": "Order in which preloading should be done. Preload order can be set to non-zero value for caches with SYNC or ASYNC preload modes only. Preloading for caches with smaller preload order will be completed first. By default, preloading is not ordered.",
    "6-1": "Preload timeout (ms).",
    "0-2": "`ASYNC`",
    "1-2": "0 (no delay)",
    "2-2": "512K",
    "3-2": "2",
    "4-2": "0 (throttling disabled)",
    "5-2": "0",
    "6-2": "10000"
  },
  "cols": 3,
  "rows": 7
}
[/block]