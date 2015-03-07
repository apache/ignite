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

Eviction policies control the maximum number of elements that can be stored in a cache on-heap memory.  Whenever maximum on-heap cache size is reached, entries are evicted into [off-heap space](doc:off-heap-memory), if one is enabled. 

In Ignite eviction policies are pluggable and are controlled via `CacheEvictionPolicy` interface. An implementation of eviction policy is notified of every cache change and defines the algorithm of choosing the entries to evict from cache. 
[block:callout]
{
  "type": "info",
  "body": "If your data set can fit in memory, then eviction policy will not provide any benefit and should be disabled, which is the default behavior."
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Least Recently Used (LRU)"
}
[/block]
LRU eviction policy is based on [Least Recently Used (LRU)](http://en.wikipedia.org/wiki/Cache_algorithms#Least_Recently_Used) algorithm, which ensures that the least recently used entry (i.e. the entry that has not been touched the longest) gets evicted first. 
[block:callout]
{
  "type": "success",
  "body": "LRU eviction policy nicely fits most of the use cases for caching. Use it whenever in doubt."
}
[/block]
This eviction policy is implemented by `CacheLruEvictionPolicy` and can be configured via `CacheConfiguration`.
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.cache.CacheConfiguration\">\n  <property name=\"name\" value=\"myCache\"/>\n    ...\n    <property name=\"evictionPolicy\">\n        <!-- LRU eviction policy. -->\n        <bean class=\"org.apache.ignite.cache.eviction.lru.CacheLruEvictionPolicy\">\n            <!-- Set the maximum cache size to 1 million (default is 100,000). -->\n            <property name=\"maxSize\" value=\"1000000\"/>\n        </bean>\n    </property>\n    ...\n</bean>",
      "language": "xml"
    },
    {
      "code": "CacheConfiguration cacheCfg = new CacheConfiguration();\n\ncacheCfg.setName(\"cacheName\");\n\n// Set the maximum cache size to 1 million (default is 100,000).\ncacheCfg.setEvictionPolicy(new CacheLruEvictionPolicy(1000000);\n\nIgniteConfiguration cfg = new IgniteConfiguration();\n\ncfg.setCacheConfiguration(cacheCfg);\n\n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "First In First Out (FIFO)"
}
[/block]
FIFO eviction policy is based on [First-In-First-Out (FIFO)](https://en.wikipedia.org/wiki/FIFO) algorithm which ensures that entry that has been in cache the longest will be evicted first. It is different from `CacheLruEvictionPolicy` because it ignores the access order of entries. 

This eviction policy is implemented by `CacheFifoEvictionPolicy` and can be configured via `CacheConfiguration`.
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.cache.CacheConfiguration\">\n  <property name=\"name\" value=\"myCache\"/>\n    ...\n    <property name=\"evictionPolicy\">\n        <!-- FIFO eviction policy. -->\n        <bean class=\"org.apache.ignite.cache.eviction.fifo.CacheFifoEvictionPolicy\">\n            <!-- Set the maximum cache size to 1 million (default is 100,000). -->\n            <property name=\"maxSize\" value=\"1000000\"/>\n        </bean>\n    </property>\n    ...\n</bean>",
      "language": "xml"
    },
    {
      "code": "CacheConfiguration cacheCfg = new CacheConfiguration();\n\ncacheCfg.setName(\"cacheName\");\n\n// Set the maximum cache size to 1 million (default is 100,000).\ncacheCfg.setEvictionPolicy(new CacheFifoEvictionPolicy(1000000);\n\nIgniteConfiguration cfg = new IgniteConfiguration();\n\ncfg.setCacheConfiguration(cacheCfg);\n\n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Random"
}
[/block]
Random eviction policy which randomly chooses entries to evict. This eviction policy is mainly used for debugging and benchmarking purposes.

This eviction policy is implemented by `CacheRandomEvictionPolicy` and can be configured via `CacheConfiguration`.
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.cache.CacheConfiguration\">\n  <property name=\"name\" value=\"myCache\"/>\n    ...\n    <property name=\"evictionPolicy\">\n        <!-- Random eviction policy. -->\n        <bean class=\"org.apache.ignite.cache.eviction.random.CacheRandomEvictionPolicy\">\n            <!-- Set the maximum cache size to 1 million (default is 100,000). -->\n            <property name=\"maxSize\" value=\"1000000\"/>\n        </bean>\n    </property>\n    ...\n</bean>",
      "language": "xml"
    },
    {
      "code": "CacheConfiguration cacheCfg = new CacheConfiguration();\n\ncacheCfg.setName(\"cacheName\");\n\n// Set the maximum cache size to 1 million (default is 100,000).\ncacheCfg.setEvictionPolicy(new CacheRandomEvictionPolicy(1000000);\n\nIgniteConfiguration cfg = new IgniteConfiguration();\n\ncfg.setCacheConfiguration(cacheCfg);\n\n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]