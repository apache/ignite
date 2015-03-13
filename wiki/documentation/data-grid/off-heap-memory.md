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

Off-Heap memory allows your cache to overcome lengthy JVM Garbage Collection (GC) pauses when working with large heap sizes by caching data outside of main Java Heap space, but still in RAM.
[block:image]
{
  "images": [
    {
      "image": [
        "https://www.filepicker.io/api/file/iXCEc4RsQ4a1SM1Vfjnl",
        "off-heap-memory.png",
        "450",
        "354",
        "#6c521f",
        ""
      ]
    }
  ]
}
[/block]

[block:callout]
{
  "type": "info",
  "title": "Off-Heap Indexes",
  "body": "Note that when off-heap memory is configured, Ignite will also store query indexes off-heap as well. This means that indexes will not take any portion of on-heap memory."
}
[/block]

[block:callout]
{
  "type": "success",
  "body": "You can also manage GC pauses by starting multiple processes with smaller heap on the same physical server. However, such approach is wasteful when using REPLICATED caches as we will end up with caching identical *replicated* data for every started JVM process.",
  "title": "Off-Heap Memory vs. Multiple Processes"
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Tiered Off-Heap Storage"
}
[/block]
Ignite provides tiered storage model, where data can be stored and moved between **on-heap**, **off-heap**, and **swap space**. Going up the tier provides more data storage capacity, with gradual increase in latency. 

Ignite provides three types of memory modes, defined in `CacheMemoryMode`, for storing cache entries, supporting tiered storage model:
[block:parameters]
{
  "data": {
    "h-0": "Memory Mode",
    "h-1": "Description",
    "0-0": "`ONHEAP_TIERED`",
    "0-1": "Store entries on-heap and evict to off-heap and optionally to swap.",
    "1-0": "`OFFHEAP_TIERED`",
    "2-0": "`OFFHEAP_VALUES`",
    "1-1": "Store entries off-heap, bypassing on-heap and optionally evicting to swap.",
    "2-1": "Store keys on-heap and values off-heap."
  },
  "cols": 2,
  "rows": 3
}
[/block]
Cache can be configured to use any of the three modes by setting the `memoryMode` configuration property of `CacheConfiguration`, as described below.
[block:api-header]
{
  "type": "basic",
  "title": "ONHEAP_TIERED"
}
[/block]
In Ignite, `ONHEAP_TIERED`  is the default memory mode, where all cache entries are stored on-heap. Entries can be moved from on-heap to off-heap storage and later to swap space, if one is configured.

To configure `ONHEAP_TIERED` memory mode, you need to:

1. Set `memoryMode` property of `CacheConfiguration` to `ONHEAP_TIERED`. 
2. Enable off-heap memory (optionally).
3. Configure *eviction policy* for on-heap memory.
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.CacheConfiguration\">\n  ...\n  <!-- Store cache entries on-heap. -->\n  <property name=\"memoryMode\" value=\"ONHEAP_TIERED\"/> \n\n  <!-- Enable Off-Heap memory with max size of 10 Gigabytes (0 for unlimited). -->\n  <property name=\"offHeapMaxMemory\" value=\"#{10 * 1024L * 1024L * 1024L}\"/>\n\n  <!-- Configure eviction policy. -->\n  <property name=\"evictionPolicy\">\n    <bean class=\"org.apache.ignite.cache.eviction.fifo.CacheFifoEvictionPolicy\">\n      <!-- Evict to off-heap after cache size reaches maxSize. -->\n      <property name=\"maxSize\" value=\"100000\"/>\n    </bean>\n  </property>\n  ...\n</bean>",
      "language": "xml"
    },
    {
      "code": "CacheConfiguration cacheCfg = new CacheConfiguration();\n\ncacheCfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);\n\n// Set off-heap memory to 10GB (0 for unlimited)\ncacheCfg.setOffHeapMaxMemory(10 * 1024L * 1024L * 1024L);\n\nCacheFifoEvictionPolicy evctPolicy = new CacheFifoEvictionPolicy();\n\n// Store only 100,000 entries on-heap.\nevctPolicy.setMaxSize(100000);\n\ncacheCfg.setEvictionPolicy(evctPolicy);\n\nIgniteConfiguration cfg = new IgniteConfiguration();\n\ncfg.setCacheConfiguration(cacheCfg);\n\n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]

[block:callout]
{
  "type": "warning",
  "body": "Note that if you do not enable eviction policy in ONHEAP_TIERED mode, data will never be moved from on-heap to off-heap memory.",
  "title": "Eviction Policy"
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "OFFHEAP_TIERED"
}
[/block]
This memory mode allows you to configure your cache to store entries directly into off-heap storage, bypassing on-heap memory. Since all entries are stored off-heap, there is no need to explicitly configure an eviction policy. If off-heap storage size is exceeded (0 for unlimited), then LRU eviction policy is used to evict entries from off-heap store and optionally moving them to swap space, if one is configured.

To configure `OFFHEAP_TIERED` memory mode, you need to:

1. Set `memoryMode` property of `CacheConfiguration` to `OFFHEAP_TIERED`. 
2. Enable off-heap memory (optionally).
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.CacheConfiguration\">\n  ...\n  <!-- Always store cache entries in off-heap memory. -->\n  <property name=\"memoryMode\" value=\"OFFHEAP_TIERED\"/>\n\n  <!-- Enable Off-Heap memory with max size of 10 Gigabytes (0 for unlimited). -->\n  <property name=\"offHeapMaxMemory\" value=\"#{10 * 1024L * 1024L * 1024L}\"/>\n  ...\n</bean>",
      "language": "xml"
    },
    {
      "code": "CacheConfiguration cacheCfg = new CacheConfiguration();\n\ncacheCfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);\n\n// Set off-heap memory to 10GB (0 for unlimited)\ncacheCfg.setOffHeapMaxMemory(10 * 1024L * 1024L * 1024L);\n\nIgniteConfiguration cfg = new IgniteConfiguration();\n\ncfg.setCacheConfiguration(cacheCfg);\n\n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "OFFHEAP_VALUES"
}
[/block]
Setting this memory mode allows you to store keys on-heap and values off-heap. This memory mode is useful when keys are small and values are large.

To configure `OFFHEAP_VALUES` memory mode, you need to:

1. Set `memoryMode` property of `CacheConfiguration` to `OFFHEAP_VALUES`. 
2. Enable off-heap memory.
3. Configure *eviction policy* for on-heap memory (optionally).
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.CacheConfiguration\">\n  ...\n  <!-- Always store cache entries in off-heap memory. -->\n  <property name=\"memoryMode\" value=\"OFFHEAP_VALUES\"/>\n\n  <!-- Enable Off-Heap memory with max size of 10 Gigabytes (0 for unlimited). -->\n  <property name=\"offHeapMaxMemory\" value=\"#{10 * 1024L * 1024L * 1024L}\"/>\n  ...\n</bean>",
      "language": "xml"
    },
    {
      "code": "CacheConfiguration cacheCfg = new CacheConfiguration();\n\ncacheCfg.setMemoryMode(CacheMemoryMode.OFFHEAP_VALUES);\n\nIgniteConfiguration cfg = new IgniteConfiguration();\n\ncfg.setCacheConfiguration(cacheCfg);\n\n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Swap Space"
}
[/block]
Whenever your data set exceeds the limits of on-heap and off-heap memory, you can configure swap space in which case Ignite will evict entries to the disk instead of discarding them.
[block:callout]
{
  "type": "warning",
  "title": "Swap Space Performance",
  "body": "Since swap space is on-disk, it is significantly slower than on-heap or off-heap memory."
}
[/block]

[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.CacheConfiguration\">\n  ...\n  <!-- Enable swap. -->\n  <property name=\"swapEnabled\" value=\"true\"/> \n  ...\n</bean>",
      "language": "xml"
    },
    {
      "code": "CacheConfiguration cacheCfg = new CacheConfiguration();\n\ncacheCfg.setSwapEnabled(true);\n\nIgniteConfiguration cfg = new IgniteConfiguration();\n\ncfg.setCacheConfiguration(cacheCfg);\n\n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]