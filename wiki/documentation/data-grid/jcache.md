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

Apache Ignite data grid is an implementation of JCache (JSR 107) specification (currently undergoing JSR107 TCK testing). JCache provides a very simple to use, but yet very powerful API for data access. However, the specification purposely omits any details about data distribution and consistency to allow vendors enough freedom in their own implementations. 

In addition to JCache, Ignite provides ACID transactions, data querying capabilities (including SQL), various memory models, queries, transactions, etc...
[block:api-header]
{
  "type": "basic",
  "title": "IgniteCache"
}
[/block]
`IgniteCache` is based on **JCache (JSR 107)**, so at the very basic level the APIs can be reduced to `javax.cache.Cache` interface. However, `IgniteCache` API also provides functionality that facilitates features outside of JCache spec, like data loading, querying, asynchronous mode, etc.

You can get an instance of `IgniteCache` directly from `Ignite`:
[block:code]
{
  "codes": [
    {
      "code": "Ignite ignite = Ignition.ignite();\n\nIgniteCache cache = ignite.jcache(\"mycache\");",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Basic Operations"
}
[/block]
Here are some basic JCache atomic operation examples.
[block:code]
{
  "codes": [
    {
      "code": "try (Ignite ignite = Ignition.start(\"examples/config/example-cache.xml\")) {\n    IgniteCache<Integer, String> cache = ignite.cache(CACHE_NAME);\n \n    // Store keys in cache (values will end up on different cache nodes).\n    for (int i = 0; i < 10; i++)\n        cache.put(i, Integer.toString(i));\n \n    for (int i = 0; i < 10; i++)\n        System.out.println(\"Got [key=\" + i + \", val=\" + cache.get(i) + ']');\n}",
      "language": "java",
      "name": "Put & Get"
    },
    {
      "code": "// Put-if-absent which returns previous value.\nInteger oldVal = cache.getAndPutIfAbsent(\"Hello\", 11);\n  \n// Put-if-absent which returns boolean success flag.\nboolean success = cache.putIfAbsent(\"World\", 22);\n  \n// Replace-if-exists operation (opposite of getAndPutIfAbsent), returns previous value.\noldVal = cache.getAndReplace(\"Hello\", 11);\n \n// Replace-if-exists operation (opposite of putIfAbsent), returns boolean success flag.\nsuccess = cache.replace(\"World\", 22);\n  \n// Replace-if-matches operation.\nsuccess = cache.replace(\"World\", 2, 22);\n  \n// Remove-if-matches operation.\nsuccess = cache.remove(\"Hello\", 1);",
      "language": "java",
      "name": "Atomic"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "EntryProcessor"
}
[/block]
Whenever doing `puts` and `updates` in cache, you are usually sending full state object state across the network. `EntryProcessor` allows for processing data directly on primary nodes, often transferring only the deltas instead of the full state. 

Moreover, you can embed your own logic into `EntryProcessors`, for example, taking previous cached value and incrementing it by 1.
[block:code]
{
  "codes": [
    {
      "code": "IgniteCache<String, Integer> cache = ignite.jcache(\"mycache\");\n\n// Increment cache value 10 times.\nfor (int i = 0; i < 10; i++)\n  cache.invoke(\"mykey\", (entry, args) -> {\n    Integer val = entry.getValue();\n\n    entry.setValue(val == null ? 1 : val + 1);\n\n    return null;\n  });",
      "language": "java",
      "name": "invoke"
    },
    {
      "code": "IgniteCache<String, Integer> cache = ignite.jcache(\"mycache\");\n\n// Increment cache value 10 times.\nfor (int i = 0; i < 10; i++)\n  cache.invoke(\"mykey\", new EntryProcessor<String, Integer, Void>() {\n    @Override \n    public Object process(MutableEntry<Integer, String> entry, Object... args) {\n      Integer val = entry.getValue();\n\n      entry.setValue(val == null ? 1 : val + 1);\n\n      return null;\n    }\n  });",
      "language": "java",
      "name": "java7 invoke"
    }
  ]
}
[/block]

[block:callout]
{
  "type": "info",
  "title": "Atomicity",
  "body": "`EntryProcessors` are executed atomically within a lock on the given cache key."
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Asynchronous Support"
}
[/block]
Just like all distributed APIs in Ignite, `IgniteCache` extends [IgniteAsynchronousSupport](doc:async-support) interface and can be used in asynchronous mode.
[block:code]
{
  "codes": [
    {
      "code": "// Enable asynchronous mode.\nIgniteCache<String, Integer> asyncCache = ignite.jcache(\"mycache\").withAsync();\n\n// Asynhronously store value in cache.\nasyncCache.getAndPut(\"1\", 1);\n\n// Get future for the above invocation.\nIgniteFuture<Integer> fut = asyncCache.future();\n\n// Asynchronously listen for the operation to complete.\nfut.listenAsync(f -> System.out.println(\"Previous cache value: \" + f.get()));",
      "language": "java",
      "name": "Async"
    }
  ]
}
[/block]