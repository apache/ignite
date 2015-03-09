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

Ignite In-Memory Data Fabric, in addition to providing standard key-value map-like storage, also provides an implementation of a fast Distributed Blocking Queue and Distributed Set.

`IgniteQueue` and `IgniteSet`, an implementation of `java.util.concurrent.BlockingQueue` and `java.util.Set` interface respectively,  also support all operations from `java.util.Collection` interface. Both, queue and set can be created in either collocated or non-collocated mode.

Below is an example of how to create a distributed queue and set.
[block:code]
{
  "codes": [
    {
      "code": "Ignite ignite = Ignition.ignite();\n\nIgniteQueue<String> queue = ignite.queue(\n    \"queueName\", // Queue name.\n    0,          // Queue capacity. 0 for unbounded queue.\n    null         // Collection configuration.\n);",
      "language": "java",
      "name": "Queue"
    },
    {
      "code": "Ignite ignite = Ignition.ignite();\n\nIgniteSet<String> set = ignite.set(\n    \"setName\", // Queue name.\n    null       // Collection configuration.\n);",
      "language": "java",
      "name": "Set"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Collocated vs. Non-Collocated Mode"
}
[/block]
If you plan to create just a few queues or sets containing lots of data, then you would create them in non-collocated mode. This will make sure that about equal portion of each queue or set will be stored on each cluster node. On the other hand, if you plan to have many queues or sets, relatively small in size (compared to the whole cache), then you would most likely create them in collocated mode. In this mode all queue or set elements will be stored on the same cluster node, but about equal amount of queues/sets will be assigned to every node.
A collocated queue and set can be created by setting the `collocated` property of `CollectionConfiguration`, like so:
[block:code]
{
  "codes": [
    {
      "code": "Ignite ignite = Ignition.ignite();\n\nCollectionConfiguration colCfg = new CollectionConfiguration();\n\ncolCfg.setCollocated(true); \n\n// Create collocated queue.\nIgniteQueue<String> queue = ignite.queue(\"queueName\", 0, colCfg);\n\n// Create collocated set.\nIgniteSet<String> set = ignite.set(\"setName\", colCfg);",
      "language": "java",
      "name": "Queue"
    },
    {
      "code": "Ignite ignite = Ignition.ignite();\n\nCollectionConfiguration colCfg = new CollectionConfiguration();\n\ncolCfg.setCollocated(true); \n\n// Create collocated set.\nIgniteSet<String> set = ignite.set(\"setName\", colCfg);",
      "language": "text",
      "name": "Set"
    }
  ]
}
[/block]

[block:callout]
{
  "type": "info",
  "title": "",
  "body": "Non-collocated mode only makes sense for and is only supported for `PARTITIONED` caches."
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Bounded Queues"
}
[/block]
Bounded queues allow users to have many queues with maximum size which gives a better control over the overall cache capacity. They can be either *collocated* or *non-collocated*. When bounded queues are relatively small and used in collocated mode, all queue operations become extremely fast. Moreover, when used in combination with compute grid, users can collocate their compute jobs with cluster nodes on which queues are located to make sure that all operations are local and there is none (or minimal) data distribution. 

Here is an example of how a job could be send directly to the node on which a queue resides:
[block:code]
{
  "codes": [
    {
      "code": "Ignite ignite = Ignition.ignite();\n\nCollectionConfiguration colCfg = new CollectionConfiguration();\n\ncolCfg.setCollocated(true); \n\ncolCfg.setCacheName(\"cacheName\");\n\nfinal IgniteQueue<String> queue = ignite.queue(\"queueName\", 20, colCfg);\n \n// Add queue elements (queue is cached on some node).\nfor (int i = 0; i < 20; i++)\n    queue.add(\"Value \" + Integer.toString(i));\n \nIgniteRunnable queuePoller = new IgniteRunnable() {\n    @Override public void run() throws IgniteException {\n        // Poll is local operation due to collocation.\n        for (int i = 0; i < 20; i++)\n            System.out.println(\"Polled element: \" + queue.poll());\n    }\n};\n\n// Drain queue on the node where the queue is cached.\nignite.compute().affinityRun(\"cacheName\", \"queueName\", queuePoller);",
      "language": "java",
      "name": "Queue"
    }
  ]
}
[/block]

[block:callout]
{
  "type": "success",
  "body": "Refer to [Collocate Compute and Data](doc:collocate-compute-and-data) section for more information on collocating computations with data."
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Cache Queues and Load Balancing"
}
[/block]
Given that elements will remain in the queue until someone takes them, and that no two nodes should ever receive the same element from the queue, cache queues can be used as an alternate work distribution and load balancing approach within Ignite. 

For example, you could simply add computations, such as instances of `IgniteRunnable` to a queue, and have threads on remote nodes call `IgniteQueue.take()`  method which will block if queue is empty. Once the `take()` method will return a job, a thread will process it and call `take()` again to get the next job. Given this approach, threads on remote nodes will only start working on the next job when they have completed the previous one, hence creating ideally balanced system where every node only takes the number of jobs it can process, and not more.
[block:api-header]
{
  "type": "basic",
  "title": "Collection Configuration"
}
[/block]
Ignite collections can be in configured in API via `CollectionConfiguration` (see above examples). The following configuration parameters can be used:
[block:parameters]
{
  "data": {
    "h-0": "Setter Method",
    "0-0": "`setCollocated(boolean)`",
    "1-0": "`setCacheName(String)`",
    "h-1": "Description",
    "h-2": "Default",
    "0-2": "false",
    "0-1": "Sets collocation mode.",
    "1-1": "Set name of the cache to store this collection.",
    "1-2": "null"
  },
  "cols": 3,
  "rows": 2
}
[/block]