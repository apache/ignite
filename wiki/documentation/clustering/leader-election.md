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

When working in distributed environments, sometimes you need to have a guarantee that you always will pick the same node, regardless of the cluster topology changes. Such nodes are usually called **leaders**. 

In many systems electing cluster leaders usually has to do with data consistency and is generally handled via collecting votes from cluster members. Since in Ignite the data consistency is handled by data grid affinity function (e.g. [Rendezvous Hashing](http://en.wikipedia.org/wiki/Rendezvous_hashing)), picking leaders in traditional sense for data consistency outside of the data grid is not really needed.

However, you may still wish to have a *coordinator* node for certain tasks. For this purpose, Ignite lets you automatically always pick either oldest or youngest nodes in the cluster.
[block:callout]
{
  "type": "warning",
  "title": "Use Service Grid",
  "body": "Note that for most *leader* or *singleton-like* use cases, it is recommended to use the **Service Grid** functionality, as it allows to automatically deploy various [Cluster Singleton Services](doc:cluster-singletons) and is usually easier to use."
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Oldest Node"
}
[/block]
Oldest node has a property that it remains constant whenever new nodes are added. The only time when the oldest node in the cluster changes is when it leaves the cluster or crashes.

Here is an example of how to select [Cluster Group](doc:cluster-group) with only the oldest node in it.
[block:code]
{
  "codes": [
    {
      "code": "IgniteCluster cluster = ignite.cluster();\n\n// Dynamic cluster group representing the oldest cluster node.\n// Will automatically shift to the next oldest, if the oldest\n// node crashes.\nClusterGroup oldestNode = cluster.forOldest();",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Youngest Node"
}
[/block]
Youngest node, unlike the oldest node, constantly changes every time a new node joins a cluster. However, sometimes it may still become handy, especially if you need to execute some task only on the newly joined node.

Here is an example of how to select [Cluster Group](doc:cluster-groups) with only the youngest node in it.
[block:code]
{
  "codes": [
    {
      "code": "gniteCluster cluster = ignite.cluster();\n\n// Dynamic cluster group representing the youngest cluster node.\n// Will automatically shift to the next oldest, if the oldest\n// node crashes.\nClusterGroup youngestNode = cluster.forYoungest();",
      "language": "java"
    }
  ]
}
[/block]

[block:callout]
{
  "type": "success",
  "body": "Once the cluster group is obtained, you can use it for executing tasks, deploying services, sending messages, and more."
}
[/block]