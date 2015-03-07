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

`ClusterGroup` represents a logical grouping of cluster nodes. 

In Ignite all nodes are equal by design, so you don't have to start any nodes in specific order, or assign any specific roles to them. However, Ignite allows users to logically group cluster nodes for any application specific purpose. For example, you may wish to deploy a service only on remote nodes, or assign a role of "worker" to some worker nodes for job execution.
[block:callout]
{
  "type": "success",
  "body": "Note that `IgniteCluster` interface is also a cluster group which includes all nodes in the cluster."
}
[/block]
You can limit job execution, service deployment, messaging, events, and other tasks to run only within some cluster group. For example, here is how to broadcast a job only to remote nodes (excluding the local node).
[block:code]
{
  "codes": [
    {
      "code": "final Ignite ignite = Ignition.ignite();\n\nIgniteCluster cluster = ignite.cluster();\n\n// Get compute instance which will only execute\n// over remote nodes, i.e. not this node.\nIgniteCompute compute = ignite.compute(cluster.forRemotes());\n\n// Broadcast to all remote nodes and print the ID of the node \n// on which this closure is executing.\ncompute.broadcast(() -> System.out.println(\"Hello Node: \" + ignite.cluster().localNode().id());\n",
      "language": "java",
      "name": "broadcast"
    },
    {
      "code": "final Ignite ignite = Ignition.ignite();\n\nIgniteCluster cluster = ignite.cluster();\n\n// Get compute instance which will only execute\n// over remote nodes, i.e. not this node.\nIgniteCompute compute = ignite.compute(cluster.forRemotes());\n\n// Broadcast closure only to remote nodes.\ncompute.broadcast(new IgniteRunnable() {\n    @Override public void run() {\n        // Print ID of the node on which this runnable is executing.\n        System.out.println(\">>> Hello Node: \" + ignite.cluster().localNode().id());\n    }\n}",
      "language": "java",
      "name": "java7 broadcast"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Predefined Cluster Groups"
}
[/block]
You can create cluster groups based on any predicate. For convenience Ignite comes with some predefined cluster groups.

Here are examples of some cluster groups available on `ClusterGroup` interface.
[block:code]
{
  "codes": [
    {
      "code": "IgniteCluster cluster = ignite.cluster();\n\n// Cluster group with remote nodes, i.e. other than this node.\nClusterGroup remoteGroup = cluster.forRemotes();",
      "language": "java",
      "name": "Remote Nodes"
    },
    {
      "code": "IgniteCluster cluster = ignite.cluster();\n\n// All nodes on wich cache with name \"myCache\" is deployed.\nClusterGroup cacheGroup = cluster.forCache(\"myCache\");",
      "language": "java",
      "name": "Cache Nodes"
    },
    {
      "code": "IgniteCluster cluster = ignite.cluster();\n\n// All nodes with attribute \"ROLE\" equal to \"worker\".\nClusterGroup attrGroup = cluster.forAttribute(\"ROLE\", \"worker\");",
      "language": "java",
      "name": "Nodes With Attributes"
    },
    {
      "code": "IgniteCluster cluster = ignite.cluster();\n\n// Cluster group containing one random node.\nClusterGroup randomGroup = cluster.forRandom();\n\n// First (and only) node in the random group.\nClusterNode randomNode = randomGroup.node();",
      "language": "java",
      "name": "Random Node"
    },
    {
      "code": "IgniteCluster cluster = ignite.cluster();\n\n// Pick random node.\nClusterGroup randomNode = cluster.forRandeom();\n\n// All nodes on the same physical host as the random node.\nClusterGroup cacheNodes = cluster.forHost(randomNode);",
      "language": "java",
      "name": "Host Nodes"
    },
    {
      "code": "IgniteCluster cluster = ignite.cluster();\n\n// Dynamic cluster group representing the oldest cluster node.\n// Will automatically shift to the next oldest, if the oldest\n// node crashes.\nClusterGroup oldestNode = cluster.forOldest();",
      "language": "java",
      "name": "Oldest Node"
    },
    {
      "code": "IgniteCluster cluster = ignite.cluster();\n\n// Cluster group with only this (local) node in it.\nClusterGroup localGroup = cluster.forLocal();\n\n// Local node.\nClusterNode localNode = localGroup.node();",
      "language": "java",
      "name": "Local Node"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Cluster Groups with Node Attributes"
}
[/block]
The unique characteristic of Ignite is that all grid nodes are equal. There are no master or server nodes, and there are no worker or client nodes either. All nodes are equal from Ignite’s point of view - however, users can configure nodes to be masters and workers, or clients and data nodes. 

All cluster nodes on startup automatically register all environment and system properties as node attributes. However, users can choose to assign their own node attributes through configuration:
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.IgniteConfiguration\">\n    ...\n    <property name=\"userAttributes\">\n        <map>\n            <entry key=\"ROLE\" value=\"worker\"/>\n        </map>\n    </property>\n    ...\n</bean>",
      "language": "xml"
    },
    {
      "code": "IgniteConfiguration cfg = new IgniteConfiguration();\n\nMap<String, String> attrs = Collections.singletonMap(\"ROLE\", \"worker\");\n\ncfg.setUserAttributes(attrs);\n\n// Start Ignite node.\nIgnite ignite = Ignition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]

[block:callout]
{
  "type": "success",
  "body": "All environment variables and system properties are automatically registered as node attributes on startup."
}
[/block]

[block:callout]
{
  "type": "success",
  "body": "Node attributes are available via `ClusterNode.attribute(\"propertyName\")` method."
}
[/block]
Following example shows how to get the nodes where "worker" attribute has been set.
[block:code]
{
  "codes": [
    {
      "code": "IgniteCluster cluster = ignite.cluster();\n\nClusterGroup workerGroup = cluster.forAttribute(\"ROLE\", \"worker\");\n\nCollection<GridNode> workerNodes = workerGroup.nodes();",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Custom Cluster Groups"
}
[/block]
You can define dynamic cluster groups based on some predicate. Such cluster groups will always only include the nodes that pass the predicate.

Here is an example of a cluster group over nodes that have less than 50% CPU utilization. Note that the nodes in this group will change over time based on their CPU load.
[block:code]
{
  "codes": [
    {
      "code": "IgniteCluster cluster = ignite.cluster();\n\n// Nodes with less than 50% CPU load.\nClusterGroup readyNodes = cluster.forPredicate((node) -> node.metrics().getCurrentCpuLoad() < 0.5);",
      "language": "java",
      "name": "custom group"
    },
    {
      "code": "IgniteCluster cluster = ignite.cluster();\n\n// Nodes with less than 50% CPU load.\nClusterGroup readyNodes = cluster.forPredicate(\n    new IgnitePredicate<ClusterNode>() {\n        @Override public boolean apply(ClusterNode node) {\n            return node.metrics().getCurrentCpuLoad() < 0.5;\n        }\n    }\n));",
      "language": "java",
      "name": "java7 custom group"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Combining Cluster Groups"
}
[/block]
You can combine cluster groups by nesting them within each other. For example, the following code snippet shows how to get a random remote node by combing remote group with random group.
[block:code]
{
  "codes": [
    {
      "code": "// Group containing oldest node out of remote nodes.\nClusterGroup oldestGroup = cluster.forRemotes().forOldest();\n\nClusterNode oldestNode = oldestGroup.node();",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Getting Nodes from Cluster Groups"
}
[/block]
You can get to various cluster group nodes as follows:
[block:code]
{
  "codes": [
    {
      "code": "ClusterGroup remoteGroup = cluster.forRemotes();\n\n// All cluster nodes in the group.\nCollection<ClusterNode> grpNodes = remoteGroup.nodes();\n\n// First node in the group (useful for groups with one node).\nClusterNode node = remoteGroup.node();\n\n// And if you know a node ID, get node by ID.\nUUID myID = ...;\n\nnode = remoteGroup.node(myId);",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Cluster Group Metrics"
}
[/block]
Ignite automatically collects metrics about all the cluster nodes. The cool thing about cluster groups is that it automatically aggregates the metrics across all the nodes in the group and provides proper averages, mins, and maxes within the group.

Group metrics are available via `ClusterMetrics` interface which contains over 50 various metrics (note that the same metrics are available for individual cluster nodes as well).

Here is an example of getting some metrics, including average CPU load and used heap, across all remote nodes:
[block:code]
{
  "codes": [
    {
      "code": "// Cluster group with remote nodes, i.e. other than this node.\nClusterGroup remoteGroup = ignite.cluster().forRemotes();\n\n// Cluster group metrics.\nClusterMetrics metrics = remoteGroup.metrics();\n\n// Get some metric values.\ndouble cpuLoad = metrics.getCurrentCpuLoad();\nlong usedHeap = metrics.getHeapMemoryUsed();\nint numberOfCores = metrics.getTotalCpus();\nint activeJobs = metrics.getCurrentActiveJobs();",
      "language": "java"
    }
  ]
}
[/block]