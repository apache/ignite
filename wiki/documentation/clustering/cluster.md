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

Ignite nodes can automatically discover each other. This helps to scale the cluster when needed, without having to restart the whole cluster. Developers can also leverage from Ignite’s hybrid cloud support that allows establishing connection between private cloud and public clouds such as Amazon Web Services, providing them with best of both worlds. 
[block:image]
{
  "images": [
    {
      "image": [
        "https://www.filepicker.io/api/file/KBkahg31S4qWXEBjfoya",
        "ignite_cluster.png",
        "500",
        "350",
        "#f48745",
        ""
      ]
    }
  ]
}
[/block]
##Features
  * Pluggable Design via `IgniteDiscoverySpi`
  * Dynamic topology management
  * Automatic discovery on LAN, WAN, and AWS
  * On-demand and direct deployment
  * Support for virtual clusters and node groupings
[block:api-header]
{
  "type": "basic",
  "title": "IgniteCluster"
}
[/block]
Cluster functionality is provided via `IgniteCluster` interface. You can get an instance of `IgniteCluster` from `Ignite` as follows:
[block:code]
{
  "codes": [
    {
      "code": "Ignite ignite = Ignition.ignite();\n\nIgniteCluster cluster = ignite.cluster();",
      "language": "java"
    }
  ]
}
[/block]
Through `IgniteCluster` interface you can:
 * Start and stop remote cluster nodes
 * Get a list of all cluster members
 * Create logical [Cluster Groups](doc:cluster-groups)
[block:api-header]
{
  "type": "basic",
  "title": "ClusterNode"
}
[/block]
The `ClusterNode` interface has very concise API and deals only with the node as a logical network endpoint in the topology: its globally unique ID, the node metrics, its static attributes set by the user and a few other parameters.
[block:api-header]
{
  "type": "basic",
  "title": "Cluster Node Attributes"
}
[/block]
All cluster nodes on startup automatically register all environment and system properties as node attributes. However, users can choose to assign their own node attributes through configuration:
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.IgniteConfiguration\">\n    ...\n    <property name=\"userAttributes\">\n        <map>\n            <entry key=\"ROLE\" value=\"worker\"/>\n        </map>\n    </property>\n    ...\n</bean>",
      "language": "xml"
    }
  ]
}
[/block]
Following example shows how to get the nodes where "worker" attribute has been set.
[block:code]
{
  "codes": [
    {
      "code": "ClusterGroup workers = ignite.cluster().forAttribute(\"ROLE\", \"worker\");\n\nCollection<GridNode> nodes = workers.nodes();",
      "language": "java"
    }
  ]
}
[/block]

[block:callout]
{
  "type": "success",
  "body": "All node attributes are available via `ClusterNode.attribute(\"propertyName\")` method."
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Cluster Node Metrics"
}
[/block]
Ignite automatically collects metrics for all cluster nodes. Metrics are collected in the background and are updated with every heartbeat message exchanged between cluster nodes.

Node metrics are available via `ClusterMetrics` interface which contains over 50 various metrics (note that the same metrics are available for [Cluster Groups](doc:cluster-groups)  as well).

Here is an example of getting some metrics, including average CPU load and used heap, for the local node:
[block:code]
{
  "codes": [
    {
      "code": "// Local Ignite node.\nClusterNode localNode = cluster.localNode();\n\n// Node metrics.\nClusterMetrics metrics = localNode.metrics();\n\n// Get some metric values.\ndouble cpuLoad = metrics.getCurrentCpuLoad();\nlong usedHeap = metrics.getHeapMemoryUsed();\nint numberOfCores = metrics.getTotalCpus();\nint activeJobs = metrics.getCurrentActiveJobs();",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Local Cluster Node"
}
[/block]
Local grid node is an instance of the `ClusterNode` representing *this* Ignite node. 

Here is an example of how to get a local node:
[block:code]
{
  "codes": [
    {
      "code": "ClusterNode localNode = ignite.cluster().localNode();",
      "language": "java"
    }
  ]
}
[/block]