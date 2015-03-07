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

`IgniteServices` facade allows to deploy any number of services on any of the grid nodes. However, the most commonly used feature is to deploy singleton services on the cluster. Ignite will manage the singleton contract regardless of topology changes and node crashes.
[block:callout]
{
  "type": "info",
  "body": "Note that in case of topology changes, due to network delays, there may be a temporary situation when a singleton service instance will be active on more than one node (e.g. crash detection delay)."
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Cluster Singleton"
}
[/block]
You can deploy a cluster-wide singleton service. Ignite will guarantee that there is always one instance of the service in the cluster. In case the cluster node on which the service was deployed crashes or stops, Ignite will automatically redeploy it on another node. However, if the node on which the service is deployed remains in topology, then the service will always be deployed on that node only, regardless of topology changes.
[block:code]
{
  "codes": [
    {
      "code": "IgniteServices svcs = ignite.services();\n\nsvcs.deployClusterSingleton(\"myClusterSingleton\", new MyService());",
      "language": "java"
    }
  ]
}
[/block]
The above method is analogous to calling 
[block:code]
{
  "codes": [
    {
      "code": "svcs.deployMultiple(\"myClusterSingleton\", new MyService(), 1, 1)",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Node Singleton"
}
[/block]
You can deploy a per-node singleton service. Ignite will guarantee that there is always one instance of the service running on each node. Whenever new nodes are started within the cluster group, Ignite will automatically deploy one instance of the service on every new node.
[block:code]
{
  "codes": [
    {
      "code": "IgniteServices svcs = ignite.services();\n\nsvcs.deployNodeSingleton(\"myNodeSingleton\", new MyService());",
      "language": "java"
    }
  ]
}
[/block]
The above method is analogous to calling 
[block:code]
{
  "codes": [
    {
      "code": "svcs.deployMultiple(\"myNodeSingleton\", new MyService(), 0, 1);",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Cache Key Affinity Singleton"
}
[/block]
You can deploy one instance of this service on the primary node for a given affinity key. Whenever topology changes and primary key node assignment changes, Ignite will always make sure that the service is undeployed on the previous primary node and is deployed on the new primary node. 
[block:code]
{
  "codes": [
    {
      "code": "IgniteServices svcs = ignite.services();\n\nsvcs.deployKeyAffinitySingleton(\"myKeySingleton\", new MyService(), \"myCache\", new MyCacheKey());",
      "language": "java"
    }
  ]
}
[/block]
The above method is analogous to calling
[block:code]
{
  "codes": [
    {
      "code": "IgniteServices svcs = ignite.services();\n\nServiceConfiguration cfg = new ServiceConfiguration();\n \ncfg.setName(\"myKeySingleton\");\ncfg.setService(new MyService());\ncfg.setCacheName(\"myCache\");\ncfg.setAffinityKey(new MyCacheKey());\ncfg.setTotalCount(1);\ncfg.setMaxPerNodeCount(1);\n \nsvcs.deploy(cfg);",
      "language": "java"
    }
  ]
}
[/block]