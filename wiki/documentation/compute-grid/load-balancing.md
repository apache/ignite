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

Load balancing component balances job distribution among cluster nodes. In Ignite load balancing is achieved via `LoadBalancingSpi` which controls load on all nodes and makes sure that every node in the cluster is equally loaded. In homogeneous environments with homogeneous tasks load balancing is achieved by random or round-robin policies. However, in many other use cases, especially under uneven load, more complex adaptive load-balancing policies may be needed.
[block:callout]
{
  "type": "info",
  "body": "Note that load balancing is triggered whenever your jobs are not collocated with data or have no real preference on which node to execute. If [Collocation Of Compute and Data](doc:collocate-compute-and-data) is used, then data affinity takes priority over load balancing.",
  "title": "Data Affinity"
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Round-Robin Load Balancing"
}
[/block]
`RoundRobinLoadBalancingSpi` iterates through nodes in round-robin fashion and picks the next sequential node. Two modes of operation are supported: per-task and global.

##Per-Task Mode
When configured in per-task mode, implementation will pick a random node at the beginning of every task execution and then sequentially iterate through all nodes in topology starting from the picked node. This is the default configuration For cases when split size is equal to the number of nodes, this mode guarantees that all nodes will participate in the split.

##Global Mode
When configured in global mode, a single sequential queue of nodes is maintained for all tasks and the next node in the queue is picked every time. In this mode (unlike in per-task mode) it is possible that even if split size may be equal to the number of nodes, some jobs within the same task will be assigned to the same node whenever multiple tasks are executing concurrently.
[block:code]
{
  "codes": [
    {
      "code": "<bean id=\"grid.custom.cfg\" class=\"org.apache.ignite.IgniteConfiguration\" singleton=\"true\">\n  ...\n  <property name=\"loadBalancingSpi\">\n    <bean class=\"org.apache.ignite.spi.loadbalancing.roundrobin.RoundRobinLoadBalancingSpi\">\n      <!-- Set to per-task round-robin mode (this is default behavior). -->\n      <property name=\"perTask\" value=\"true\"/>\n    </bean>\n  </property>\n  ...\n</bean>",
      "language": "xml",
      "name": null
    },
    {
      "code": "RoundRobinLoadBalancingSpi = new RoundRobinLoadBalancingSpi();\n \n// Configure SPI to use per-task mode (this is default behavior).\nspi.setPerTask(true);\n \nIgniteConfiguration cfg = new IgniteConfiguration();\n \n// Override default load balancing SPI.\ncfg.setLoadBalancingSpi(spi);\n \n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Random and Weighted Load Balancing"
}
[/block]
`WeightedRandomLoadBalancingSpi` picks a random node for job execution by default. You can also optionally assign weights to nodes, so nodes with larger weights will end up getting proportionally more jobs routed to them. By default all nodes get equal weight of 10.
[block:code]
{
  "codes": [
    {
      "code": "<bean id=\"grid.custom.cfg\" class=\"org.apache.ignite.IgniteConfiguration\" singleton=\"true\">\n  ...\n  <property name=\"loadBalancingSpi\">\n    <bean class=\"org.apache.ignite.spi.loadbalancing.weightedrandom.WeightedRandomLoadBalancingSpi\">\n      <property name=\"useWeights\" value=\"true\"/>\n      <property name=\"nodeWeight\" value=\"10\"/>\n    </bean>\n  </property>\n  ...\n</bean>",
      "language": "xml"
    },
    {
      "code": "WeightedRandomLoadBalancingSpi = new WeightedRandomLoadBalancingSpi();\n \n// Configure SPI to used weighted random load balancing.\nspi.setUseWeights(true);\n \n// Set weight for the local node.\nspi.setWeight(10);\n \nIgniteConfiguration cfg = new IgniteConfiguration();\n \n// Override default load balancing SPI.\ncfg.setLoadBalancingSpi(spi);\n \n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]