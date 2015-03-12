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

Distributed computations are performed in parallel fashion to gain **high performance**, **low latency**, and **linear scalability**. Ignite compute grid provides a set of simple APIs that allow users distribute computations and data processing across multiple computers in the cluster. Distributed parallel processing is based on the ability to take any computation and execute it on any set of cluster nodes and return the results back.
[block:image]
{
  "images": [
    {
      "image": [
        "https://www.filepicker.io/api/file/zrJB0GshRdS3hLn0QGlI",
        "in_memory_compute.png",
        "400",
        "301",
        "#da4204",
        ""
      ]
    }
  ]
}
[/block]
##Features
  * [Distributed Closure Execution](doc:distributed-closures)
  * [MapReduce & ForkJoin Processing](doc:compute-tasks)
  * [Clustered Executor Service](doc:executor-service)
  * [Collocation of Compute and Data](doc:collocate-compute-and-data) 
  * [Load Balancing](doc:load-balancing) 
  * [Fault Tolerance](doc:fault-tolerance)
  * [Job State Checkpointing](doc:checkpointing) 
  * [Job Scheduling](doc:job-scheduling) 
[block:api-header]
{
  "type": "basic",
  "title": "IgniteCompute"
}
[/block]
`IgniteCompute` interface provides methods for running many types of computations over nodes in a cluster or a cluster group. These methods can be used to execute Tasks or Closures in distributed fashion.

All jobs and closures are [guaranteed to be executed](doc:fault-tolerance) as long as there is at least one node standing. If a job execution is rejected due to lack of resources, a failover mechanism is provided. In case of failover, the load balancer picks the next available node to execute the job. Here is how you can get an `IgniteCompute` instance:
[block:code]
{
  "codes": [
    {
      "code": "Ignite ignite = Ignition.ignite();\n\n// Get compute instance over all nodes in the cluster.\nIgniteCompute compute = ignite.compute();",
      "language": "java"
    }
  ]
}
[/block]
You can also limit the scope of computations to a [Cluster Group](doc:cluster-groups). In this case, computation will only execute on the nodes within the cluster group.
[block:code]
{
  "codes": [
    {
      "code": "Ignite ignite = Ignitition.ignite();\n\nClusterGroup remoteGroup = ignite.cluster().forRemotes();\n\n// Limit computations only to remote nodes (exclude local node).\nIgniteCompute compute = ignite.compute(remoteGroup);",
      "language": "java"
    }
  ]
}
[/block]