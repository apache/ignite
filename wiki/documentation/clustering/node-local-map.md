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

Often it is useful to share a state between different compute jobs or different deployed services. For this purpose Ignite provides a shared concurrent **node-local-map** available on each node.
[block:code]
{
  "codes": [
    {
      "code": "IgniteCluster cluster = ignite.cluster();\n\nConcurrentMap<String, Integer> nodeLocalMap = cluster.nodeLocalMap():",
      "language": "java"
    }
  ]
}
[/block]
Node-local values are similar to thread locals in a way that these values are not distributed and kept only on the local node. Node-local data can be used by compute jobs to share the state between executions. It can also be used by deployed services as well. 

As an example, let's create a job which increments a node-local counter every time it executes on some node. This way, the node-local counter on each node will tell us how many times a job had executed on that cluster node. 
[block:code]
{
  "codes": [
    {
      "code": "private IgniteCallable<Long> job = new IgniteCallable<Long>() {\n  @IgniteInstanceResource\n  private Ignite ignite;\n  \n  @Override \n  public Long call() {                  \n    // Get a reference to node local.\n    ConcurrentMap<String, AtomicLong> nodeLocalMap = ignite.cluster().nodeLocalMap();\n\n    AtomicLong cntr = nodeLocalMap.get(\"counter\");\n\n    if (cntr == null) {\n      AtomicLong old = nodeLocalMap.putIfAbsent(\"counter\", cntr = new AtomicLong());\n      \n      if (old != null)\n        cntr = old;\n    }\n    \n    return cntr.incrementAndGet();\n  }\n}",
      "language": "java"
    }
  ]
}
[/block]
Now let's execute this job 2 times on the same node and make sure that the value of the counter is 2.
[block:code]
{
  "codes": [
    {
      "code": "ClusterGroup random = ignite.cluster().forRandom();\n\nIgniteCompute compute = ignite.compute(random);\n\n// The first time the counter on the picked node will be initialized to 1.\nLong res = compute.call(job);\n\nassert res == 1;\n\n// Now the counter will be incremented and will have value 2.\nres = compute.call(job);\n\nassert res == 2;",
      "language": "java"
    }
  ]
}
[/block]