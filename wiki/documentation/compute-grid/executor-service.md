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

[IgniteCompute](doc:compute) provides a convenient API for executing computations on the cluster. However, you can also work directly with standard `ExecutorService` interface from JDK. Ignite provides a cluster-enabled implementation of `ExecutorService` and automatically executes all the computations in load-balanced fashion within the cluster. Your computations also become fault-tolerant and are guaranteed to execute as long as there is at least one node left. You can think of it as a distributed cluster-enabled thread pool. 
[block:code]
{
  "codes": [
    {
      "code": "// Get cluster-enabled executor service.\nExecutorService exec = ignite.executorService();\n \n// Iterate through all words in the sentence and create jobs.\nfor (final String word : \"Print words using runnable\".split(\" \")) {\n  // Execute runnable on some node.\n  exec.submit(new IgniteRunnable() {\n    @Override public void run() {\n      System.out.println(\">>> Printing '\" + word + \"' on this node from grid job.\");\n    }\n  });\n}",
      "language": "java"
    }
  ]
}
[/block]
 
You can also limit the job execution with some subset of nodes from your grid:
[block:code]
{
  "codes": [
    {
      "code": "// Cluster group for nodes where the attribute 'worker' is defined.\nClusterGroup workerGrp = ignite.cluster().forAttribute(\"ROLE\", \"worker\");\n\n// Get cluster-enabled executor service for the above cluster group.\nExecutorService exec = icnite.executorService(workerGrp);\n",
      "language": "java"
    }
  ]
}
[/block]