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

`ComputeTask` is the Ignite abstraction for the simplified in-memory MapReduce, which is also very close to ForkJoin paradigm. Pure MapReduce was never built for performance and only works well when dealing with off-line batch oriented processing (e.g. Hadoop MapReduce). However, when computing on data that resides in-memory, real-time low latencies and high throughput usually take the highest priority. Also, simplicity of the API becomes very important as well. With that in mind, Ignite introduced the `ComputeTask` API, which is a light-weight MapReduce (or ForkJoin) implementation.
[block:callout]
{
  "type": "info",
  "body": "Use `ComputeTask` only when you need fine-grained control over the job-to-node mapping, or custom fail-over logic. For all other cases you should use simple closure executions on the cluster documented in [Distributed Computations](doc:compute) section."
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "ComputeTask"
}
[/block]
`ComputeTask` defines jobs to execute on the cluster, and the mappings of those jobs to nodes. It also defines how to process (reduce) the job results. All `IgniteCompute.execute(...)` methods execute the given task on the grid. User applications should implement `map(...)` and `reduce(...)` methods of ComputeTask interface.

Tasks are defined by implementing the 2 or 3 methods on `ComputeTask` interface

##Map Method
Method `map(...)` instantiates the jobs and maps them to worker nodes. The method receives the collection of cluster nodes on which the task is run and the task argument. The method should return a map with jobs as keys and mapped worker nodes as values. The jobs are then sent to the mapped nodes and executed there.
[block:callout]
{
  "type": "info",
  "body": "Refer to [ComputeTaskSplitAdapter](#computetasksplitadapter) for simplified implementation of the `map(...)` method."
}
[/block]
##Result Method
Method `result(...)` is called each time a job completes on some cluster node. It receives the result returned by the completed job, as well as the list of all the job results received so far. The method should return a `ComputeJobResultPolicy` instance, indicating what to do next:
  * `WAIT` - wait for all remaining jobs to complete (if any)
  * `REDUCE` - immediately move to reduce step, discarding all the remaining jobs and unreceived yet results
  * `FAILOVER` - failover the job to another node (see Fault Tolerance)
All the received job results will be available in the `reduce(...)` method as well.

##Reduce Method
Method `reduce(...)` is called on reduce step, when all the jobs have completed (or REDUCE result policy was returned from the `result(...)` method). The method receives a list with all the completed results and should return a final result of the computation. 
[block:api-header]
{
  "type": "basic",
  "title": "Compute Task Adapters"
}
[/block]
It is not necessary to implement all 3 methods of the `ComputeTask` API each time you need to define a computation. There is a number of helper classes that let you describe only a particular piece of your logic, leaving out all the rest to Ignite to handle automatically. 

##ComputeTaskAdapter
`ComputeTaskAdapter` defines a default implementation of the `result(...)` method which returns `FAILOVER` policy if a job threw an exception and `WAIT` policy otherwise, thus waiting for all jobs to finish with a result.

##ComputeTaskSplitAdapter
`ComputeTaskSplitAdapter` extends `ComputeTaskAdapter` and adds capability to automatically assign jobs to nodes. It hides the `map(...)` method and adds a new `split(...)` method in which user only needs to provide a collection of the jobs to be executed (the mapping of those jobs to nodes will be handled automatically by the adapter in a load-balanced fashion). 

This adapter is especially useful in homogeneous environments where all nodes are equally suitable for executing jobs and the mapping step can be done implicitly.
[block:api-header]
{
  "type": "basic",
  "title": "ComputeJob"
}
[/block]
All jobs that are spawned by a task are implementations of the `ComputeJob` interface. The `execute()` method of this interface defines the job logic and should return a job result. The `cancel()` method defines the logic in case if the job is discarded (for example, in case when task decides to reduce immediately or to cancel).

##ComputeJobAdapter
Convenience adapter which provides a no-op implementation of the `cancel()` method.
[block:api-header]
{
  "type": "basic",
  "title": "Example"
}
[/block]
Here is an example of `ComputeTask` and `ComputeJob` implementations.
[block:code]
{
  "codes": [
    {
      "code": "IgniteCompute compute = ignite.compute();\n\n// Execute task on the clustr and wait for its completion.\nint cnt = grid.compute().execute(CharacterCountTask.class, \"Hello Grid Enabled World!\");\n \nSystem.out.println(\">>> Total number of characters in the phrase is '\" + cnt + \"'.\");\n \n/**\n * Task to count non-white-space characters in a phrase.\n */\nprivate static class CharacterCountTask extends ComputeTaskSplitAdapter<String, Integer> {\n  // 1. Splits the received string into to words\n  // 2. Creates a child job for each word\n  // 3. Sends created jobs to other nodes for processing. \n  @Override \n  public List<ClusterNode> split(List<ClusterNode> subgrid, String arg) {\n    String[] words = arg.split(\" \");\n\n    List<ComputeJob> jobs = new ArrayList<>(words.length);\n\n    for (final String word : arg.split(\" \")) {\n      jobs.add(new ComputeJobAdapter() {\n        @Override public Object execute() {\n          System.out.println(\">>> Printing '\" + word + \"' on from compute job.\");\n\n          // Return number of letters in the word.\n          return word.length();\n        }\n      });\n    }\n\n    return jobs;\n  }\n\n  @Override \n  public Integer reduce(List<ComputeJobResult> results) {\n    int sum = 0;\n\n    for (ComputeJobResult res : results)\n      sum += res.<Integer>getData();\n\n    return sum;\n  }\n}",
      "language": "java",
      "name": "ComputeTaskSplitAdapter"
    },
    {
      "code": "IgniteCompute compute = ignite.compute();\n\n// Execute task on the clustr and wait for its completion.\nint cnt = grid.compute().execute(CharacterCountTask.class, \"Hello Grid Enabled World!\");\n \nSystem.out.println(\">>> Total number of characters in the phrase is '\" + cnt + \"'.\");\n \n/**\n * Task to count non-white-space characters in a phrase.\n */\nprivate static class CharacterCountTask extends ComputeTaskAdapter<String, Integer> {\n    // 1. Splits the received string into to words\n    // 2. Creates a child job for each word\n    // 3. Sends created jobs to other nodes for processing. \n    @Override \n    public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, String arg) {\n        String[] words = arg.split(\" \");\n      \n        Map<ComputeJob, ClusterNode> map = new HashMap<>(words.length);\n        \n        Iterator<ClusterNode> it = subgrid.iterator();\n         \n        for (final String word : arg.split(\" \")) {\n            // If we used all nodes, restart the iterator.\n            if (!it.hasNext())\n                it = subgrid.iterator();\n             \n            ClusterNode node = it.next();\n                \n            map.put(new ComputeJobAdapter() {\n                @Override public Object execute() {\n                    System.out.println(\">>> Printing '\" + word + \"' on this node from grid job.\");\n                  \n                    // Return number of letters in the word.\n                    return word.length();\n                }\n             }, node);\n        }\n      \n        return map;\n    }\n \n    @Override \n    public Integer reduce(List<ComputeJobResult> results) {\n        int sum = 0;\n      \n        for (ComputeJobResult res : results)\n            sum += res.<Integer>getData();\n      \n        return sum;\n    }\n}",
      "language": "java",
      "name": "ComputeTaskAdapter"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Distributed Task Session"
}
[/block]
Distributed task session is created for every task execution. It is defined by `ComputeTaskSession interface. Task session is visible to the task and all the jobs spawned by it, so attributes set on a task or on a job can be accessed on other jobs.  Task session also allows to receive notifications when attributes are set or wait for an attribute to be set.

The sequence in which session attributes are set is consistent across the task and all job siblings within it. There will never be a case when one job sees attribute A before attribute B, and another job sees attribute B before A.

In the example below, we have all jobs synchronize on STEP1 before moving on to STEP2. 
[block:code]
{
  "codes": [
    {
      "code": "IgniteCompute compute = ignite.commpute();\n\ncompute.execute(new ComputeTasSplitAdapter<Object, Object>() {\n  @Override \n  protected Collection<? extends GridJob> split(int gridSize, Object arg)  {\n    Collection<ComputeJob> jobs = new LinkedList<>();\n\n    // Generate jobs by number of nodes in the grid.\n    for (int i = 0; i < gridSize; i++) {\n      jobs.add(new ComputeJobAdapter(arg) {\n        // Auto-injected task session.\n        @TaskSessionResource\n        private ComputeTaskSession ses;\n        \n        // Auto-injected job context.\n        @JobContextResource\n        private ComputeJobContext jobCtx;\n\n        @Override \n        public Object execute() {\n          // Perform STEP1.\n          ...\n          \n          // Tell other jobs that STEP1 is complete.\n          ses.setAttribute(jobCtx.getJobId(), \"STEP1\");\n          \n          // Wait for other jobs to complete STEP1.\n          for (ComputeJobSibling sibling : ses.getJobSiblings())\n            ses.waitForAttribute(sibling.getJobId(), \"STEP1\", 0);\n          \n          // Move on to STEP2.\n          ...\n        }\n      }\n    }\n  }\n               \n  @Override \n  public Object reduce(List<ComputeJobResult> results) {\n    // No-op.\n    return null;\n  }\n}, null);\n",
      "language": "java"
    }
  ]
}
[/block]