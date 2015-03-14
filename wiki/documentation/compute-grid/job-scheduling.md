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

In Ignite, jobs are mapped to cluster nodes during initial task split or closure execution on the  client side. However, once jobs arrive to the designated nodes, then need to be ordered for execution. By default, jobs are submitted to a thread pool and are executed in random order.  However, if you need to have a fine-grained control over job ordering, you can enable `CollisionSpi`.
[block:api-header]
{
  "type": "basic",
  "title": "FIFO Ordering"
}
[/block]
`FifoQueueCollisionSpi` allows a certain number of jobs in first-in first-out order to proceed without interruptions. All other jobs will be put on a waiting list until their turn.

Number of parallel jobs is controlled by `parallelJobsNumber` configuration parameter. Default is number of cores times 2.

##One at a Time
Note that by setting `parallelJobsNumber` to 1, you can guarantee that all jobs will be executed one-at-a-time, and no two jobs will be executed concurrently.
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.IgniteConfiguration\" singleton=\"true\">\n  ...\n  <property name=\"collisionSpi\">\n    <bean class=\"org.apache.ignite.spi.collision.fifoqueue.FifoQueueCollisionSpi\">\n      <!-- Execute one job at a time. -->\n      <property name=\"parallelJobsNumber\" value=\"1\"/>\n    </bean>\n  </property>\n  ...\n</bean>",
      "language": "xml"
    },
    {
      "code": "FifoQueueCollisionSpi colSpi = new FifoQueueCollisionSpi();\n \n// Execute jobs sequentially, one at a time, \n// by setting parallel job number to 1.\ncolSpi.setParallelJobsNumber(1);\n \nIgniteConfiguration cfg = new IgniteConfiguration();\n \n// Override default collision SPI.\ncfg.setCollisionSpi(colSpi);\n \n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java",
      "name": null
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Priority Ordering"
}
[/block]
`PriorityQueueCollisionSpi` allows to assign priorities to individual jobs, so jobs with higher priority will be executed ahead of lower priority jobs. 

#Task Priorities
Task priorities are set in the [task session](/docs/compute-tasks#distributed-task-session) via `grid.task.priority` attribute. If no priority has been assigned to a task, then default priority of 0 is used.

Below is an example showing how task priority can be set. 
[block:code]
{
  "codes": [
    {
      "code": "public class MyUrgentTask extends ComputeTaskSplitAdapter<Object, Object> {\n  // Auto-injected task session.\n  @TaskSessionResource\n  private GridTaskSession taskSes = null;\n \n  @Override\n  protected Collection<ComputeJob> split(int gridSize, Object arg) {\n    ...\n    // Set high task priority.\n    taskSes.setAttribute(\"grid.task.priority\", 10);\n \n    List<ComputeJob> jobs = new ArrayList<>(gridSize);\n    \n    for (int i = 1; i <= gridSize; i++) {\n      jobs.add(new GridJobAdapter() {\n        ...\n      });\n    }\n    ...\n      \n    // These jobs will be executed with higher priority.\n    return jobs;\n  }\n}",
      "language": "java"
    }
  ]
}
[/block]
Just like with [FIFO Ordering](#fifo-ordering), number of parallel jobs is controlled by `parallelJobsNumber` configuration parameter. 

##Configuration
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.IgniteConfiguration\" singleton=\"true\">\n\t...\n\t<property name=\"collisionSpi\">\n\t\t<bean class=\"org.apache.ignite.spi.collision.priorityqueue.PriorityQueueCollisionSpi\">\n      <!-- \n        Change the parallel job number if needed.\n        Default is number of cores times 2.\n      -->\n\t\t\t<property name=\"parallelJobsNumber\" value=\"5\"/>\n\t\t</bean>\n\t</property>\n\t...\n</bean>",
      "language": "xml"
    },
    {
      "code": "PriorityQueueCollisionSpi colSpi = new PriorityQueueCollisionSpi();\n\n// Change the parallel job number if needed.\n// Default is number of cores times 2.\ncolSpi.setParallelJobsNumber(5);\n \nIgniteConfiguration cfg = new IgniteConfiguration();\n \n// Override default collision SPI.\ncfg.setCollisionSpi(colSpi);\n \n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java",
      "name": ""
    }
  ]
}
[/block]