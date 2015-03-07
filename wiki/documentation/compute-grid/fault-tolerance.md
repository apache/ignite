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

Ignite supports automatic job failover. In case of a node crash, jobs are automatically transferred to other available nodes for re-execution. However, in Ignite you can also treat any job result as a failure as well. The worker node can still be alive, but it may be running low on CPU, I/O, disk space, etc. There are many conditions that may result in a failure within your application and you can trigger a failover. Moreover, you have the ability to choose to which node a job should be failed over to, as it could be different for different applications or different computations within the same application.

The `FailoverSpi` is responsible for handling the selection of a new node for the execution of a failed job. `FailoverSpi` inspects the failed job and the list of all available grid nodes on which the job execution can be retried. It ensures that the job is not re-mapped to the same node it had failed on. Failover is triggered when the method `ComputeTask.result(...)` returns the `ComputeJobResultPolicy.FAILOVER` policy. Ignite comes with a number of built-in customizable Failover SPI implementations.
[block:api-header]
{
  "type": "basic",
  "title": "At Least Once Guarantee"
}
[/block]
As long as there is at least one node standing, no job will ever be lost.

By default, Ignite will failover all jobs from stopped or crashed nodes automatically. For custom failover behavior, you should implement `ComputeTask.result()` method. The example below triggers failover whenever a job throws any `IgniteException` (or its subclasses):
[block:code]
{
  "codes": [
    {
      "code": "public class MyComputeTask extends ComputeTaskSplitAdapter<String, String> {\n    ...\n      \n    @Override \n    public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {\n        IgniteException err = res.getException();\n     \n        if (err != null)\n            return ComputeJobResultPolicy.FAILOVER;\n    \n        // If there is no exception, wait for all job results.\n        return ComputeJobResultPolicy.WAIT;\n    }\n  \n    ...\n}\n",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Closure Failover"
}
[/block]
Closure failover is by default governed by `ComputeTaskAdapter`, which is triggered if a remote node either crashes or rejects closure execution. This default behavior may be overridden by using `IgniteCompute.withNoFailover()` method, which creates an instance of `IgniteCompute` with a **no-failover flag** set on it . Here is an example:
[block:code]
{
  "codes": [
    {
      "code": "IgniteCompute compute = ignite.compute().withNoFailover();\n\ncompute.apply(() -> {\n    // Do something\n    ...\n}, \"Some argument\");\n",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "AlwaysFailOverSpi"
}
[/block]
`AlwaysFailoverSpi` always reroutes a failed job to another node. Note, that at first an attempt will be made to reroute the failed job to a node that the task was not executed on. If no such nodes are available, then an attempt will be made to reroute the failed job to the nodes that may be running other jobs from the same task. If none of the above attempts succeeded, then the job will not be failed over and null will be returned.

The following configuration parameters can be used to configure `AlwaysFailoverSpi`.
[block:parameters]
{
  "data": {
    "h-0": "Setter Method",
    "h-1": "Description",
    "h-2": "Default",
    "0-0": "`setMaximumFailoverAttempts(int)`",
    "0-1": "Sets the maximum number of attempts to fail-over a failed job to other nodes.",
    "0-2": "5"
  },
  "cols": 3,
  "rows": 1
}
[/block]

[block:code]
{
  "codes": [
    {
      "code": "<bean id=\"grid.custom.cfg\" class=\"org.apache.ignite.IgniteConfiguration\" singleton=\"true\">\n  ...\n  <bean class=\"org.apache.ignite.spi.failover.always.AlwaysFailoverSpi\">\n    <property name=\"maximumFailoverAttempts\" value=\"5\"/>\n  </bean>\n  ...\n</bean>\n",
      "language": "xml"
    },
    {
      "code": "AlwaysFailoverSpi failSpi = new AlwaysFailoverSpi();\n \nIgniteConfiguration cfg = new IgniteConfiguration();\n \n// Override maximum failover attempts.\nfailSpi.setMaximumFailoverAttempts(5);\n \n// Override the default failover SPI.\ncfg.setFailoverSpi(failSpi);\n \n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]