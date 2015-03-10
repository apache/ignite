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

The closures and tasks that you use for your computations may be of any custom class, including anonymous classes. In Ignite, the remote nodes will automatically become aware of those classes, and you won't need to explicitly deploy or move any .jar files to any remote nodes. 

Such behavior is possible due to peer class loading (P2P class loading), a special **distributed  ClassLoader** in Ignite for inter-node byte-code exchange. With peer-class-loading enabled, you don't have to manually deploy your Java or Scala code on each node in the grid and re-deploy it each time it changes.

A code example like below would run on all remote nodes due to peer class loading, without any explicit deployment step.
[block:code]
{
  "codes": [
    {
      "code": "IgniteCluster cluster = ignite.cluster();\n\n// Compute instance over remote nodes.\nIgniteCompute compute = ignite.compute(cluster.forRemotes());\n\n// Print hello message on all remote nodes.\ncompute.broadcast(() -> System.out.println(\"Hello node: \" + cluster.localNode().id());",
      "language": "java"
    }
  ]
}
[/block]
Here is how peer class loading can be configured:
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.IgniteConfiguration\">\n    ...   \n    <!-- Explicitly enable peer class loading. -->\n    <property name=\"peerClassLoadingEnabled\" value=\"true\"/>\n    ...\n</bean>",
      "language": "xml"
    },
    {
      "code": "IgniteConfiguration cfg = new IgniteConfiguration();\n\ncfg.setPeerClassLoadingEnabled(true);\n\n// Start Ignite node.\nIgnite ignite = Ignition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]
Peer class loading sequence works as follows:
1. Ignite will check if class is available on local classpath (i.e. if it was loaded at system startup), and if it was, it will be returned. No class loading from a peer node will take place in this case.
2. If class is not locally available, then a request will be sent to the originating node to provide class definition. Originating node will send class byte-code definition and the class will be loaded on the worker node. This happens only once per class - once class definition is loaded on a node, it will never have to be loaded again.
[block:callout]
{
  "type": "warning",
  "title": "Development vs Production",
  "body": "It is recommended that peer-class-loading is disabled in production. Generally you want to have a controlled production environment without any magic."
}
[/block]

[block:callout]
{
  "type": "warning",
  "title": "Auto-Clearing Caches for Hot Redeployment",
  "body": "Whenever you change class definitions for the data stored in cache, Ignite will automatically clear the caches for previous class definitions before peer-deploying the new data to avoid class-loading conflicts."
}
[/block]

[block:callout]
{
  "type": "info",
  "title": "3rd Party Libraries",
  "body": "When utilizing peer class loading, you should be aware of the libraries that get loaded from peer nodes vs. libraries that are already available locally in the class path. Our suggestion is to include all 3rd party libraries into class path of every node. This way you will not transfer megabytes of 3rd party classes to remote nodes every time you change a line of code."
}
[/block]