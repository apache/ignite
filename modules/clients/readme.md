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

<center>
![Ignite Logo](https://ignite.incubator.apache.org/images/logo3.png "Ignite Logo")
</center>

## Java Client README

Java Client is a **lightweight gateway** to Ignite nodes.

Client communicates with grid nodes via REST interface and provides reduced but powerful subset of Ignite API.
Java Client allows to use Ignite features from devices and environments where fully-functional Ignite node
could not (*or should not*) be started.

## Client vs Grid Node
Note that for performance and ease-of-use reasons, you should always prefer to start grid node in your cluster instead of remote client. Grid node will generally perform a lot faster and can easily exhibit client-only functionality by excluding it from task/job execution and from caching data. 

For example, you can prevent a grid node from participating in caching by setting `CacheConfiguration.setDistributionMode(...)` value to either `CLIENT_ONLY` or `NEAR_ONLY`.

