/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//tag::cluster-groups[]
Ignite ignite = Ignition::Get();
ClusterGroup cluster = ignite.GetCluster().AsClusterGroup();
// All nodes on which cache with name "myCache" is deployed,
// either in client or server mode.
ClusterGroup cacheGroup = cluster.ForCacheNodes("myCache");
// All data nodes responsible for caching data for "myCache".
ClusterGroup dataGroup = cluster.ForDataNodes("myCache");
// All client nodes that access "myCache".
ClusterGroup clientGroup = cluster.ForClientNodes("myCache");
//end::cluster-groups[]
