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

namespace Apache.Ignite.Core.Services
{
    using Apache.Ignite.Core.Cluster;

    /// <summary>
    /// Service configuration.
    /// </summary>
    public class ServiceConfiguration
    {
        /// <summary>
        /// Gets or sets the service name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the service instance.
        /// </summary>
        public IService Service { get; set; }

        /// <summary>
        /// Gets or sets the total number of deployed service instances in the cluster, 0 for unlimited.
        /// </summary>
        public int TotalCount { get; set; }

        /// <summary>
        /// Gets or sets maximum number of deployed service instances on each node, 0 for unlimited.
        /// </summary>
        public int MaxPerNodeCount { get; set; }

        /// <summary>
        /// Gets or sets cache name used for key-to-node affinity calculation.
        /// </summary>
        public string CacheName { get; set; }

        /// <summary>
        /// Gets or sets affinity key used for key-to-node affinity calculation.
        /// </summary>
        public object AffinityKey { get; set; }

        /// <summary>
        /// Gets or sets node filter used to filter nodes on which the service will be deployed.
        /// </summary>
        public IClusterNodeFilter NodeFilter { get; set; } 
    }
}