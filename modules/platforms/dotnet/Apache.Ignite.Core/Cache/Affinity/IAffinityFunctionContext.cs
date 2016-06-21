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

namespace Apache.Ignite.Core.Cache.Affinity
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Events;

    /// <summary>
    /// Affinity function context to be passed to <see cref="IAffinityFunction" />.
    /// </summary>
    public interface IAffinityFunctionContext
    {
        /// <summary>
        /// Gets the affinity assignment for given partition on previous topology version.
        /// First node in returned list is a primary node, other nodes are backups.
        /// </summary>
        /// <param name="partition">The partition to get previous assignment for.</param>
        /// <returns>
        /// List of nodes assigned to a given partition on previous topology version or <code>null</code> 
        /// if this information is not available.
        /// </returns>
        ICollection<IClusterNode> GetPreviousAssignment(int partition);

        /// <summary>
        /// Gets number of backups for new assignment.
        /// </summary>
        int Backups { get; }

        /// <summary>
        /// Gets the current topology snapshot. Snapshot will contain only nodes on which the particular 
        /// cache is configured. List of passed nodes is guaranteed to be sorted in a same order 
        /// on all nodes on which partition assignment is performed.
        /// </summary>
        ICollection<IClusterNode> CurrentTopologySnapshot { get; }

        /// <summary>
        /// Gets the current topology version.
        /// </summary>
        AffinityTopologyVersion CurrentTopologyVersion { get; }

        /// <summary>
        /// Gets the discovery event that caused the topology change.
        /// </summary>
        DiscoveryEvent DiscoveryEvent { get; }
    }
}