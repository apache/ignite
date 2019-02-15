/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Services
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Service deployment descriptor.
    /// </summary>
    public interface IServiceDescriptor
    {
        /// <summary>
        /// Gets service name.
        /// </summary>
        /// <returns>
        /// Service name.
        /// </returns>
        string Name { get; }

        /// <summary>
        /// Gets the service type.
        /// </summary>
        /// <value>
        /// Service type.
        /// </value>
        Type Type { get; }

        /// <summary>
        /// Gets maximum allowed total number of deployed services in the grid, 0 for unlimited.
        /// </summary>
        /// <returns>
        /// Maximum allowed total number of deployed services in the grid, 0 for unlimited.
        /// </returns>
        int TotalCount { get; }

        /// <summary>
        /// Gets maximum allowed number of deployed services on each node, 0 for unlimited.
        /// </summary>
        /// <returns>
        /// Maximum allowed total number of deployed services on each node, 0 for unlimited.
        /// </returns>
        int MaxPerNodeCount { get; }

        /// <summary>
        /// Gets cache name used for key-to-node affinity calculation. 
        /// This parameter is optional and is set only when key-affinity service was deployed.
        /// </summary>
        /// <returns>
        /// Cache name, possibly null.
        /// </returns>
        string CacheName { get; }

        /// <summary>
        /// Gets affinity key used for key-to-node affinity calculation. 
        /// This parameter is optional and is set only when key-affinity service was deployed.
        /// </summary>
        /// <value>
        /// Affinity key, possibly null.
        /// </value>
        object AffinityKey { get; }

        /// <summary>
        /// Gets affinity key used for key-to-node affinity calculation. 
        /// This parameter is optional and is set only when key-affinity service was deployed.
        /// </summary>
        /// <returns>
        /// Affinity key, possibly null.
        /// </returns>
        Guid OriginNodeId { get; }

        /// <summary>
        /// Gets service deployment topology snapshot. Service topology snapshot is represented
        /// by number of service instances deployed on a node mapped to node ID.
        /// </summary>
        /// <value>
        /// Map of number of service instances per node ID.
        /// </value>
        IDictionary<Guid, int> TopologySnapshot { get; }
    }
}