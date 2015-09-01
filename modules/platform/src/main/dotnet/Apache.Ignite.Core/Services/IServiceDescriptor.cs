/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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