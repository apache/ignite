/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Services
{
    using System;

    /// <summary>
    /// Represents service execution context.
    /// </summary>
    public interface IServiceContext
    {
        /// <summary>
        /// Gets service name.
        /// </summary>
        /// <returns>
        /// Service name.
        /// </returns>
        string Name { get; }

        /// <summary>
        /// Gets service execution ID. Execution ID is guaranteed to be unique across all service deployments.
        /// </summary>
        /// <returns>
        /// Service execution ID.
        /// </returns>
        Guid ExecutionId { get; }

        /// <summary>
        /// Get flag indicating whether service has been cancelled or not.
        /// </summary>
        /// <returns>
        /// Flag indicating whether service has been cancelled or not.
        /// </returns>
        bool IsCancelled { get; }

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
    }
}