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