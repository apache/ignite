/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Cluster
{
    /// <summary>
    /// Represents cluster node filter.
    /// </summary>
    public interface IClusterNodeFilter
    {
        /// <summary>
        /// Returns a value indicating whether provided node satisfies this predicate.
        /// </summary>
        /// <param name="node">Cluster node.</param>
        /// <returns>Value indicating whether provided node satisfies this predicate.</returns>
        bool Invoke(IClusterNode node);
    }
}