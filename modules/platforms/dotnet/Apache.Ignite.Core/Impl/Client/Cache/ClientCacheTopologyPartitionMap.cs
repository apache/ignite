/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Impl.Client.Cache
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Cache.Affinity;

    /// <summary>
    /// Partition maps for specific topology version.
    /// </summary>
    internal class ClientCacheTopologyPartitionMap
    {
        /** */
        private readonly Dictionary<int, ClientCachePartitionMap> _cachePartitionMap;

        /** */
        private readonly AffinityTopologyVersion _affinityTopologyVersion;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientCacheTopologyPartitionMap"/> class.
        /// </summary>
        /// <param name="cachePartitionMap">Partition map.</param>
        /// <param name="affinityTopologyVersion">Topology version.</param>
        public ClientCacheTopologyPartitionMap(
            Dictionary<int, ClientCachePartitionMap> cachePartitionMap,
            AffinityTopologyVersion affinityTopologyVersion)
        {
            Debug.Assert(cachePartitionMap != null);

            _cachePartitionMap = cachePartitionMap;
            _affinityTopologyVersion = affinityTopologyVersion;
        }

        /// <summary>
        /// Gets the cache partition map.
        /// </summary>
        public Dictionary<int, ClientCachePartitionMap> CachePartitionMap
        {
            get { return _cachePartitionMap; }
        }

        /// <summary>
        /// Gets the affinity topology version.
        /// </summary>
        public AffinityTopologyVersion AffinityTopologyVersion
        {
            get { return _affinityTopologyVersion; }
        }
    }
}
