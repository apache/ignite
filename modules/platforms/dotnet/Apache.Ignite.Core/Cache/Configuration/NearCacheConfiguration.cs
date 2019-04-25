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

namespace Apache.Ignite.Core.Cache.Configuration
{
    using System.ComponentModel;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Eviction;

    /// <summary>
    /// Defines near cache configuration.
    /// <para />
    /// Distributed cache can also be fronted by a Near cache, which is a smaller local cache that stores most 
    /// recently or most frequently accessed data. 
    /// Just like with a partitioned cache, the user can control the size of the near cache and its eviction policies. 
    /// </summary>
    public class NearCacheConfiguration
    {
        /// <summary> Initial default near cache size. </summary>
        public const int DefaultNearStartSize = 1500000 / 4;

        /// <summary>
        /// Initializes a new instance of the <see cref="NearCacheConfiguration"/> class.
        /// </summary>
        public NearCacheConfiguration()
        {
            NearStartSize = DefaultNearStartSize;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="NearCacheConfiguration"/> class.
        /// </summary>
        internal NearCacheConfiguration(IBinaryRawReader reader)
        {
            NearStartSize = reader.ReadInt();
            EvictionPolicy = EvictionPolicyBase.Read(reader);
        }

        /// <summary>
        /// Writes to the specified writer.
        /// </summary>
        internal void Write(IBinaryRawWriter writer)
        {
            writer.WriteInt(NearStartSize);
            EvictionPolicyBase.Write(writer, EvictionPolicy);
        }

        /// <summary>
        /// Gets or sets the eviction policy.
        /// Null value means disabled evictions.
        /// </summary>
        public IEvictionPolicy EvictionPolicy { get; set; }

        /// <summary>
        /// Gets or sets the initial cache size for near cache which will be used 
        /// to pre-create internal hash table after start.
        /// </summary>
        [DefaultValue(DefaultNearStartSize)]
        public int NearStartSize { get; set; }
    }
}
