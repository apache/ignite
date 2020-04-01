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
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Native .NET near cache configuration.
    /// </summary>
    public class PlatformNearCacheConfiguration
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="NearCacheConfiguration"/> class.
        /// </summary>
        public PlatformNearCacheConfiguration()
        {
            // No-op.
        }
        
        /// <summary>
        /// Initializes a new instance of the <see cref="NearCacheConfiguration"/> class.
        /// </summary>
        internal PlatformNearCacheConfiguration(IBinaryRawReader reader)
        {
            KeyTypeName = reader.ReadString();
            ValueTypeName = reader.ReadString();
            KeepBinary = reader.ReadBoolean();
        }

        /// <summary>
        /// Gets or sets fully-qualified platform type name of the cache key used for the local map.
        /// When not set, object-based map is used, which can reduce performance and increase allocations due to boxing.
        /// </summary>
        public string KeyTypeName { get; set; }
        
        /// <summary>
        /// Gets or sets fully-qualified platform type name of the cache value used for the local map.
        /// When not set, object-based map is used, which can reduce performance and increase allocations due to boxing.
        /// </summary>
        public string ValueTypeName { get; set; }
        
        /// <summary>
        /// Gets or sets a value indicating whether platform near cache should store keys and values in binary form.
        /// </summary>
        public bool KeepBinary { get; set; }

        /// <summary>
        /// Writes to the specified writer.
        /// </summary>
        internal void Write(IBinaryRawWriter writer)
        {
            writer.WriteString(KeyTypeName);
            writer.WriteString(ValueTypeName);
            writer.WriteBoolean(KeepBinary);
        }
    }
}