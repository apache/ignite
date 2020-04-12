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

namespace Apache.Ignite.Core.Cache.Configuration
{
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Query;

    /// <summary>
    /// Native .NET cache configuration.
    /// <para />
    /// Enables native .NET cache. Cache entries will be stored in deserialized form in CLR heap.
    /// <para />
    /// When enabled on server nodes, all primary keys will be stored in platform memory as well.
    /// <para />
    /// Same eviction policy applies to near cache entries for all keys on client nodes and
    /// non-primary keys on server nodes.
    /// <para />
    /// Enabling this can greatly improve performance for key-value operations and scan queries,
    /// at the expense of RAM usage.
    /// <para />
    /// Supported operations (async counterparts included):
    /// <list type="bullet">
    /// <item><term><see cref="ICache{TK,TV}.Get"/>, <see cref="ICache{TK,TV}.TryGet"/></term></item>
    /// <item><term><see cref="ICache{TK,TV}.GetAll"/></term></item>
    /// <item><term><see cref="ICache{TK,TV}.ContainsKey"/>, <see cref="ICache{TK,TV}.ContainsKeys"/></term></item>
    /// <item><term><see cref="ICache{TK,TV}.LocalPeek"/>, <see cref="ICache{TK,TV}.TryLocalPeek"/></term></item>
    /// <item><term><see cref="ICache{TK,TV}.GetLocalEntries"/></term></item>
    /// <item><term><see cref="ICache{TK,TV}.GetLocalSize"/></term></item>
    /// <item><term><see cref="ICache{TK,TV}.Query(QueryBase)"/> with <see cref="ScanQuery{TK,TV}"/></term></item>
    /// </list>
    /// </summary>
    public class PlatformCacheConfiguration
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PlatformCacheConfiguration"/> class.
        /// </summary>
        public PlatformCacheConfiguration()
        {
            // No-op.
        }
        
        /// <summary>
        /// Initializes a new instance of the <see cref="PlatformCacheConfiguration"/> class.
        /// </summary>
        internal PlatformCacheConfiguration(IBinaryRawReader reader)
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
        /// Gets or sets a value indicating whether platform cache should store keys and values in binary form.
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