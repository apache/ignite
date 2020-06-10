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

namespace Apache.Ignite.Core.Impl.Cache.Query
{
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Cursor for fields query, but returns cache entries.
    /// </summary>
    internal class FieldsCacheEntryQueryCursor<TK, TV> : PlatformQueryQursorBase<ICacheEntry<TK, TV>>
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="keepBinary">Keep binary flag.</param>
        public FieldsCacheEntryQueryCursor(IPlatformTargetInternal target, bool keepBinary)
            : base(target, keepBinary,
                r => ReadEntry(r))
        {
            // No-op.
        }

        /// <summary>
        /// Reads the cache entry.
        /// </summary>
        private static CacheEntry<TK, TV> ReadEntry(IBinaryRawReader r)
        {
            // Reading and skipping row size in bytes.
            r.ReadInt();

            var cnt = r.ReadInt();

            if (cnt != 2)
            {
                throw new IgniteException(
                    "SqlFieldsQuery should return _key and _val fields ('select _key, _val from ...'), " +
                    string.Format("but returns {0} field(s)", cnt));
            }

            return new CacheEntry<TK, TV>(r.ReadObject<TK>(), r.ReadObject<TV>());
        }
    }
}
