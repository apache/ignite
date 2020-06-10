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
            var arr = r.ReadArray<object>();

            if (arr == null)
            {
                // TODO: Better exception that explains what to do.
                throw new IgniteException("Query is expected to return cache entry, but returned null");
            }

            if (arr.Length != 2)
            {
                // TODO: Better exception that explains what to do.
                throw new IgniteException(
                    "Query is expected to return _key and _val, but returns " + arr.Length + "items");
            }

            return new CacheEntry<TK, TV>((TK) arr[0], (TV) arr[1]);
        }
    }
}
