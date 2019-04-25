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

namespace Apache.Ignite.Core.Impl.Cache.Query
{
    using Apache.Ignite.Core.Cache;

    /// <summary>
    /// Cursor for entry-based queries.
    /// </summary>
    internal class QueryCursor<TK, TV> : PlatformQueryQursorBase<ICacheEntry<TK, TV>>
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="keepBinary">Keep poratble flag.</param>
        public QueryCursor(IPlatformTargetInternal target, bool keepBinary)
            : base(target, keepBinary,
                r => new CacheEntry<TK, TV>(r.ReadObject<TK>(), r.ReadObject<TV>()))
        {
            // No-op.
        }
    }
}
