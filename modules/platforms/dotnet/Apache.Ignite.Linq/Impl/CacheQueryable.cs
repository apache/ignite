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

namespace Apache.Ignite.Linq.Impl
{
    using System.Linq;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Impl.Cache;

    /// <summary>
    /// <see cref="IQueryable{T}"/> implementation for <see cref="ICache{TK,TV}"/>.
    /// </summary>
    internal class CacheQueryable<TKey, TValue> : CacheQueryableBase<ICacheEntry<TKey, TValue>>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CacheQueryable{TKey, TValue}" /> class.
        /// </summary>
        /// <param name="cache">The cache.</param>
        /// <param name="queryOptions">The query options.</param>
        /// <param name="ignite">The ignite.</param>
        public CacheQueryable(ICacheInternal cache, QueryOptions queryOptions, IIgnite ignite = null)
            : base(new CacheFieldsQueryProvider(CacheQueryParser.Instance,
                new CacheFieldsQueryExecutor(cache, queryOptions), 
                ignite, cache.GetConfiguration(), queryOptions.TableName, typeof(TValue)))
        {
            // No-op.
        }
    }
}
