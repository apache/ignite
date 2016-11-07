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

namespace Apache.Ignite.Linq.Impl
{
    using System.Linq;
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
        public CacheQueryable(ICache<TKey, TValue> cache, QueryOptions queryOptions)
            : base(new CacheFieldsQueryProvider(CacheQueryParser.Instance,
                new CacheFieldsQueryExecutor((ICacheInternal) cache, queryOptions.Local, queryOptions.PageSize,
                    queryOptions.EnableDistributedJoins, queryOptions.EnforceJoinOrder), 
                cache.Ignite, cache.GetConfiguration(), queryOptions.TableName, typeof(TValue)))
        {
            // No-op.
        }
    }
}
