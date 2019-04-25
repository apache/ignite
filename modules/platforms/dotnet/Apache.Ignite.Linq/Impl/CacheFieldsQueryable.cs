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
    using System.Linq.Expressions;
    using Apache.Ignite.Core.Cache;

    /// <summary>
    /// Fields <see cref="IQueryable{T}"/> implementation for <see cref="ICache{TK,TV}"/>.
    /// </summary>
    internal class CacheFieldsQueryable<T> : CacheQueryableBase<T>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CacheQueryable{TKey, TValue}"/> class.
        /// </summary>
        /// <param name="provider">The provider used to execute the query represented by this queryable 
        /// and to construct new queries.</param>
        /// <param name="expression">The expression representing the query.</param>
        public CacheFieldsQueryable(IQueryProvider provider, Expression expression) : base(provider, expression)
        {
            // No-op.
        }
    }
}