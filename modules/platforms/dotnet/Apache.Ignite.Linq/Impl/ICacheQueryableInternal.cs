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
    using System;
    using System.Linq.Expressions;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;
    using Remotion.Linq;

    /// <summary>
    /// Internal queryable interface.
    /// </summary>
    internal interface ICacheQueryableInternal : ICacheQueryable
    {
        /// <summary>
        /// Gets the configuration of the cache that is associated with this query.
        /// </summary>
        /// <value>
        /// The configuration of the cache.
        /// </value>
        CacheConfiguration CacheConfiguration { get; }

        /// <summary>
        /// Gets the name of the table.
        /// </summary>
        string TableName { get; }

        /// <summary>
        /// Gets the query model.
        /// </summary>
        QueryModel GetQueryModel();

        /// <summary>
        /// Compiles the query.
        /// </summary>
        /// <param name="queryExpression">The query expression.</param>
        Func<object[], IQueryCursor<T>> CompileQuery<T>(LambdaExpression queryExpression);
        
        /// <summary>
        /// Compiles the query without regard to the order and number of arguments.
        /// </summary>
        Func<object[], IQueryCursor<T>> CompileQuery<T>();
    }
}