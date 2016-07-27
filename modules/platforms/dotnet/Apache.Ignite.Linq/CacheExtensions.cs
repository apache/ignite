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

namespace Apache.Ignite.Linq
{
    using System.Linq;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Linq.Impl;

    /// <summary>
    /// Extensions methods for <see cref="ICache{TK,TV}"/>.
    /// </summary>
    public static class CacheLinqExtensions
    {
        /// <summary>
        /// Gets an <see cref="IQueryable{T}"/> instance over this cache.
        /// <para />
        /// Resulting query will be translated to cache SQL query and executed over the cache instance 
        /// via either <see cref="ICache{TK,TV}.Query"/> or <see cref="ICache{TK,TV}.QueryFields"/>,
        /// depending on requested result. 
        /// <para />
        /// Result of this method (and subsequent query) can be cast to <see cref="ICacheQueryable"/> for introspection.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="cache">The cache.</param>
        /// <returns><see cref="IQueryable{T}"/> instance over this cache.</returns>
        public static IQueryable<ICacheEntry<TKey, TValue>> AsCacheQueryable<TKey, TValue>(
            this ICache<TKey, TValue> cache)
        {
            return cache.AsCacheQueryable(false, null);
        }

        /// <summary>
        /// Gets an <see cref="IQueryable{T}"/> instance over this cache.
        /// <para />
        /// Resulting query will be translated to cache SQL query and executed over the cache instance 
        /// via either <see cref="ICache{TK,TV}.Query"/> or <see cref="ICache{TK,TV}.QueryFields"/>,
        /// depending on requested result. 
        /// <para />
        /// Result of this method (and subsequent query) can be cast to <see cref="ICacheQueryable"/> for introspection.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="cache">The cache.</param>
        /// <param name="local">Local flag. When set query will be executed only on local node, so only local 
        /// entries will be returned as query result.</param>
        /// <returns><see cref="IQueryable{T}"/> instance over this cache.</returns>
        public static IQueryable<ICacheEntry<TKey, TValue>> AsCacheQueryable<TKey, TValue>(
            this ICache<TKey, TValue> cache, bool local)
        {
            return cache.AsCacheQueryable(local, null);
        }

        /// <summary>
        /// Gets an <see cref="IQueryable{T}" /> instance over this cache.
        /// <para />
        /// Resulting query will be translated to cache SQL query and executed over the cache instance
        /// via either <see cref="ICache{TK,TV}.Query" /> or <see cref="ICache{TK,TV}.QueryFields" />,
        /// depending on requested result.
        /// <para />
        /// Result of this method (and subsequent query) can be cast to <see cref="ICacheQueryable" /> for introspection.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="cache">The cache.</param>
        /// <param name="local">Local flag. When set query will be executed only on local node, so only local 
        /// entries will be returned as query result.</param>
        /// <param name="tableName">
        /// Name of the table.
        /// <para />
        /// Table name is equal to short class name of a cache value.
        /// When a cache has only one type of values, or only one <see cref="QueryEntity"/> defined, 
        /// table name will be inferred and can be omitted.
        /// </param>
        /// <returns><see cref="IQueryable{T}" /> instance over this cache.</returns>
        public static IQueryable<ICacheEntry<TKey, TValue>> AsCacheQueryable<TKey, TValue>(
            this ICache<TKey, TValue> cache, bool local, string tableName)
        {
            return cache.AsCacheQueryable(new QueryOptions {Local = local, TableName = tableName});
        }

        /// <summary>
        /// Gets an <see cref="IQueryable{T}" /> instance over this cache.
        /// <para />
        /// Resulting query will be translated to cache SQL query and executed over the cache instance
        /// via either <see cref="ICache{TK,TV}.Query" /> or <see cref="ICache{TK,TV}.QueryFields" />,
        /// depending on requested result.
        /// <para />
        /// Result of this method (and subsequent query) can be cast to <see cref="ICacheQueryable" /> for introspection.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="cache">The cache.</param>
        /// <param name="queryOptions">The query options.</param>
        /// <returns>
        ///   <see cref="IQueryable{T}" /> instance over this cache.
        /// </returns>
        public static IQueryable<ICacheEntry<TKey, TValue>> AsCacheQueryable<TKey, TValue>(
            this ICache<TKey, TValue> cache, QueryOptions queryOptions)
        {
            return new CacheQueryable<TKey, TValue>(cache, queryOptions);
        }
    }
}