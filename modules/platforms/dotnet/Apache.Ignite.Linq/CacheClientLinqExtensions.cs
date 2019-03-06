/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Linq
{
    using System.Linq;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Linq.Impl;

    /// <summary>
    /// Extensions methods for <see cref="ICacheClient{TK,TV}"/>.
    /// </summary>
    public static class CacheClientLinqExtensions
    {
        /// <summary>
        /// Gets an <see cref="IQueryable{T}"/> instance over this cache.
        /// <para />
        /// Resulting query will be translated to cache SQL query and executed over the cache instance 
        /// via either <see cref="ICacheClient{TK,TV}.Query(SqlFieldsQuery)"/>.
        /// <para />
        /// Result of this method (and subsequent query) can be cast to <see cref="ICacheQueryable"/>
        /// for introspection, or converted with <see cref="CacheLinqExtensions.ToCacheQueryable{T}"/>
        /// extension method.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="cache">The cache.</param>
        /// <returns><see cref="IQueryable{T}"/> instance over this cache.</returns>
        public static IQueryable<ICacheEntry<TKey, TValue>> AsCacheQueryable<TKey, TValue>(
            this ICacheClient<TKey, TValue> cache)
        {
            IgniteArgumentCheck.NotNull(cache, "cache");

            return AsCacheQueryable(cache, false, null);
        }

        /// <summary>
        /// Gets an <see cref="IQueryable{T}"/> instance over this cache.
        /// <para />
        /// Resulting query will be translated to cache SQL query and executed over the cache instance 
        /// via either <see cref="ICacheClient{TK,TV}.Query(SqlFieldsQuery)"/>.
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
            this ICacheClient<TKey, TValue> cache, bool local)
        {
            IgniteArgumentCheck.NotNull(cache, "cache");

            return AsCacheQueryable(cache, local, null);
        }

        /// <summary>
        /// Gets an <see cref="IQueryable{T}" /> instance over this cache.
        /// <para />
        /// Resulting query will be translated to cache SQL query and executed over the cache instance 
        /// via either <see cref="ICacheClient{TK,TV}.Query(SqlFieldsQuery)"/>.
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
            this ICacheClient<TKey, TValue> cache, bool local, string tableName)
        {
            IgniteArgumentCheck.NotNull(cache, "cache");

            return AsCacheQueryable(cache, new QueryOptions {Local = local, TableName = tableName});
        }

        /// <summary>
        /// Gets an <see cref="IQueryable{T}" /> instance over this cache.
        /// <para />
        /// Resulting query will be translated to cache SQL query and executed over the cache instance 
        /// via either <see cref="ICacheClient{TK,TV}.Query(SqlFieldsQuery)"/>.
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
            this ICacheClient<TKey, TValue> cache, QueryOptions queryOptions)
        {
            IgniteArgumentCheck.NotNull(cache, "cache");
            IgniteArgumentCheck.NotNull(queryOptions, "queryOptions");

            return new CacheQueryable<TKey, TValue>((ICacheInternal) cache, queryOptions);
        }
    }
}