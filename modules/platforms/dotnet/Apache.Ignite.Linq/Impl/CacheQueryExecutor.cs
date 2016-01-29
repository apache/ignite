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
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Query;
    using Remotion.Linq;

    /// <summary>
    /// Cache query executor.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    internal class CacheQueryExecutor<TKey, TValue> : IQueryExecutor
    {
        /** */
        private readonly ICache<TKey, TValue> _cache;

        /** */
        private readonly string _queryTypeName;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheQueryExecutor{TKey, TValue}" /> class.
        /// </summary>
        /// <param name="cache">The cache.</param>
        /// <param name="queryTypeName">Name of the query type.</param>
        public CacheQueryExecutor(ICache<TKey, TValue> cache, string queryTypeName)
        {
            Debug.Assert(cache != null);
            Debug.Assert(!string.IsNullOrEmpty(queryTypeName));

            _cache = cache;
            _queryTypeName = queryTypeName;
        }

        /** <inheritdoc /> */
        public T ExecuteScalar<T>(QueryModel queryModel)
        {
            return ExecuteSingle<T>(queryModel, false);
        }

        /** <inheritdoc /> */
        public T ExecuteSingle<T>(QueryModel queryModel, bool returnDefaultWhenEmpty)
        {
            var collection = ExecuteCollection<T>(queryModel);

            return returnDefaultWhenEmpty ? collection.SingleOrDefault() : collection.Single();
        }

        /** <inheritdoc /> */
        public IEnumerable<T> ExecuteCollection<T>(QueryModel queryModel)
        {
            var queryData = CacheQueryModelVisitor.GenerateQuery(queryModel, _queryTypeName);

            var query = new SqlQuery(_queryTypeName, queryData.QueryText, queryData.Parameters.ToArray());

            Debug.WriteLine("SQL Query: {0} | {1}", queryData.QueryText,
                string.Join(", ", queryData.Parameters.Select(x => x.ToString())));

            return (IEnumerable<T>) _cache.Query(query);
        }
    }
}