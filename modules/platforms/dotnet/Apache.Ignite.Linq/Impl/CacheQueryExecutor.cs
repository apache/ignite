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
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Impl.Common;
    using Remotion.Linq;

    /// <summary>
    /// Cache query executor.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    internal class CacheQueryExecutor<TKey, TValue> : IQueryExecutor
    {
        private readonly ICache<TKey, TValue> _cache;

        public CacheQueryExecutor(ICache<TKey, TValue> cache)
        {
            IgniteArgumentCheck.NotNull(cache, "cache");

            _cache = cache;
        }

        public T ExecuteScalar<T>(QueryModel queryModel)
        {
            throw new System.NotImplementedException();
        }

        public T ExecuteSingle<T>(QueryModel queryModel, bool returnDefaultWhenEmpty)
        {
            throw new System.NotImplementedException();
        }

        public IEnumerable<T> ExecuteCollection<T>(QueryModel queryModel)
        {
            var queryData = new QueryData {QueryText = "test"}; // TODO: Generate

            //var query = new SqlFieldsQuery(queryData.QueryText, queryData.Parameters);

            var query = new SqlQuery(typeof(T), queryData.QueryText, queryData.Parameters);

            // TODO: This will fail, need to map fields to T, which is anonymous class
            return (IEnumerable<T>) _cache.Query(query);
        }
    }
}