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
    using System.Linq.Expressions;
    using Apache.Ignite.Core.Cache;
    using Remotion.Linq;
    using Remotion.Linq.Clauses.StreamedData;

    /// <summary>
    /// Cache query provider.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    internal class CacheQueryProvider<TKey, TValue> : QueryProviderBase
    {
        /** */
        private readonly ICache<TKey, TValue> _cache;

        /** */
        private CacheFieldsQueryProvider _fieldsProvider;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheQueryProvider{TKey, TValue}" /> class.
        /// </summary>
        /// <param name="cache">The cache.</param>
        public CacheQueryProvider(ICache<TKey, TValue> cache) 
            : base(Remotion.Linq.Parsing.Structure.QueryParser.CreateDefault(), 
                  new CacheQueryExecutor<TKey, TValue>(cache))
        {
            _cache = cache;
        }

        /// <summary>
        /// Gets the cache.
        /// </summary>
        public ICache<TKey, TValue> Cache
        {
            get { return _cache; }
        }

        /** <inheritdoc /> */
        public override IQueryable<T> CreateQuery<T>(Expression expression)
        {
            if (typeof (T) != typeof (ICacheEntry<TKey, TValue>))
                return GetFieldsProvider().CreateQuery<T>(expression);

            return (IQueryable<T>) new CacheQueryable<TKey, TValue>(this, expression);
        }

        /** <inheritdoc /> */
        public override IStreamedData Execute(Expression expression)
        {
            if (IsFieldsQuery(expression))
                return GetFieldsProvider().Execute(expression);

            return base.Execute(expression);
        }


        /// <summary>
        /// Gets the query data.
        /// </summary>
        /// <param name="expression">The expression.</param>
        /// <returns>Query data.</returns>
        public QueryData GetQueryData(Expression expression)
        {
            var provider = IsFieldsQuery(expression) ? (QueryProviderBase) GetFieldsProvider() : this;

            var model = provider.GenerateQueryModel(expression);

            return ((ICacheQueryExecutor) provider.Executor).GetQueryData(model);
        }

        /// <summary>
        /// Determines whether specified expression represents fields query.
        /// </summary>
        /// <param name="expression">The expression.</param>
        /// <returns></returns>
        private static bool IsFieldsQuery(Expression expression)
        {
            return expression.NodeType != ExpressionType.Constant &&
                   expression.Type != typeof (IQueryable<ICacheEntry<TKey, TValue>>);
        }

        /// <summary>
        /// Gets the fields provider.
        /// </summary>
        private CacheFieldsQueryProvider GetFieldsProvider()
        {
            return _fieldsProvider ?? (_fieldsProvider =
                new CacheFieldsQueryProvider(QueryParser,
                    new CacheFieldsQueryExecutor(q => Cache.QueryFields(q)), _cache.Ignite, _cache.Name));
        }
    }
}