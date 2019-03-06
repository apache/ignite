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

namespace Apache.Ignite.Linq.Impl
{
    using System;
    using System.Linq;
    using System.Linq.Expressions;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;
    using Remotion.Linq;

    /// <summary>
    /// Base class for cache queryables.
    /// </summary>
    internal abstract class CacheQueryableBase<T> : QueryableBase<T>, ICacheQueryableInternal
    {
        /** <inheritdoc /> */
        protected CacheQueryableBase(IQueryProvider provider) : base(provider)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        protected CacheQueryableBase(IQueryProvider provider, Expression expression) : base(provider, expression)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public CacheConfiguration CacheConfiguration
        {
            get { return CacheQueryProvider.CacheConfiguration; }
        }

        /** <inheritdoc /> */
        public string CacheName
        {
            get { return CacheConfiguration.Name; }
        }

        /** <inheritdoc /> */
        [Obsolete("Deprecated, null for thin client.")]
        public IIgnite Ignite
        {
            get { return CacheQueryProvider.Ignite; }
        }

        /** <inheritdoc /> */
        public SqlFieldsQuery GetFieldsQuery()
        {
            var data = GetQueryData();
            var executor = CacheQueryProvider.Executor;

            return executor.GetFieldsQuery(data.QueryText, data.Parameters.ToArray());
        }

        /** <inheritdoc /> */
        public QueryModel GetQueryModel()
        {
            return CacheQueryProvider.GenerateQueryModel(Expression);
        }

        /** <inheritdoc /> */
        public string TableName
        {
            get { return CacheQueryProvider.TableName; }
        }

        /** <inheritdoc /> */
        public Func<object[], IQueryCursor<TQ>> CompileQuery<TQ>(LambdaExpression queryExpression)
        {
            var executor = CacheQueryProvider.Executor;

            // Generate two models: from current expression, and from provided lambda.
            // Lambda expression provides a way to identify argument mapping.
            // Comparing two models allows to check whether whole query is within lambda.
            var model = GetQueryModel();
            var lambdaModel = CacheQueryProvider.GenerateQueryModel(queryExpression.Body);

            return executor.CompileQuery<TQ>(model, lambdaModel, queryExpression);
        }

        /** <inheritdoc /> */
        public Func<object[], IQueryCursor<TQ>> CompileQuery<TQ>()
        {
            var executor = CacheQueryProvider.Executor;

            return executor.CompileQuery<TQ>(GetQueryModel());
        }

        /// <summary>
        /// Gets the cache query provider.
        /// </summary>
        private CacheFieldsQueryProvider CacheQueryProvider
        {
            get { return (CacheFieldsQueryProvider)Provider; }
        }

        /// <summary>
        /// Gets the query data.
        /// </summary>
        /// <returns></returns>
        private QueryData GetQueryData()
        {
            var model = GetQueryModel();

            return CacheFieldsQueryExecutor.GetQueryData(model);
        }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="string" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return string.Format("CacheQueryable [CacheName={0}, TableName={1}, Query={2}]", 
                CacheName, TableName, GetFieldsQuery());
        }
    }
}