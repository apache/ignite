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
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Linq.Expressions;
    using Apache.Ignite.Core.Cache.Query;
    using Remotion.Linq;

    /// <summary>
    /// Fields query executor.
    /// </summary>
    internal class CacheFieldsQueryExecutor : IQueryExecutor
    {
        /** */
        private readonly Func<SqlFieldsQuery, IQueryCursor<IList>> _executorFunc;

        /** */
        private readonly string _tableName;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheFieldsQueryExecutor" /> class.
        /// </summary>
        /// <param name="executorFunc">The executor function.</param>
        /// <param name="tableName">Name of the table.</param>
        public CacheFieldsQueryExecutor(Func<SqlFieldsQuery, IQueryCursor<IList>> executorFunc, string tableName)
        {
            Debug.Assert(executorFunc != null);

            _executorFunc = executorFunc;
            _tableName = tableName;
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
            var queryData = CacheFieldsQueryModelVisitor.GenerateQuery(queryModel, _tableName);

            var query = new SqlFieldsQuery(queryData.QueryText, queryData.Parameters.ToArray());

            var queryCursor = _executorFunc(query);

            var newExpr = queryModel.SelectClause.Selector as NewExpression;

            if (newExpr != null)
            {
                // TODO: Compile Func<IList, T>
                var ctor = newExpr.Constructor;

                return queryCursor.Select(fields => (T) ctor.Invoke(fields.Cast<object>().ToArray()));
            }

            return queryCursor.Select(ConvertSingleField<T>);
        }

        /// <summary>
        /// Converts the single field from the list to specified type.
        /// </summary>
        private static T ConvertSingleField<T>(IList fields)
        {
            if (fields.Count != 1)
                throw new InvalidOperationException("Single-field query returned unexpected number of values: " +
                                                    fields.Count);

            var f = fields[0];

            if (f is T)
                return (T) f;

            return (T) Convert.ChangeType(fields[0], typeof (T));
        }
    }
}