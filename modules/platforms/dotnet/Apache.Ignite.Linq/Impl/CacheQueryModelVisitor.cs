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
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Text;
    using Apache.Ignite.Core.Cache;
    using Remotion.Linq;
    using Remotion.Linq.Clauses;

    /// <summary>
    /// Query visitor, transforms LINQ expression to SQL.
    /// </summary>
    internal class CacheQueryModelVisitor : QueryModelVisitorBase
    {
        /** */
        private readonly StringBuilder _builder = new StringBuilder();

        /** */
        private readonly List<object> _parameters = new List<object>();

        /** */
        private string _tableName;

        /// <summary>
        /// Generates the query.
        /// </summary>
        public static QueryData GenerateQuery(QueryModel queryModel, string tableName)
        {
            var visitor = new CacheQueryModelVisitor(tableName);

            visitor.VisitQueryModel(queryModel);

            return visitor.GetQuery();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheQueryModelVisitor"/> class.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        private CacheQueryModelVisitor(string tableName)
        {
            Debug.Assert(!string.IsNullOrEmpty(tableName));

            _tableName = tableName;
        }

        /// <summary>
        /// Gets the query.
        /// </summary>
        /// <returns>Query data.</returns>
        protected QueryData GetQuery()
        {
            return new QueryData(Builder.ToString().TrimEnd(), _parameters);
        }

        /// <summary>
        /// Gets the builder.
        /// </summary>
        protected StringBuilder Builder
        {
            get { return _builder; }
        }

        /** <inheritdoc /> */
        public override void VisitMainFromClause(MainFromClause fromClause, QueryModel queryModel)
        {
            base.VisitMainFromClause(fromClause, queryModel);

            Builder.AppendFormat("from {0} ", _tableName);
        }

        /** <inheritdoc /> */
        public override void VisitWhereClause(WhereClause whereClause, QueryModel queryModel, int index)
        {
            base.VisitWhereClause(whereClause, queryModel, index);

            var whereSql = GetSqlExpression(whereClause.Predicate);

            Builder.Append(_parameters.Any() ? "and" : "where");
            Builder.AppendFormat(" {0} ", whereSql.QueryText);

            _parameters.AddRange(whereSql.Parameters);
        }

        /// <summary>
        /// Gets the SQL expression.
        /// </summary>
        protected static QueryData GetSqlExpression(Expression expression)
        {
            return CacheQueryExpressionVisitor.GetSqlExpression(expression);
        }
    }
}
