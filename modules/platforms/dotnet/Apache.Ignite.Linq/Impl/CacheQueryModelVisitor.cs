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

        /// <summary>
        /// Generates the query.
        /// </summary>
        public static QueryData GenerateQuery(QueryModel queryModel)
        {
            var visitor = new CacheQueryModelVisitor();

            visitor.VisitQueryModel(queryModel);

            return visitor.GetQuery();
        }

        /// <summary>
        /// Gets the query.
        /// </summary>
        /// <returns>Query data.</returns>
        private QueryData GetQuery()
        {
            return new QueryData(_builder.ToString().TrimEnd(), _parameters);
        }

        /** <inheritdoc /> */
        public override void VisitMainFromClause(MainFromClause fromClause, QueryModel queryModel)
        {
            base.VisitMainFromClause(fromClause, queryModel);

            var cacheEntryType = fromClause.ItemType;

            Debug.Assert(cacheEntryType.IsGenericType);
            Debug.Assert(cacheEntryType.GetGenericTypeDefinition() == typeof(ICacheEntry<,>));

            var tableName = cacheEntryType.GetGenericArguments()[1].Name;

            _builder.AppendFormat("from {0} ", tableName);
        }

        /** <inheritdoc /> */
        public override void VisitWhereClause(WhereClause whereClause, QueryModel queryModel, int index)
        {
            base.VisitWhereClause(whereClause, queryModel, index);

            var whereSql = GetSqlExpression(whereClause.Predicate);

            _builder.Append(_parameters.Any() ? "and" : "where");
            _builder.AppendFormat(" {0} ", whereSql.QueryText);

            _parameters.AddRange(whereSql.Parameters);
        }

        /** <inheritdoc /> */
        public override void VisitSelectClause(SelectClause selectClause, QueryModel queryModel)
        {
            base.VisitSelectClause(selectClause, queryModel);

            //_select = selectClause;

        }

        /** <inheritdoc /> */
        public override void VisitResultOperator(ResultOperatorBase resultOperator, QueryModel queryModel, int index)
        {
            //base.VisitResultOperator(resultOperator, queryModel, index);

            //if (resultOperator is CountResultOperator)
            //    Query += " Select " + "cast(count({0}) as int";
            //else
                throw new NotSupportedException("TODO");  // Min, max, etc
        }

        /// <summary>
        /// Gets the SQL expression.
        /// </summary>
        private static QueryData GetSqlExpression(Expression expression)
        {
            return CacheQueryExpressionVisitor.GetSqlExpression(expression);
        }
    }
}
