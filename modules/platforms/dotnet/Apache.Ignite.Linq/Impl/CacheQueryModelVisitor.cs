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
    using System.Linq.Expressions;
    using System.Text;
    using Apache.Ignite.Core.Cache;
    using Remotion.Linq;
    using Remotion.Linq.Clauses;

    /// <summary>
    /// Query visitor, transforms LINQ expression to SQL.
    /// </summary>
    internal class CacheQueryModelVisitor
    {
        /// <summary>
        /// Generates the query.
        /// </summary>
        public static QueryData GenerateQuery<TKey, TValue>(QueryModel queryModel, ICache<TKey, TValue> cache)
        {
            var visitor = new CacheQueryModelVisitor<TKey, TValue>(cache);

            visitor.VisitQueryModel(queryModel);

            return visitor.GetQuery();
        }
    }

    /// <summary>
    /// Query visitor, transforms LINQ expression to SQL.
    /// </summary>
    internal class CacheQueryModelVisitor<TKey, TValue> : QueryModelVisitorBase
    {
        private readonly ICache<TKey, TValue> _cache;
        private readonly List<WhereClause> _where = new List<WhereClause>();
        private SelectClause _select;

        public CacheQueryModelVisitor(ICache<TKey, TValue> cache)
        {
            Debug.Assert(cache != null);

            _cache = cache;
        }

        public QueryData GetQuery()
        {
            var builder = new StringBuilder();
            var parameters = new List<object>();

            builder.AppendFormat("from {0} ", typeof (TValue).Name);

            foreach (var whereClause in _where)
            {
                var whereSql = GetSqlExpression(whereClause.Predicate);

                parameters.AddRange(whereSql.Parameters);

                builder.AppendFormat("where {0} ", whereSql.QueryText);
            }

            return new QueryData(builder.ToString().TrimEnd(), parameters);
        }

        /** <inheritdoc /> */
        public override void VisitWhereClause(WhereClause whereClause, QueryModel queryModel, int index)
        {
            base.VisitWhereClause(whereClause, queryModel, index);

            _where.Add(whereClause);
        }

        /** <inheritdoc /> */
        public override void VisitSelectClause(SelectClause selectClause, QueryModel queryModel)
        {
            base.VisitSelectClause(selectClause, queryModel);

            _select = selectClause;

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
        private QueryData GetSqlExpression(Expression expression)
        {
            return CacheQueryExpressionVisitor.GetSqlExpression(_cache, expression);
        }
    }
}
