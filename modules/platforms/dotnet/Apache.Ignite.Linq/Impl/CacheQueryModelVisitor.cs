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
    using System.Diagnostics;
    using System.Linq.Expressions;
    using Apache.Ignite.Core.Cache;
    using Remotion.Linq;
    using Remotion.Linq.Clauses;
    using Remotion.Linq.Clauses.ResultOperators;



    /// <summary>
    /// Query visitor, transforms LINQ expression to SQL.
    /// </summary>
    internal class CacheQueryModelVisitor<TKey, TValue> : QueryModelVisitorBase
    {
        private readonly ICache<TKey, TValue> _cache;
        private WhereClause _where;
        private SelectClause _select;

        public CacheQueryModelVisitor(ICache<TKey, TValue> cache)
        {
            Debug.Assert(cache != null);

            _cache = cache;
        }

        public QueryData GetQuery()
        {
            var sql = _where == null ? null : GetSqlExpression(_where.Predicate);

            if (_select.Selector.Type == typeof (ICacheEntry<TKey, TValue>))
            {
                // Full entry query
                return new QueryData(sql);
            }

            return new QueryData(sql) {IsFieldsQuery = true};
        }

        public override void VisitWhereClause(WhereClause whereClause, QueryModel queryModel, int index)
        {
            base.VisitWhereClause(whereClause, queryModel, index);

            _where = whereClause;
            //Query += " Where " + GetSqlExpression(whereClause.Predicate);
        }

        public override void VisitSelectClause(SelectClause selectClause, QueryModel queryModel)
        {
            base.VisitSelectClause(selectClause, queryModel);

            _select = selectClause;

            //Query += " Select " + GetSqlExpression(selectClause.Selector);
        }

        public override void VisitResultOperator(ResultOperatorBase resultOperator, QueryModel queryModel, int index)
        {
            //base.VisitResultOperator(resultOperator, queryModel, index);

            //if (resultOperator is CountResultOperator)
            //    Query += " Select " + "cast(count({0}) as int";
            //else
                throw new NotSupportedException("TODO");  // Min, max, etc
        }

        private string GetSqlExpression(Expression expression)
        {
            return CacheQueryExpressionVisitor.GetSqlStatement(_cache, expression);
        }

        private string GetCacheName(MainFromClause fromClause)
        {
            return "CacheNameTODO";
        }
    }

    internal class CacheQueryModelVisitor
    {
        public static QueryData GenerateQuery<TKey, TValue>(QueryModel queryModel, ICache<TKey, TValue> cache)
        {
            var visitor = new CacheQueryModelVisitor<TKey, TValue>(cache);

            visitor.VisitQueryModel(queryModel);

            return visitor.GetQuery();
        }
    }
}
