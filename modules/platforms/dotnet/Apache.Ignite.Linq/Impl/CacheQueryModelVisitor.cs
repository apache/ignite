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
    using System.Linq.Expressions;
    using Remotion.Linq;
    using Remotion.Linq.Clauses;
    using Remotion.Linq.Clauses.ResultOperators;

    /// <summary>
    /// Query visitor, transforms LINQ expression to SQL.
    /// </summary>
    internal class CacheQueryModelVisitor : QueryModelVisitorBase
    {
        public static QueryData GenerateQuery(QueryModel queryModel)
        {
            var visitor = new CacheQueryModelVisitor();

            visitor.Query = "";
            visitor.VisitQueryModel(queryModel);
            
            return new QueryData(visitor.Query);
        }

        public string Query { get; set; }

        public override void VisitMainFromClause(MainFromClause fromClause, QueryModel queryModel)
        {
            base.VisitMainFromClause(fromClause, queryModel);

            Query += " From " + fromClause.ItemName;
        }

        public override void VisitWhereClause(WhereClause whereClause, QueryModel queryModel, int index)
        {
            base.VisitWhereClause(whereClause, queryModel, index);

            Query += " Where " + GetSqlExpression(whereClause.Predicate);
        }

        public override void VisitSelectClause(SelectClause selectClause, QueryModel queryModel)
        {
            base.VisitSelectClause(selectClause, queryModel);

            Query += " Select " + GetSqlExpression(selectClause.Selector);
        }

        public override void VisitResultOperator(ResultOperatorBase resultOperator, QueryModel queryModel, int index)
        {
            base.VisitResultOperator(resultOperator, queryModel, index);

            if (resultOperator is CountResultOperator)
                Query += " Select " + "cast(count({0}) as int";
            else
                throw new NotSupportedException("TODO");  // Min, max, etc
        }

        private static string GetSqlExpression(Expression expression)
        {
            return CacheQueryExpressionVisitor.GetSqlStatement(expression);
        }
    }
}
