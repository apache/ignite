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
        private readonly string _schemaName;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheQueryModelVisitor"/> class.
        /// </summary>
        /// <param name="schemaName">Name of the schema.</param>
        protected CacheQueryModelVisitor(string schemaName)
        {
            _schemaName = schemaName;
        }

        /// <summary>
        /// Generates the query.
        /// </summary>
        public static QueryData GenerateQuery(QueryModel queryModel, string schemaName)
        {
            Debug.Assert(queryModel != null);

            var visitor = new CacheQueryModelVisitor(schemaName);

            visitor.VisitQueryModel(queryModel);

            return visitor.GetQuery();
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
        private StringBuilder Builder
        {
            get { return _builder; }
        }

        /** <inheritdoc /> */
        public override void VisitMainFromClause(MainFromClause fromClause, QueryModel queryModel)
        {
            base.VisitMainFromClause(fromClause, queryModel);

            Builder.AppendFormat("from \"{0}\".{1} ", _schemaName, TableNameMapper.GetTableName(fromClause));
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

        /** <inheritdoc /> */
        public override void VisitJoinClause(JoinClause joinClause, QueryModel queryModel, int index)
        {
            base.VisitJoinClause(joinClause, queryModel, index);

            var innerExpr = joinClause.InnerSequence as ConstantExpression;

            if (innerExpr == null)
                throw new NotSupportedException("Unexpected JOIN inner sequence (squbqueries are not supported): " +
                                                joinClause.InnerSequence);

            if (!(innerExpr.Value is ICacheQueryable))
                throw new NotSupportedException("Unexpected JOIN inner sequence " +
                                                "(only results of cache.ToQueryable() are supported): " +
                                                innerExpr.Value);

            Builder.AppendFormat("join {0} on ({1} = {2}) ", TableNameMapper.GetTableName(joinClause),
                GetSqlExpression(joinClause.InnerKeySelector).QueryText,
                GetSqlExpression(joinClause.OuterKeySelector).QueryText);
        }

        /// <summary>
        /// Gets the SQL expression.
        /// </summary>
        protected QueryData GetSqlExpression(Expression expression, bool aggregating = false)
        {
            return CacheQueryExpressionVisitor.GetSqlExpression(expression, aggregating, _schemaName);
        }
    }
}
