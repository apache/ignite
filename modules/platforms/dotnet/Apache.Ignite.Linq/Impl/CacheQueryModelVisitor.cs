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
    using Remotion.Linq.Clauses.Expressions;
    using Remotion.Linq.Clauses.ResultOperators;

    /// <summary>
    /// Query visitor, transforms LINQ expression to SQL.
    /// </summary>
    internal sealed class CacheQueryModelVisitor : QueryModelVisitorBase
    {
        /** */
        private readonly StringBuilder _builder = new StringBuilder();

        /** */
        private readonly List<object> _parameters = new List<object>();

        /** */
        private int _aliasIndex = 0;

        /// <summary>
        /// Generates the query.
        /// </summary>
        public QueryData GenerateQuery(QueryModel queryModel)
        {
            Debug.Assert(_builder.Length == 0);
            Debug.Assert(_parameters.Count == 0);

            VisitQueryModel(queryModel);

            if (char.IsWhiteSpace(_builder[_builder.Length - 1]))
                _builder.Remove(_builder.Length - 1, 1);  // TrimEnd

            var queryText = _builder.ToString();

            return new QueryData(queryText, _parameters, true);
        }

        /** <inheritdoc /> */
        public override void VisitQueryModel(QueryModel queryModel)
        {
            // SELECT TOP 1
            _builder.Append("select ");

            var parenCount = ProcessResultOperatorsBegin(queryModel);

            // FIELD1, FIELD2 FROM TABLE1, TABLE2
            BuildSqlExpression(queryModel.SelectClause.Selector, parenCount > 0);
            _builder.Append(')', parenCount).Append(" ");

            // WHERE ... JOIN ...
            base.VisitQueryModel(queryModel);

            // UNION ...
            ProcessResultOperatorsEnd(queryModel);
        }

        /// <summary>
        /// Processes the result operators.
        /// </summary>
        private int ProcessResultOperatorsBegin(QueryModel queryModel)
        {
            int parenCount = 0;

            foreach (var op in queryModel.ResultOperators.Reverse())
            {
                if (op is CountResultOperator)
                {
                    _builder.Append("count (");
                    parenCount++;
                }
                else if (op is SumResultOperator)
                {
                    _builder.Append("sum (");
                    parenCount++;
                }
                else if (op is MinResultOperator)
                {
                    _builder.Append("min (");
                    parenCount++;
                }
                else if (op is MaxResultOperator)
                {
                    _builder.Append("max (");
                    parenCount++;
                }
                else if (op is DistinctResultOperator)
                    _builder.Append("distinct ");
                else if (op is FirstResultOperator || op is SingleResultOperator)
                    _builder.Append("top 1 ");
                else if (op is TakeResultOperator)
                    _builder.AppendFormat("top {0} ", ((TakeResultOperator) op).Count);
                else if (op is UnionResultOperator || op is IntersectResultOperator || op is ExceptResultOperator
                         || op is DefaultIfEmptyResultOperator)
                    // Will be processed later
                    break;
                else
                    throw new NotSupportedException("Operator is not supported: " + op);
            }
            return parenCount;
        }

        /// <summary>
        /// Processes the result operators that go in the beginning of the query.
        /// </summary>
        private void ProcessResultOperatorsEnd(QueryModel queryModel)
        {
            foreach (var op in queryModel.ResultOperators.Reverse())
            {
                // SELECT TOP 10 * FROM "person_cache".PERSON where _KEY>10 UNION (select * from "".PERSON where _key > 50 limit 20)
                string keyword = null;
                Expression source = null;

                var union = op as UnionResultOperator;
                if (union != null)
                {
                    keyword = "union";
                    source = union.Source2;
                }

                var intersect = op as IntersectResultOperator;
                if (intersect != null)
                {
                    keyword = "intersect";
                    source = intersect.Source2;
                }

                var except = op as ExceptResultOperator;
                if (except != null)
                {
                    keyword = "except";
                    source = except.Source2;
                }

                if (keyword != null)
                {
                    _builder.Append(keyword).Append(" (");

                    var subQuery = source as SubQueryExpression;

                    if (subQuery != null)  // Subquery union
                        VisitQueryModel(subQuery.QueryModel);
                    else
                    {
                        // Direct cache union, source is ICacheQueryable
                        var innerExpr = source as ConstantExpression;

                        if (innerExpr == null)
                            throw new NotSupportedException("Unexpected UNION inner sequence: " + source);

                        var queryable = innerExpr.Value as ICacheQueryableInternal;

                        if (queryable == null)
                            throw new NotSupportedException("Unexpected UNION inner sequence " +
                                                            "(only results of cache.ToQueryable() are supported): " +
                                                            innerExpr.Value);

                        VisitQueryModel(queryable.GetQueryModel());
                    }

                    _builder.Append(")");
                }
            }
        }

        /** <inheritdoc /> */
        public override void VisitMainFromClause(MainFromClause fromClause, QueryModel queryModel)
        {
            base.VisitMainFromClause(fromClause, queryModel);

            _builder.AppendFormat("from {0} ", TableNameMapper.GetTableNameWithSchema(fromClause));

            foreach (var additionalFrom in queryModel.BodyClauses.OfType<AdditionalFromClause>())
                _builder.AppendFormat(", {0} ", TableNameMapper.GetTableNameWithSchema(additionalFrom));
        }

        /** <inheritdoc /> */
        public override void VisitWhereClause(WhereClause whereClause, QueryModel queryModel, int index)
        {
            base.VisitWhereClause(whereClause, queryModel, index);

            _builder.Append(index > 0 ? "and " : "where ");

            BuildSqlExpression(whereClause.Predicate);

            _builder.Append(" ");
        }

        /** <inheritdoc /> */
        public override void VisitJoinClause(JoinClause joinClause, QueryModel queryModel, int index)
        {
            base.VisitJoinClause(joinClause, queryModel, index);

            var subQuery = joinClause.InnerSequence as SubQueryExpression;

            string tableNameOverride = null;

            if (subQuery != null)
            {
                var isOuter = subQuery.QueryModel.ResultOperators.OfType<DefaultIfEmptyResultOperator>().Any();

                _builder.AppendFormat("{0} join (", isOuter ? "left outer" : "inner");

                VisitQueryModel(subQuery.QueryModel);

                tableNameOverride = GetNextAlias();

                _builder.AppendFormat(") as {0} on (", tableNameOverride);
            }
            else
            {
                var innerExpr = joinClause.InnerSequence as ConstantExpression;

                if (innerExpr == null)
                    throw new NotSupportedException("Unexpected JOIN inner sequence (subqueries are not supported): " +
                                                    joinClause.InnerSequence);

                if (!(innerExpr.Value is ICacheQueryable))
                    throw new NotSupportedException("Unexpected JOIN inner sequence " +
                                                    "(only results of cache.ToQueryable() are supported): " +
                                                    innerExpr.Value);

                _builder.AppendFormat("inner join {0} on (", TableNameMapper.GetTableNameWithSchema(joinClause));
            }

            BuildJoinCondition(joinClause.InnerKeySelector, joinClause.OuterKeySelector, tableNameOverride);

            _builder.Append(") ");
        }

        /// <summary>
        /// Builds the join condition ('x=y AND foo=bar').
        /// </summary>
        /// <param name="innerKey">The inner key selector.</param>
        /// <param name="outerKey">The outer key selector.</param>
        /// <param name="innerTableAlias">The inner table alias.</param>
        /// <exception cref="System.NotSupportedException">
        /// </exception>
        private void BuildJoinCondition(Expression innerKey, Expression outerKey, string innerTableAlias)
        {
            var innerNew = innerKey as NewExpression;
            var outerNew = outerKey as NewExpression;

            if (innerNew == null && outerNew == null)
            {
                BuildJoinSubCondition(innerKey, outerKey, innerTableAlias);
                return;
            }

            if (innerNew != null && outerNew != null)
            {
                if (innerNew.Constructor != outerNew.Constructor)
                    throw new NotSupportedException(
                        string.Format("Unexpected JOIN condition. Multi-key joins should have " +
                                      "the same initializers on both sides: '{0} = {1}'", innerKey, outerKey));

                for (var i = 0; i < innerNew.Arguments.Count; i++)
                {
                    if (i > 0)
                        _builder.Append(" and ");

                    BuildJoinSubCondition(innerNew.Arguments[i], outerNew.Arguments[i], innerTableAlias);
                }

                return;
            }

            throw new NotSupportedException(
                string.Format("Unexpected JOIN condition. Multi-key joins should have " +
                              "anonymous type instances on both sides: '{0} = {1}'", innerKey, outerKey));
        }

        /// <summary>
        /// Builds the join sub condition.
        /// </summary>
        /// <param name="innerKey">The inner key.</param>
        /// <param name="outerKey">The outer key.</param>
        /// <param name="innerTableAlias">The inner table alias.</param>
        private void BuildJoinSubCondition(Expression innerKey, Expression outerKey, string innerTableAlias)
        {
            BuildSqlExpression(innerKey, tableNameOverride: innerTableAlias);
            _builder.Append(" = ");
            BuildSqlExpression(outerKey);
        }

        /// <summary>
        /// Builds the SQL expression.
        /// </summary>
        private void BuildSqlExpression(Expression expression, bool aggregating = false, 
            string tableNameOverride = null)
        {
            new CacheQueryExpressionVisitor(_builder, _parameters, aggregating, tableNameOverride).Visit(expression);
        }

        /// <summary>
        /// Gets the next alias.
        /// </summary>
        private string GetNextAlias()
        {
            return "tbl" + _aliasIndex++;
        }
    }
}
