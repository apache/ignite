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
    using System.Collections.ObjectModel;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
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
        private readonly List<Expression> _parameterExpressions = new List<Expression>();

        /** */
        private readonly AliasDictionary _aliases = new AliasDictionary();

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

            var qryText = _builder.ToString();

            return new QueryData(qryText, _parameters, _parameterExpressions);
        }

        /// <summary>
        /// Gets the builder.
        /// </summary>
        public StringBuilder Builder
        {
            get { return _builder; }
        }

        /// <summary>
        /// Gets the parameters.
        /// </summary>
        public IList<object> Parameters
        {
            get { return _parameters; }
        }

        /// <summary>
        /// Gets the parameters.
        /// </summary>
        public IList<Expression> ParameterExpressions
        {
            get { return _parameterExpressions; }
        }

        /// <summary>
        /// Gets the aliases.
        /// </summary>
        public AliasDictionary Aliases
        {
            get { return _aliases; }
        }

        /** <inheritdoc /> */
        public override void VisitQueryModel(QueryModel queryModel)
        {
            VisitQueryModel(queryModel, false);
        }

        /// <summary>
        /// Visits the query model.
        /// </summary>
        private void VisitQueryModel(QueryModel queryModel, bool forceStar)
        {
            _aliases.Push();

            // SELECT
            _builder.Append("select ");

            // TOP 1 FLD1, FLD2
            VisitSelectors(queryModel, forceStar);

            // FROM ... WHERE ... JOIN ...
            base.VisitQueryModel(queryModel);

            // UNION ...
            ProcessResultOperatorsEnd(queryModel);

            _aliases.Pop();
        }

        /// <summary>
        /// Visits the selectors.
        /// </summary>
        public void VisitSelectors(QueryModel queryModel, bool forceStar)
        {
            var parenCount = ProcessResultOperatorsBegin(queryModel);

            if (parenCount >= 0)
            {
                // FIELD1, FIELD2
                BuildSqlExpression(queryModel.SelectClause.Selector, forceStar || parenCount > 0);
                _builder.Append(')', parenCount).Append(" ");
            }
        }

        /// <summary>
        /// Processes the result operators that come right after SELECT: min/max/count/sum/distinct
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1800:DoNotCastUnnecessarily")]
        private int ProcessResultOperatorsBegin(QueryModel queryModel)
        {
            int parenCount = 0;

            foreach (var op in queryModel.ResultOperators.Reverse())
            {
                if (op is CountResultOperator || op is AnyResultOperator)
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
                else if (op is AverageResultOperator)
                {
                    _builder.Append("avg (");
                    parenCount++;
                }
                else if (op is DistinctResultOperator)
                    _builder.Append("distinct ");
                else if (op is FirstResultOperator || op is SingleResultOperator)
                    _builder.Append("top 1 ");
                else if (op is UnionResultOperator || op is IntersectResultOperator || op is ExceptResultOperator
                         || op is DefaultIfEmptyResultOperator || op is SkipResultOperator || op is TakeResultOperator)
                    // Will be processed later
                    break;
                else if (op is ContainsResultOperator)
                    // Should be processed already
                    break;
                else
                    throw new NotSupportedException("Operator is not supported: " + op);
            }
            return parenCount;
        }

        /// <summary>
        /// Processes the result operators that go in the end of the query: limit/offset/union/intersect/except
        /// </summary>
        private void ProcessResultOperatorsEnd(QueryModel queryModel)
        {
            ProcessSkipTake(queryModel);

            foreach (var op in queryModel.ResultOperators.Reverse())
            {
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

        /// <summary>
        /// Processes the pagination (skip/take).
        /// </summary>
        private void ProcessSkipTake(QueryModel queryModel)
        {
            var limit = queryModel.ResultOperators.OfType<TakeResultOperator>().FirstOrDefault();
            var offset = queryModel.ResultOperators.OfType<SkipResultOperator>().FirstOrDefault();

            if (limit == null && offset == null)
                return;

            // "limit" is mandatory if there is "offset", but not vice versa
            _builder.Append("limit ");

            if (limit == null)
            {
                // Workaround for unlimited offset (IGNITE-2602)
                // H2 allows NULL & -1 for unlimited, but Ignite indexing does not
                // Maximum limit that works is (int.MaxValue - offset) 

                if (offset.Count is ParameterExpression)
                    throw new NotSupportedException("Skip() without Take() is not supported in compiled queries.");

                var offsetInt = (int) ((ConstantExpression) offset.Count).Value;
                _builder.Append((int.MaxValue - offsetInt).ToString(CultureInfo.InvariantCulture));
            }
            else
                BuildSqlExpression(limit.Count);

            if (offset != null)
            {
                _builder.Append(" offset ");
                BuildSqlExpression(offset.Count);
            }
        }

        /** <inheritdoc /> */
        protected override void VisitBodyClauses(ObservableCollection<IBodyClause> bodyClauses, QueryModel queryModel)
        {
            var i = 0;
            foreach (var join in bodyClauses.OfType<JoinClause>())
                VisitJoinClause(join, queryModel, i++);

            var hasGroups = ProcessGroupings(queryModel);

            i = 0;
            foreach (var where in bodyClauses.OfType<WhereClause>())
                VisitWhereClause(where, i++, hasGroups);

            i = 0;
            foreach (var orderBy in bodyClauses.OfType<OrderByClause>())
                VisitOrderByClause(orderBy, queryModel, i++);
        }

        /// <summary>
        /// Processes the groupings.
        /// </summary>
        private bool ProcessGroupings(QueryModel queryModel)
        {
            var subQuery = queryModel.MainFromClause.FromExpression as SubQueryExpression;

            if (subQuery == null)
                return false;

            var groupBy = subQuery.QueryModel.ResultOperators.OfType<GroupResultOperator>().FirstOrDefault();

            if (groupBy == null)
                return false;

            // Visit inner joins before grouping
            var i = 0;
            foreach (var join in subQuery.QueryModel.BodyClauses.OfType<JoinClause>())
                VisitJoinClause(join, queryModel, i++);

            // Append grouping
            _builder.Append("group by (");

            BuildSqlExpression(groupBy.KeySelector);

            _builder.Append(") ");

            return true;
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public override void VisitMainFromClause(MainFromClause fromClause, QueryModel queryModel)
        {
            base.VisitMainFromClause(fromClause, queryModel);

            _builder.AppendFormat("from ");
            _aliases.AppendAsClause(_builder, fromClause).Append(" ");

            foreach (var additionalFrom in queryModel.BodyClauses.OfType<AdditionalFromClause>())
            {
                _builder.AppendFormat(", ");
                _aliases.AppendAsClause(_builder, additionalFrom).Append(" ");
            }
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public override void VisitWhereClause(WhereClause whereClause, QueryModel queryModel, int index)
        {
            base.VisitWhereClause(whereClause, queryModel, index);

            VisitWhereClause(whereClause, index, false);
        }

        /// <summary>
        /// Visits the where clause.
        /// </summary>
        private void VisitWhereClause(WhereClause whereClause, int index, bool hasGroups)
        {
            _builder.Append(index > 0
                ? "and "
                : hasGroups
                    ? "having"
                    : "where ");

            BuildSqlExpression(whereClause.Predicate);

            _builder.Append(" ");
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public override void VisitOrderByClause(OrderByClause orderByClause, QueryModel queryModel, int index)
        {
            base.VisitOrderByClause(orderByClause, queryModel, index);

            _builder.Append("order by ");

            for (int i = 0; i < orderByClause.Orderings.Count; i++)
            {
                var ordering = orderByClause.Orderings[i];

                if (i > 0)
                    _builder.Append(", ");

                _builder.Append("(");

                BuildSqlExpression(ordering.Expression);

                _builder.Append(")");

                _builder.Append(ordering.OrderingDirection == OrderingDirection.Asc ? " asc" : " desc");
            }

            _builder.Append(" ");
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public override void VisitJoinClause(JoinClause joinClause, QueryModel queryModel, int index)
        {
            base.VisitJoinClause(joinClause, queryModel, index);

            var subQuery = joinClause.InnerSequence as SubQueryExpression;

            if (subQuery != null)
            {
                var isOuter = subQuery.QueryModel.ResultOperators.OfType<DefaultIfEmptyResultOperator>().Any();

                _builder.AppendFormat("{0} join (", isOuter ? "left outer" : "inner");

                VisitQueryModel(subQuery.QueryModel, true);

                var alias = _aliases.GetTableAlias(subQuery.QueryModel.MainFromClause);
                _builder.AppendFormat(") as {0} on (", alias);
            }
            else
            {
                var queryable = ExpressionWalker.GetCacheQueryable(joinClause);
                var tableName = ExpressionWalker.GetTableNameWithSchema(queryable);
                var alias = _aliases.GetTableAlias(joinClause);
                _builder.AppendFormat("inner join {0} as {1} on (", tableName, alias);
            }

            BuildJoinCondition(joinClause.InnerKeySelector, joinClause.OuterKeySelector);

            _builder.Append(") ");
        }

        /// <summary>
        /// Builds the join condition ('x=y AND foo=bar').
        /// </summary>
        /// <param name="innerKey">The inner key selector.</param>
        /// <param name="outerKey">The outer key selector.</param>
        /// <exception cref="System.NotSupportedException">
        /// </exception>
        private void BuildJoinCondition(Expression innerKey, Expression outerKey)
        {
            var innerNew = innerKey as NewExpression;
            var outerNew = outerKey as NewExpression;

            if (innerNew == null && outerNew == null)
            {
                BuildJoinSubCondition(innerKey, outerKey);
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

                    BuildJoinSubCondition(innerNew.Arguments[i], outerNew.Arguments[i]);
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
        private void BuildJoinSubCondition(Expression innerKey, Expression outerKey)
        {
            BuildSqlExpression(innerKey);
            _builder.Append(" = ");
            BuildSqlExpression(outerKey);
        }

        /// <summary>
        /// Builds the SQL expression.
        /// </summary>
        private void BuildSqlExpression(Expression expression, bool useStar = false)
        {
            new CacheQueryExpressionVisitor(this, useStar).Visit(expression);
        }
    }
}
