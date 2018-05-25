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
    using Apache.Ignite.Linq.Impl.Dml;
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
        private readonly AliasDictionary _aliases = new AliasDictionary();

        /** */
        private static readonly Type DefaultIfEmptyEnumeratorType = new object[0]
            .DefaultIfEmpty()
            .GetType()
            .GetGenericTypeDefinition();

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

            return new QueryData(qryText, _parameters);
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
        internal void VisitQueryModel(QueryModel queryModel, bool includeAllFields, bool copyAliases = false)
        {
            _aliases.Push(copyAliases);

            var lastResultOp = queryModel.ResultOperators.LastOrDefault();
            if (lastResultOp is RemoveAllResultOperator)
            {
                VisitRemoveOperator(queryModel);
            }
            else if(lastResultOp is UpdateAllResultOperator)
            {
                VisitUpdateAllOperator(queryModel);
            }
            else
            {
                // SELECT
                _builder.Append("select ");

                // TOP 1 FLD1, FLD2
                VisitSelectors(queryModel, includeAllFields);

                // FROM ... WHERE ... JOIN ...
                base.VisitQueryModel(queryModel);

                // UNION ...
                ProcessResultOperatorsEnd(queryModel);
            }

            _aliases.Pop();
        }

        /// <summary>
        /// Visits the remove operator.
        /// </summary>
        private void VisitRemoveOperator(QueryModel queryModel)
        {
            var resultOps = queryModel.ResultOperators;

            _builder.Append("delete ");

            if (resultOps.Count == 2)
            {
                var resOp = resultOps[0] as TakeResultOperator;

                if (resOp == null)
                    throw new NotSupportedException(
                        "RemoveAll can not be combined with result operators (other than Take): " +
                        resultOps[0].GetType().Name);

                _builder.Append("top ");
                BuildSqlExpression(resOp.Count);
                _builder.Append(" ");
            }
            else if (resultOps.Count > 2)
            {
                throw new NotSupportedException(
                    "RemoveAll can not be combined with result operators (other than Take): " +
                    string.Join(", ", resultOps.Select(x => x.GetType().Name)));
            }

            // FROM ... WHERE ... JOIN ...
            base.VisitQueryModel(queryModel);
        }

        /// <summary>
        /// Visits the UpdateAll operator.
        /// </summary>
        private void VisitUpdateAllOperator(QueryModel queryModel)
        {
            var resultOps = queryModel.ResultOperators;

            _builder.Append("update ");

            // FROM ... WHERE ...
            base.VisitQueryModel(queryModel);

            if (resultOps.Count == 2)
            {
                var resOp = resultOps[0] as TakeResultOperator;

                if (resOp == null)
                    throw new NotSupportedException(
                        "UpdateAll can not be combined with result operators (other than Take): " +
                        resultOps[0].GetType().Name);

                _builder.Append("limit ");
                BuildSqlExpression(resOp.Count);
            }
            else if (resultOps.Count > 2)
            {
                throw new NotSupportedException(
                    "UpdateAll can not be combined with result operators (other than Take): " +
                    string.Join(", ", resultOps.Select(x => x.GetType().Name)));
            }
        }

        /// <summary>
        /// Visits the selectors.
        /// </summary>
        public void VisitSelectors(QueryModel queryModel, bool includeAllFields)
        {
            var parenCount = ProcessResultOperatorsBegin(queryModel);

            if (parenCount >= 0)
            {
                // FIELD1, FIELD2
                BuildSqlExpression(queryModel.SelectClause.Selector, parenCount > 0, includeAllFields);
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

            i = 0;
            foreach (var where in subQuery.QueryModel.BodyClauses.OfType<WhereClause>())
                VisitWhereClause(where, i++, false);

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

            var isUpdateQuery = queryModel.ResultOperators.LastOrDefault() is UpdateAllResultOperator;
            if (!isUpdateQuery)
            {
                _builder.Append("from ");
            }

            ValidateFromClause(fromClause);
            _aliases.AppendAsClause(_builder, fromClause).Append(" ");

            var i = 0;
            foreach (var additionalFrom in queryModel.BodyClauses.OfType<AdditionalFromClause>())
            {
                _builder.Append(", ");
                ValidateFromClause(additionalFrom);

                VisitAdditionalFromClause(additionalFrom, queryModel, i++);
            }

            if (isUpdateQuery)
            {
                BuildSetClauseForUpdateAll(queryModel);
            }
        }

        /// <summary>
        /// Validates from clause.
        /// </summary>
        // ReSharper disable once UnusedParameter.Local
        private static void ValidateFromClause(IFromClause clause)
        {
            // Only IQueryable can be used in FROM clause. IEnumerable is not supported.
            if (!typeof(IQueryable).IsAssignableFrom(clause.FromExpression.Type))
            {
                throw new NotSupportedException("FROM clause must be IQueryable: " + clause);
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
            var queryable = ExpressionWalker.GetCacheQueryable(joinClause, false);

            if (queryable != null)
            {
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
                    var tableName = ExpressionWalker.GetTableNameWithSchema(queryable);
                    var alias = _aliases.GetTableAlias(joinClause);
                    _builder.AppendFormat("inner join {0} as {1} on (", tableName, alias);
                }
            }
            else
            {
                VisitJoinWithLocalCollectionClause(joinClause);
            }

            BuildJoinCondition(joinClause.InnerKeySelector, joinClause.OuterKeySelector);

            _builder.Append(") ");
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public override void VisitAdditionalFromClause(AdditionalFromClause fromClause, QueryModel queryModel,
            int index)
        {
            base.VisitAdditionalFromClause(fromClause, queryModel, index);

            var subQuery = fromClause.FromExpression as SubQueryExpression;
            if (subQuery != null)
            {
                _builder.Append("(");

                VisitQueryModel(subQuery.QueryModel, true);

                var alias = _aliases.GetTableAlias(subQuery.QueryModel.MainFromClause);
                _builder.AppendFormat(") as {0} ", alias);
            }
            else
            {
                _aliases.AppendAsClause(_builder, fromClause).Append(" ");
            }
        }

        /// <summary>
        /// Visists Join clause in case of join with local collection
        /// </summary>
        private void VisitJoinWithLocalCollectionClause(JoinClause joinClause)
        {
            var type = joinClause.InnerSequence.Type;

            var itemType = EnumerableHelper.GetIEnumerableItemType(type);

            var sqlTypeName = SqlTypes.GetSqlTypeName(itemType);

            if (string.IsNullOrWhiteSpace(sqlTypeName))
            {
                throw new NotSupportedException("Not supported item type for Join with local collection: " + type.Name);
            }

            var isOuter = false;
            var sequenceExpression = joinClause.InnerSequence;
            object values;

            var subQuery = sequenceExpression as SubQueryExpression;
            if (subQuery != null)
            {
                isOuter = subQuery.QueryModel.ResultOperators.OfType<DefaultIfEmptyResultOperator>().Any();
                sequenceExpression = subQuery.QueryModel.MainFromClause.FromExpression;
            }

            switch (sequenceExpression.NodeType)
            {
                case ExpressionType.Constant:
                    var constantValueType = ((ConstantExpression)sequenceExpression).Value.GetType();
                    if (constantValueType.IsGenericType)
                    {
                        isOuter = DefaultIfEmptyEnumeratorType == constantValueType.GetGenericTypeDefinition();
                    }
                    values = ExpressionWalker.EvaluateEnumerableValues(sequenceExpression);
                    break;

                case ExpressionType.Parameter:
                    values = ExpressionWalker.EvaluateExpression<object>(sequenceExpression);
                    break;

                default:
                    throw new NotSupportedException("Expression not supported for Join with local collection: "
                                                    + sequenceExpression);
            }

            var tableAlias = _aliases.GetTableAlias(joinClause);
            var fieldAlias = _aliases.GetFieldAlias(joinClause.InnerKeySelector);

            _builder.AppendFormat("{0} join table ({1} {2} = ?) {3} on (", 
                isOuter ? "left outer" : "inner",
                fieldAlias,
                sqlTypeName, 
                tableAlias);

            Parameters.Add(values);
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
        private void BuildSqlExpression(Expression expression, bool useStar = false, bool includeAllFields = false,
            bool visitSubqueryModel = false)
        {
            new CacheQueryExpressionVisitor(this, useStar, includeAllFields, visitSubqueryModel).Visit(expression);
        }

        /// <summary>
        /// Builds SET clause of UPDATE statement
        /// </summary>
        private void BuildSetClauseForUpdateAll(QueryModel queryModel)
        {
            var updateAllResultOperator = queryModel.ResultOperators.LastOrDefault() as UpdateAllResultOperator;
            if (updateAllResultOperator != null)
            {
                _builder.Append("set ");
                var first = true;
                foreach (var update in updateAllResultOperator.Updates)
                {
                    if (!first) _builder.Append(", ");
                    first = false;
                    BuildSqlExpression(update.Selector);
                    _builder.Append(" = ");
                    BuildSqlExpression(update.Value, visitSubqueryModel: true);
                }

                _builder.Append(" ");
            }
        }
    }
}
