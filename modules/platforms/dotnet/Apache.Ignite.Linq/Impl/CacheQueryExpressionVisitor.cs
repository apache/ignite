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

using System;
using System.Text;

namespace Apache.Ignite.Linq.Impl
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Common;
    using Remotion.Linq;
    using Remotion.Linq.Clauses;
    using Remotion.Linq.Clauses.Expressions;
    using Remotion.Linq.Clauses.ResultOperators;
    using Remotion.Linq.Parsing;

    /// <summary>
    /// Expression visitor, transforms query subexpressions (such as Where clauses) to SQL.
    /// </summary>
    internal class CacheQueryExpressionVisitor : ThrowingExpressionVisitor
    {
        /** */
        private readonly bool _useStar;

        /** */
        private readonly CacheQueryModelVisitor _modelVisitor;

        /** */
        private static readonly CopyOnWriteConcurrentDictionary<MemberInfo, string> FieldNameMap =
            new CopyOnWriteConcurrentDictionary<MemberInfo, string>();

        /** */
        private readonly bool _includeAllFields;

        /** */
        private readonly bool _visitEntireSubQueryModel;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheQueryExpressionVisitor" /> class.
        /// </summary>
        /// <param name="modelVisitor">The _model visitor.</param>
        /// <param name="useStar">
        /// Flag indicating that star '*' qualifier should be used
        /// for the whole-table select instead of _key, _val.
        /// </param>
        /// <param name="includeAllFields">
        /// Flag indicating that star '*' qualifier should be used
        /// for the whole-table select as well as _key, _val.
        /// </param>
        /// <param name="visitEntireSubQueryModel">
        /// Flag indicating that subquery 
        /// should be visited as full query
        /// </param>
        public CacheQueryExpressionVisitor(CacheQueryModelVisitor modelVisitor, bool useStar, bool includeAllFields,
            bool visitEntireSubQueryModel)
        {
            Debug.Assert(modelVisitor != null);

            _modelVisitor = modelVisitor;
            _useStar = useStar;
            _includeAllFields = includeAllFields;
            _visitEntireSubQueryModel = visitEntireSubQueryModel;
        }

        /// <summary>
        /// Gets the result builder.
        /// </summary>
        public StringBuilder ResultBuilder
        {
            get { return _modelVisitor.Builder; }
        }

        /// <summary>
        /// Gets the parameters.
        /// </summary>
        public IList<object> Parameters
        {
            get { return _modelVisitor.Parameters; }
        }

        /// <summary>
        /// Gets the aliases.
        /// </summary>
        private AliasDictionary Aliases
        {
            get { return _modelVisitor.Aliases; }
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        protected override Expression VisitUnary(UnaryExpression expression)
        {
            var closeBracket = false;

            switch (expression.NodeType)
            {
                case ExpressionType.Negate:
                    ResultBuilder.Append("(");
                    ResultBuilder.Append("-");
                    closeBracket = true;
                    break;

                case ExpressionType.Not:
                    ResultBuilder.Append("(");
                    ResultBuilder.Append("not ");
                    closeBracket = true;
                    break;

                case ExpressionType.Convert:
                    // Ignore, let the db do the conversion
                    break;

                default:
                    return base.VisitUnary(expression);
            }

            Visit(expression.Operand);

            if(closeBracket)
                ResultBuilder.Append(")");

            return expression;
        }

        /// <summary>
        /// Visits the binary function.
        /// </summary>
        /// <param name="expression">The expression.</param>
        /// <returns>True if function detected, otherwise false.</returns>
        private bool VisitBinaryFunc(BinaryExpression expression)
        {
            if (expression.NodeType == ExpressionType.Add && expression.Left.Type == typeof (string))
                ResultBuilder.Append("concat(");
            else if (expression.NodeType == ExpressionType.Coalesce)
                ResultBuilder.Append("coalesce(");
            else
                return false;

            Visit(expression.Left);
            ResultBuilder.Append(", ");
            Visit(expression.Right);
            ResultBuilder.Append(")");

            return true;
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        protected override Expression VisitBinary(BinaryExpression expression)
        {
            // Either func or operator
            if (VisitBinaryFunc(expression))
                return expression;

            ResultBuilder.Append("(");

            Visit(expression.Left);

            switch (expression.NodeType)
            {
                case ExpressionType.Equal:
                {
                    // Use `IS [NOT] DISTINCT FROM` for correct null comparison semantics.
                    // E.g. when user says `.Where(x => x == null)`, it should work, but with `=` it does not.
                    ResultBuilder.Append(" IS NOT DISTINCT FROM ");

                    break;
                }

                case ExpressionType.NotEqual:
                {
                    ResultBuilder.Append(" IS DISTINCT FROM ");

                    break;
                }

                case ExpressionType.AndAlso:
                case ExpressionType.And:
                    ResultBuilder.Append(" and ");
                    break;

                case ExpressionType.OrElse:
                case ExpressionType.Or:
                    ResultBuilder.Append(" or ");
                    break;

                case ExpressionType.Add:
                    ResultBuilder.Append(" + ");
                    break;

                case ExpressionType.Subtract:
                    ResultBuilder.Append(" - ");
                    break;

                case ExpressionType.Multiply:
                    ResultBuilder.Append(" * ");
                    break;

                case ExpressionType.Modulo:
                    ResultBuilder.Append(" % ");
                    break;

                case ExpressionType.Divide:
                    ResultBuilder.Append(" / ");
                    break;

                case ExpressionType.GreaterThan:
                    ResultBuilder.Append(" > ");
                    break;

                case ExpressionType.GreaterThanOrEqual:
                    ResultBuilder.Append(" >= ");
                    break;

                case ExpressionType.LessThan:
                    ResultBuilder.Append(" < ");
                    break;

                case ExpressionType.LessThanOrEqual:
                    ResultBuilder.Append(" <= ");
                    break;

                case ExpressionType.Coalesce:
                    break;

                default:
                    base.VisitBinary(expression);
                    break;
            }

            Visit(expression.Right);
            ResultBuilder.Append(")");

            return expression;
        }

        /** <inheritdoc /> */
        public override Expression Visit(Expression expression)
        {
            var paramExpr = expression as ParameterExpression;

            if (paramExpr != null)
            {
                // This happens only with compiled queries, where parameters come from enclosing lambda.
                AppendParameter(paramExpr);
                return expression;
            }

            return base.Visit(expression);
        }

        /** <inheritdoc /> */
        protected override Expression VisitQuerySourceReference(QuerySourceReferenceExpression expression)
        {
            // In some cases of Join clause different handling should be introduced
            var joinClause = expression.ReferencedQuerySource as JoinClause;
            if (joinClause != null && ExpressionWalker.GetCacheQueryable(expression, false) == null)
            {
                var tableName = Aliases.GetTableAlias(expression);
                var fieldname = Aliases.GetFieldAlias(expression);

                ResultBuilder.AppendFormat("{0}.{1}", tableName, fieldname);
            }
            else if (joinClause != null && joinClause.InnerSequence is SubQueryExpression)
            {
                var subQueryExpression = (SubQueryExpression) joinClause.InnerSequence;
                base.Visit(subQueryExpression.QueryModel.SelectClause.Selector);
            }
            else
            {
                // Count, sum, max, min expect a single field or *
                // In other cases we need both parts of cache entry
                var format = _includeAllFields
                    ? "{0}.*, {0}._KEY, {0}._VAL"
                    : _useStar
                        ? "{0}.*"
                        : "{0}._KEY, {0}._VAL";

                var tableName = Aliases.GetTableAlias(expression);

                ResultBuilder.AppendFormat(format, tableName);
            }

            return expression;
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        protected override Expression VisitMember(MemberExpression expression)
        {
            // Field hierarchy is flattened (Person.Address.Street is just Street), append as is, do not call Visit.

            // Property call (string.Length, DateTime.Month, etc).
            if (MethodVisitor.VisitPropertyCall(expression, this))
                return expression;

            // Special case: grouping
            if (VisitGroupByMember(expression.Expression))
                return expression;

            var queryable = ExpressionWalker.GetCacheQueryable(expression, false);

            if (queryable != null)
            {
                var fieldName = GetEscapedFieldName(expression, queryable);

                ResultBuilder.AppendFormat("{0}.{1}", Aliases.GetTableAlias(expression), fieldName);
            }
            else
                AppendParameter(ExpressionWalker.EvaluateExpression<object>(expression));

            return expression;
        }

        /// <summary>
        /// Gets the name of the field from a member expression, with quotes when necessary.
        /// </summary>
        private static string GetEscapedFieldName(MemberExpression expression, ICacheQueryableInternal queryable)
        {
            var sqlEscapeAll = queryable.CacheConfiguration.SqlEscapeAll;
            var fieldName = GetFieldName(expression, queryable);

            return sqlEscapeAll ? string.Format("\"{0}\"", fieldName) : fieldName;
        }

        /// <summary>
        /// Gets the name of the field from a member expression, with quotes when necessary.
        /// </summary>
        private static string GetFieldName(MemberExpression expression, ICacheQueryableInternal queryable,
            bool ignoreAlias = false)
        {
            var fieldName = GetMemberFieldName(expression.Member);

            // Look for a field alias
            var cacheCfg = queryable.CacheConfiguration;

            if (cacheCfg.QueryEntities == null || cacheCfg.QueryEntities.All(x => x.Aliases == null))
            {
                // There are no aliases defined - early exit.
                return fieldName;
            }

            // Find query entity by key-val types
            var entity = GetQueryEntity(queryable, cacheCfg);

            if (entity == null)
            {
                return fieldName;
            }

            // There are some aliases for the current query type
            // Calculate full field name and look for alias
            var fullFieldName = fieldName;
            var member = expression;

            while ((member = member.Expression as MemberExpression) != null &&
                   member.Member.DeclaringType != queryable.ElementType)
            {
                fullFieldName = GetFieldName(member, queryable, true) + "." + fullFieldName;
            }

            var alias = ignoreAlias ? null : ((IQueryEntityInternal)entity).GetAlias(fullFieldName);

            return alias ?? fieldName;
        }

        /// <summary>
        /// Finds matching query entity in the cache configuration.
        /// </summary>
        private static QueryEntity GetQueryEntity(ICacheQueryableInternal queryable, CacheConfiguration cacheCfg)
        {
            if (cacheCfg.QueryEntities.Count == 1)
            {
                return cacheCfg.QueryEntities.Single();
            }

            var keyValTypes = queryable.ElementType.GetGenericArguments();

            Debug.Assert(keyValTypes.Length == 2);

            // PERF: No LINQ.
            foreach (var e in cacheCfg.QueryEntities)
            {
                if (e.Aliases != null
                    && (e.KeyType == keyValTypes[0] || e.KeyTypeName == keyValTypes[0].FullName)
                    && (e.ValueType == keyValTypes[1] || e.ValueTypeName == keyValTypes[1].FullName))
                {
                    return e;
                }
            }
            
            return null;
        }

        /// <summary>
        /// Gets the name of the member field.
        /// </summary>
        [SuppressMessage("Microsoft.Globalization", "CA1308:NormalizeStringsToUppercase", 
            Justification = "Not applicable.")]
        private static string GetMemberFieldName(MemberInfo member)
        {
            string fieldName;

            if (FieldNameMap.TryGetValue(member, out fieldName))
                return fieldName;

            return FieldNameMap.GetOrAdd(member, m =>
            {
                // Special case: _key, _val
                if (m.DeclaringType != null &&
                    m.DeclaringType.IsGenericType &&
                    m.DeclaringType.GetGenericTypeDefinition() == typeof (ICacheEntry<,>))
                    return "_" + m.Name.ToUpperInvariant().Substring(0, 3);

                var qryFieldAttr = m.GetCustomAttributes(true)
                    .OfType<QuerySqlFieldAttribute>().FirstOrDefault();

                return qryFieldAttr == null || string.IsNullOrEmpty(qryFieldAttr.Name)
                    ? m.Name
                    : qryFieldAttr.Name;
            });
        }

        /// <summary>
        /// Visits the group by member.
        /// </summary>
        private bool VisitGroupByMember(Expression expression)
        {
            var srcRef = expression as QuerySourceReferenceExpression;
            if (srcRef == null)
                return false;

            var from = srcRef.ReferencedQuerySource as IFromClause;
            if (from == null)
                return false;

            var subQry = from.FromExpression as SubQueryExpression;
            if (subQry == null)
                return false;

            var grpBy = subQry.QueryModel.ResultOperators.OfType<GroupResultOperator>().FirstOrDefault();
            if (grpBy == null)
                return false;

            Visit(grpBy.KeySelector);

            return true;
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        protected override Expression VisitConstant(ConstantExpression expression)
        {
            if (MethodVisitor.VisitConstantCall(expression, this))
            {
                return expression;
            }

            AppendParameter(expression.Value);

            return expression;
        }

        /// <summary>
        /// Appends the parameter.
        /// </summary>
        public void AppendParameter(object value)
        {
            ResultBuilder.Append("?");

            _modelVisitor.Parameters.Add(value);
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        protected override Expression VisitMethodCall(MethodCallExpression expression)
        {
            MethodVisitor.VisitMethodCall(expression, this);

            return expression;
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        protected override Expression VisitNew(NewExpression expression)
        {
            VisitArguments(expression.Arguments);

            return expression;
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        protected override Expression VisitInvocation(InvocationExpression expression)
        {
            VisitArguments(expression.Arguments);

            return expression;
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        protected override Expression VisitConditional(ConditionalExpression expression)
        {
            ResultBuilder.Append("casewhen(");

            Visit(expression.Test);

            // Explicit type specification is required when all arguments of CASEWHEN are parameters
            ResultBuilder.Append(", cast(");
            Visit(expression.IfTrue);
            ResultBuilder.AppendFormat(" as {0}), ", SqlTypes.GetSqlTypeName(expression.Type) ?? "other");

            Visit(expression.IfFalse);
            ResultBuilder.Append(")");

            return expression;
        }

        /** <inheritdoc /> */

        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        protected override Expression VisitSubQuery(SubQueryExpression expression)
        {
            var subQueryModel = expression.QueryModel;

            var contains = subQueryModel.ResultOperators.FirstOrDefault() as ContainsResultOperator;

            // Check if IEnumerable.Contains is used.
            if (subQueryModel.ResultOperators.Count == 1 && contains != null) 
            {
                VisitContains(subQueryModel, contains);
            }
            else if (_visitEntireSubQueryModel)
            {
                ResultBuilder.Append("(");
                _modelVisitor.VisitQueryModel(subQueryModel, false, true);
                ResultBuilder.Append(")");
            }
            else
            {
                // This happens when New expression uses a subquery, in a GroupBy.
                _modelVisitor.VisitSelectors(expression.QueryModel, false);
            }

            return expression;
        }

        /// <summary>
        /// Visits IEnumerable.Contains
        /// </summary>
        private void VisitContains(QueryModel subQueryModel, ContainsResultOperator contains)
        {
            ResultBuilder.Append("(");

            var fromExpression = subQueryModel.MainFromClause.FromExpression;

            var queryable = ExpressionWalker.GetCacheQueryable(fromExpression, false);

            if (queryable != null)
            {
                Visit(contains.Item);

                ResultBuilder.Append(" IN (");
                if (_visitEntireSubQueryModel)
                {
                    _modelVisitor.VisitQueryModel(subQueryModel, false, true);
                }
                else
                {
                    _modelVisitor.VisitQueryModel(subQueryModel);
                }
                
                ResultBuilder.Append(")");
            }
            else
            {
                var inValues = ExpressionWalker.EvaluateEnumerableValues(fromExpression).ToArray();

                var hasNulls = inValues.Any(o => o == null);

                if (hasNulls)
                {
                    ResultBuilder.Append("(");
                }

                Visit(contains.Item);

                ResultBuilder.Append(" IN (");
                AppendInParameters(inValues);
                ResultBuilder.Append(")");

                if (hasNulls)
                {
                    ResultBuilder.Append(") OR ");
                    Visit(contains.Item);
                    ResultBuilder.Append(" IS NULL");
                }
            }

            ResultBuilder.Append(")");
        }

        /// <summary>
        /// Appends not null parameters using ", " as delimeter.
        /// </summary>
        private void AppendInParameters(IEnumerable<object> values)
        {
            var first = true;

            foreach (var val in values)
            {
                if (val == null)
                    continue;

                if (!first)
                {
                    ResultBuilder.Append(", ");
                }

                first = false;

                AppendParameter(val);
            }
        }

        /** <inheritdoc /> */
        protected override Exception CreateUnhandledItemException<T>(T unhandledItem, string visitMethod)
        {
            return new NotSupportedException(string.Format("The expression '{0}' (type: {1}) is not supported.",
                unhandledItem, typeof (T)));
        }

        /// <summary>
        /// Visits multiple arguments.
        /// </summary>
        /// <param name="arguments">The arguments.</param>
        private void VisitArguments(IEnumerable<Expression> arguments)
        {
            var first = true;

            foreach (var e in arguments)
            {
                if (!first)
                {
                    if (_useStar)
                        throw new NotSupportedException("Aggregate functions do not support multiple fields");

                    ResultBuilder.Append(", ");
                }

                first = false;

                Visit(e);
            }
        }
    }
}
