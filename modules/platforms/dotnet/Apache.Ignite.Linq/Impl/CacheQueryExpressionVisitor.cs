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
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
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

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheQueryExpressionVisitor" /> class.
        /// </summary>
        /// <param name="modelVisitor">The _model visitor.</param>
        /// <param name="useStar">Flag indicating that star '*' qualifier should be used
        /// for the whole-table select instead of _key, _val.</param>
        public CacheQueryExpressionVisitor(CacheQueryModelVisitor modelVisitor, bool useStar)
        {
            Debug.Assert(modelVisitor != null);

            _modelVisitor = modelVisitor;
            _useStar = useStar;
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
            ResultBuilder.Append("(");

            switch (expression.NodeType)
            {
                case ExpressionType.Negate:
                    ResultBuilder.Append("-");
                    break;
                case ExpressionType.Not:
                    ResultBuilder.Append("not ");
                    break;
                case ExpressionType.Convert:
                    // Ignore, let the db do the conversion
                    break;
                default:
                    return base.VisitUnary(expression);
            }

            Visit(expression.Operand);

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
                    var rightConst = expression.Right as ConstantExpression;

                    if (rightConst != null && rightConst.Value == null)
                    {
                        // Special case for nulls, since "= null" does not work in SQL
                        ResultBuilder.Append(" is null)");
                        return expression;
                    }

                    ResultBuilder.Append(" = ");
                    break;
                }

                case ExpressionType.NotEqual:
                {
                    var rightConst = expression.Right as ConstantExpression;

                    if (rightConst != null && rightConst.Value == null)
                    {
                        // Special case for nulls, since "<> null" does not work in SQL
                        ResultBuilder.Append(" is not null)");
                        return expression;
                    }

                    ResultBuilder.Append(" <> ");
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
            // Count, sum, max, min expect a single field or *
            // In other cases we need both parts of cache entry
            var format = _useStar ? "{0}.*" : "{0}._key, {0}._val";

            var tableName = Aliases.GetTableAlias(expression);

            ResultBuilder.AppendFormat(format, tableName);

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
                var fieldName = GetFieldName(expression, queryable);

                ResultBuilder.AppendFormat("{0}.{1}", Aliases.GetTableAlias(expression), fieldName);
            }
            else
                AppendParameter(RegisterEvaluatedParameter(expression));

            return expression;
        }

        /// <summary>
        /// Registers query parameter that is evaluated from a lambda expression argument.
        /// </summary>
        public object RegisterEvaluatedParameter(Expression expression)
        {
            _modelVisitor.ParameterExpressions.Add(expression);

            return ExpressionWalker.EvaluateExpression<object>(expression);
        }

        /// <summary>
        /// Gets the name of the field from a member expression.
        /// </summary>
        private static string GetFieldName(MemberExpression expression, ICacheQueryableInternal queryable)
        {
            var fieldName = GetMemberFieldName(expression.Member);

            // Look for a field alias
            var cacheCfg = queryable.CacheConfiguration;

            if (cacheCfg.QueryEntities == null || cacheCfg.QueryEntities.All(x => x.Aliases == null))
                return fieldName;  // There are no aliases defined - early exit

            // Find query entity by key-val types
            var keyValTypes = queryable.ElementType.GetGenericArguments();

            Debug.Assert(keyValTypes.Length == 2);

            var entity = cacheCfg.QueryEntities.FirstOrDefault(e =>
                e.Aliases != null &&
                (e.KeyType == keyValTypes[0] || e.KeyTypeName == keyValTypes[0].Name) &&
                (e.ValueType == keyValTypes[1] || e.ValueTypeName == keyValTypes[1].Name));

            if (entity == null)
                return fieldName;

            // There are some aliases for the current query type
            // Calculate full field name and look for alias
            var fullFieldName = fieldName;
            var member = expression;

            while ((member = member.Expression as MemberExpression) != null &&
                   member.Member.DeclaringType != queryable.ElementType)
                fullFieldName = GetFieldName(member, queryable) + "." + fullFieldName;

            var alias = entity.Aliases.Where(x => x.FullName == fullFieldName)
                .Select(x => x.Alias).FirstOrDefault();

            return alias ?? fieldName;
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
                    return "_" + m.Name.ToLowerInvariant().Substring(0, 3);

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
            AppendParameter(expression.Value);

            return expression;
        }

        /// <summary>
        /// Appends the parameter.
        /// </summary>
        private void AppendParameter(object value)
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
                _modelVisitor.VisitQueryModel(subQueryModel);
                ResultBuilder.Append(")");
            }
            else
            {
                var inValues = GetInValues(fromExpression).ToArray();

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
        /// Gets values for IN expression.
        /// </summary>
        private static IEnumerable<object> GetInValues(Expression fromExpression)
        {
            IEnumerable result;
            switch (fromExpression.NodeType)
            {
                case ExpressionType.MemberAccess:
                    var memberExpression = (MemberExpression) fromExpression;
                    result = ExpressionWalker.EvaluateExpression<IEnumerable>(memberExpression);
                    break;
                case ExpressionType.ListInit:
                    var listInitExpression = (ListInitExpression) fromExpression;
                    result = listInitExpression.Initializers
                        .SelectMany(init => init.Arguments)
                        .Select(ExpressionWalker.EvaluateExpression<object>);
                    break;
                case ExpressionType.NewArrayInit:
                    var newArrayExpression = (NewArrayExpression) fromExpression;
                    result = newArrayExpression.Expressions
                        .Select(ExpressionWalker.EvaluateExpression<object>);
                    break;
                case ExpressionType.Parameter:
                    // This should happen only when 'IEnumerable.Contains' is called on parameter of compiled query
                    throw new NotSupportedException("'Contains' clause coming from compiled query parameter is not supported.");
                default:
                    result = Expression.Lambda(fromExpression).Compile().DynamicInvoke() as IEnumerable;
                    break;
            }

            result = result ?? Enumerable.Empty<object>();

            return result
                .Cast<object>()
                .ToArray();
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
