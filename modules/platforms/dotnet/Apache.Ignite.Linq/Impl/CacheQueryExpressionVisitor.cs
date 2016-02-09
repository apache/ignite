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
    using Apache.Ignite.Core.Cache.Configuration;
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
        protected override Expression VisitQuerySourceReference(QuerySourceReferenceExpression expression)
        {
            // Count, sum, max, min expect a single field or *
            // In other cases we need both parts of cache entry
            var format = _useStar ? "{0}.*" : "{0}._key, {0}._val";

            var tableName = Aliases.GetTableAlias(TableNameMapper.GetTableNameWithSchema(expression));

            ResultBuilder.AppendFormat(format, tableName);

            return expression;
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        protected override Expression VisitMember(MemberExpression expression)
        {
            // Field hierarchy is flattened (Person.Address.Street is just Street), append as is, do not call Visit.
            // TODO: Aliases? How do they work? See email.

            // Special case: string.Length
            if (expression.Member == MethodVisitor.StringLength)
            {
                ResultBuilder.Append("length(");

                VisitMember((MemberExpression) expression.Expression);

                ResultBuilder.Append(")");

                return expression;
            }

            // Special case: grouping
            if (VisitGroupByMember(expression.Expression))
                return expression;

            var queryFieldAttr = expression.Member.GetCustomAttributes(true)
                .OfType<QuerySqlFieldAttribute>().FirstOrDefault();

            var fieldName = queryFieldAttr == null || string.IsNullOrEmpty(queryFieldAttr.Name)
                ? expression.Member.Name
                : queryFieldAttr.Name;

            ResultBuilder.AppendFormat("{0}.{1}",
                Aliases.GetTableAlias(TableNameMapper.GetTableNameWithSchema(expression)), fieldName);

            return expression;
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
                throw new NotSupportedException("Unexpected subquery in a member expression: " + expression);

            var subQuery = from.FromExpression as SubQueryExpression;
            if (subQuery == null)
                throw new NotSupportedException("Unexpected subquery in a member expression: " + from);

            var resOp = subQuery.QueryModel.ResultOperators;
            if (resOp.Count != 1)
                throw new NotSupportedException("Unexpected subquery in a member expression: " + from);

            var groupBy = resOp[0] as GroupResultOperator;

            if (groupBy == null)
                throw new NotSupportedException("Unexpected subquery in a member expression: " + from);

            Visit(groupBy.KeySelector);

            return true;
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        protected override Expression VisitConstant(ConstantExpression expression)
        {
            ResultBuilder.Append("?");

            _modelVisitor.Parameters.Add(expression.Value);

            return expression;
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
            // This happens when New expression uses a subquery, in a GroupBy.
            _modelVisitor.VisitSelectors(expression.QueryModel, false);

            return expression;
        }

        /** <inheritdoc /> */
        protected override Exception CreateUnhandledItemException<T>(T unhandledItem, string visitMethod)
        {
            return new NotSupportedException(string.Format("The expression '{0}' (type: {1}) is not supported by this LINQ provider.", unhandledItem, typeof (T)));
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
