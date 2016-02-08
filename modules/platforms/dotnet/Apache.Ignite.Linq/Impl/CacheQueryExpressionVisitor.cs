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
    using Remotion.Linq.Clauses.Expressions;
    using Remotion.Linq.Parsing;

    /// <summary>
    /// Expression visitor, transforms query subexpressions (such as Where clauses) to SQL.
    /// </summary>
    internal class CacheQueryExpressionVisitor : ThrowingExpressionVisitor
    {
        /** */
        private readonly bool _useStar;

        /** */
        private readonly StringBuilder _resultBuilder;

        /** */
        private readonly List<object> _parameters;

        /** */
        private readonly AliasDictionary _aliases;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheQueryExpressionVisitor" /> class.
        /// </summary>
        /// <param name="builder">The builder.</param>
        /// <param name="parameters">The parameters.</param>
        /// <param name="useStar">Flag indicating that star '*' qualifier should be used 
        /// for the whole-table select instead of _key, _val.</param>
        /// <param name="aliases">The aliases.</param>
        public CacheQueryExpressionVisitor(StringBuilder builder, List<object> parameters, bool useStar, 
            AliasDictionary aliases)
        {
            Debug.Assert(builder != null);
            Debug.Assert(parameters != null);
            Debug.Assert(aliases != null);

            _resultBuilder = builder;
            _parameters = parameters;
            _useStar = useStar;
            _aliases = aliases;
        }

        /// <summary>
        /// Gets the result builder.
        /// </summary>
        public StringBuilder ResultBuilder
        {
            get { return _resultBuilder; }
        }

        /// <summary>
        /// Gets the parameters.
        /// </summary>
        public List<object> Parameters
        {
            get { return _parameters; }
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        protected override Expression VisitUnary(UnaryExpression expression)
        {
            _resultBuilder.Append("(");

            switch (expression.NodeType)
            {
                case ExpressionType.Negate:
                    _resultBuilder.Append("-");
                    break;
                case ExpressionType.Not:
                    _resultBuilder.Append("not ");
                    break;
                case ExpressionType.Convert:
                    // Ignore, let the db do the conversion
                    break;
                default:
                    return base.VisitUnary(expression);
            }

            Visit(expression.Operand);

            _resultBuilder.Append(")");

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
                _resultBuilder.Append("concat(");
            else if (expression.NodeType == ExpressionType.Coalesce)
                _resultBuilder.Append("coalesce(");
            else
                return false;

            Visit(expression.Left);
            _resultBuilder.Append(", ");
            Visit(expression.Right);
            _resultBuilder.Append(")");

            return true;
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        protected override Expression VisitBinary(BinaryExpression expression)
        {
            // Either func or operator
            if (VisitBinaryFunc(expression))
                return expression;

            _resultBuilder.Append("(");

            Visit(expression.Left);

            switch (expression.NodeType)
            {
                case ExpressionType.Equal:
                {
                    var rightConst = expression.Right as ConstantExpression;

                    if (rightConst != null && rightConst.Value == null)
                    {
                        // Special case for nulls, since "= null" does not work in SQL
                        _resultBuilder.Append(" is null)");
                        return expression;
                    }

                    _resultBuilder.Append(" = ");
                    break;
                }

                case ExpressionType.NotEqual:
                {
                    var rightConst = expression.Right as ConstantExpression;

                    if (rightConst != null && rightConst.Value == null)
                    {
                        // Special case for nulls, since "<> null" does not work in SQL
                        _resultBuilder.Append(" is not null)");
                        return expression;
                    }

                    _resultBuilder.Append(" <> ");
                    break;
                }

                case ExpressionType.AndAlso:
                case ExpressionType.And:
                    _resultBuilder.Append(" and ");
                    break;

                case ExpressionType.OrElse:
                case ExpressionType.Or:
                    _resultBuilder.Append(" or ");
                    break;

                case ExpressionType.Add:
                    _resultBuilder.Append(" + ");
                    break;

                case ExpressionType.Subtract:
                    _resultBuilder.Append(" - ");
                    break;

                case ExpressionType.Multiply:
                    _resultBuilder.Append(" * ");
                    break;

                case ExpressionType.Modulo:
                    _resultBuilder.Append(" % ");
                    break;

                case ExpressionType.Divide:
                    _resultBuilder.Append(" / ");
                    break;

                case ExpressionType.GreaterThan:
                    _resultBuilder.Append(" > ");
                    break;

                case ExpressionType.GreaterThanOrEqual:
                    _resultBuilder.Append(" >= ");
                    break;

                case ExpressionType.LessThan:
                    _resultBuilder.Append(" < ");
                    break;

                case ExpressionType.LessThanOrEqual:
                    _resultBuilder.Append(" <= ");
                    break;

                case ExpressionType.Coalesce:
                    break;

                default:
                    base.VisitBinary(expression);
                    break;
            }

            Visit(expression.Right);
            _resultBuilder.Append(")");

            return expression;
        }

        /** <inheritdoc /> */
        protected override Expression VisitQuerySourceReference(QuerySourceReferenceExpression expression)
        {
            // Count, sum, max, min expect a single field or *
            // In other cases we need both parts of cache entry
            var format = _useStar ? "{0}.*" : "{0}._key, {0}._val";

            var tableName = _aliases.GetAlias(TableNameMapper.GetTableNameWithSchema(expression));

            _resultBuilder.AppendFormat(format, tableName);

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
                _resultBuilder.Append("length(");

                VisitMember((MemberExpression) expression.Expression);

                _resultBuilder.Append(")");

                return expression;
            }

            var queryFieldAttr = expression.Member.GetCustomAttributes(true)
                .OfType<QuerySqlFieldAttribute>().FirstOrDefault();

            var fieldName = queryFieldAttr == null || string.IsNullOrEmpty(queryFieldAttr.Name)
                ? expression.Member.Name
                : queryFieldAttr.Name;

            _resultBuilder.AppendFormat("{0}.{1}",
                _aliases.GetAlias(TableNameMapper.GetTableNameWithSchema(expression)), fieldName);

            return expression;
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        protected override Expression VisitConstant(ConstantExpression expression)
        {
            _resultBuilder.Append("?");

            _parameters.Add(expression.Value);

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
            _resultBuilder.Append("casewhen(");

            Visit(expression.Test);

            // Explicit type specification is required when all arguments of CASEWHEN are parameters
            _resultBuilder.Append(", cast(");
            Visit(expression.IfTrue);
            _resultBuilder.AppendFormat(" as {0}), ", SqlTypes.GetSqlTypeName(expression.Type) ?? "other");

            Visit(expression.IfFalse);
            _resultBuilder.Append(")");

            return expression;
        }

        /** <inheritdoc /> */
        protected override Expression VisitSubQuery(SubQueryExpression expression)
        {
            // This happens when New expression uses a subquery
            // TODO
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

                    _resultBuilder.Append(", ");
                }

                first = false;

                Visit(e);
            }
        }
    }
}
