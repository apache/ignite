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
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Apache.Ignite.Core.Cache.Configuration;
    using Remotion.Linq.Clauses.Expressions;
    using Remotion.Linq.Parsing;

    /// <summary>
    /// Expression visitor, transforms query subexpressions (such as Where clauses) to SQL.
    /// </summary>
    internal class CacheQueryExpressionVisitor : ThrowingExpressionVisitor
    {
        /** */
        private static readonly MethodInfo StringContains = typeof (string).GetMethod("Contains");

        /** */
        private static readonly MethodInfo StringStartsWith = typeof (string).GetMethod("StartsWith",
            new[] {typeof (string)});

        /** */
        private static readonly MethodInfo StringEndsWith = typeof (string).GetMethod("EndsWith",
            new[] {typeof (string)});

        /** */
        private static readonly MethodInfo StringToLower = typeof (string).GetMethod("ToLower", new Type[0]);

        /** */
        private static readonly MethodInfo StringToUpper = typeof (string).GetMethod("ToUpper", new Type[0]);

        /** */
        private readonly bool _aggregating;

        /** */
        private readonly string _schemaName;

        /** */
        private readonly StringBuilder _resultBuilder = new StringBuilder();

        /** */
        private readonly List<object> _parameters = new List<object>();

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheQueryExpressionVisitor"/> class.
        /// </summary>
        private CacheQueryExpressionVisitor(bool aggregating, string schemaName)
        {
            Debug.Assert(!string.IsNullOrEmpty(schemaName));

            _aggregating = aggregating;
            _schemaName = schemaName;
        }

        /// <summary>
        /// Gets the SQL statement.
        /// </summary>
        /// <param name="linqExpression">The linq expression.</param>
        /// <param name="aggregating"></param>
        /// <returns>
        /// SQL statement for the expression.
        /// </returns>
        public static QueryData GetSqlExpression(Expression linqExpression, bool aggregating, string schemaName)
        {
            var visitor = new CacheQueryExpressionVisitor(aggregating, schemaName);

            visitor.Visit(linqExpression);

            return visitor.GetSqlExpression();
        }

        /// <summary>
        /// Gets the SQL expression.
        /// </summary>
        private QueryData GetSqlExpression()
        {
            return new QueryData(_resultBuilder.ToString(), _parameters);
        }

        /** <inheritdoc /> */
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
                default:
                    return base.VisitUnary(expression);
            }

            Visit(expression.Operand);

            _resultBuilder.Append(")");

            return expression;
        }

        /** <inheritdoc /> */
        protected override Expression VisitBinary(BinaryExpression expression)
        {
            _resultBuilder.Append("(");

            Visit(expression.Left);

            switch (expression.NodeType)
            {
                case ExpressionType.Equal:
                    _resultBuilder.Append(" = ");
                    break;

                case ExpressionType.NotEqual:
                    _resultBuilder.Append(" <> ");
                    break;

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
            var format = _aggregating ? "{0}.*" : "{0}._key, {0}._val";

            var tableName = TableNameMapper.GetTableName(expression);

            _resultBuilder.AppendFormat(format, tableName);

            return expression;
        }

        /** <inheritdoc /> */
        protected override Expression VisitMember(MemberExpression expression)
        {
            // Field hierarchy is flattened, append as is, do not call Visit.
            // TODO: Aliases? How do they work? See email.

            var queryFieldAttr = expression.Member.GetCustomAttributes(true).OfType<QuerySqlFieldAttribute>().FirstOrDefault();

            var fieldName = queryFieldAttr == null || string.IsNullOrEmpty(queryFieldAttr.Name) ? expression.Member.Name : queryFieldAttr.Name;

            _resultBuilder.AppendFormat("\"{0}\"{1}.{2}", _schemaName, TableNameMapper.GetTableName(expression),
                fieldName);

            return expression;
        }

        /** <inheritdoc /> */
        protected override Expression VisitConstant(ConstantExpression expression)
        {
            _resultBuilder.Append("?");

            _parameters.Add(expression.Value);

            return expression;
        }

        /** <inheritdoc /> */
        protected override Expression VisitMethodCall(MethodCallExpression expression)
        {
            if (expression.Method == StringContains)
                return VisitSqlLike(expression, "%{0}%");

            if (expression.Method == StringStartsWith)
                return VisitSqlLike(expression, "{0}%");

            if (expression.Method == StringEndsWith)
                return VisitSqlLike(expression, "%{0}");

            if (expression.Method == StringToLower)
            {
                _resultBuilder.Append("lower(");
                Visit(expression.Object);
                _resultBuilder.Append(")");

                return expression;
            }

            if (expression.Method == StringToUpper)
            {
                _resultBuilder.Append("upper(");
                Visit(expression.Object);
                _resultBuilder.Append(")");

                return expression;
            }

            // Attempt to postpone method call until query result materialization
            VisitArguments(expression.Arguments);

            return expression;
        }

        /// <summary>
        /// Visits the SQL like expression.
        /// </summary>
        private Expression VisitSqlLike(MethodCallExpression expression, string likeFormat)
        {
            _resultBuilder.Append("(");

            Visit(expression.Object);

            _resultBuilder.Append(" like ?) ");

            _parameters.Add(string.Format(likeFormat, GetConstantValue(expression)));

            return expression;
        }

        /// <summary>
        /// Gets the single constant value.
        /// </summary>
        private static object GetConstantValue(MethodCallExpression expression)
        {
            var arg = expression.Arguments[0] as ConstantExpression;

            if (arg == null)
                throw new NotSupportedException("Only constant expression is supported inside Contains call: " + expression);

            return arg.Value;
        }

        /** <inheritdoc /> */
        protected override Expression VisitNew(NewExpression expression)
        {
            VisitArguments(expression.Arguments);

            return expression;
        }

        /** <inheritdoc /> */
        protected override Expression VisitInvocation(InvocationExpression expression)
        {
            VisitArguments(expression.Arguments);

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
                    if (_aggregating)
                        throw new NotSupportedException("Aggregate functions do not support multiple fields");

                    _resultBuilder.Append(", ");
                }

                first = false;

                Visit(e);
            }
        }
    }
}
