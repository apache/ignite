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
    using System.Linq.Expressions;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Remotion.Linq.Clauses.Expressions;
    using Remotion.Linq.Parsing;

    /// <summary>
    /// Expression visitor, transforms query subexpressions (such as Where clauses) to SQL.
    /// </summary>
    internal static class CacheQueryExpressionVisitor
    {
        /// <summary>
        /// Gets the SQL statement.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="cache">The cache.</param>
        /// <param name="linqExpression">The linq expression.</param>
        /// <returns>SQL statement for the expression.</returns>
        public static QueryData GetSqlExpression<TKey, TValue>(ICache<TKey, TValue> cache, Expression linqExpression)
        {
            var visitor = new CacheQueryExpressionVisitor<TKey, TValue>(cache);

            visitor.Visit(linqExpression);

            return visitor.GetSqlExpression();
        }
    }

    /// <summary>
    /// Expression visitor, transforms query subexpressions (such as Where clauses) to SQL.
    /// </summary>
    internal class CacheQueryExpressionVisitor<TKey, TValue> : ThrowingExpressionVisitor
    {
        /** */
        private readonly ICache<TKey, TValue> _cache;

        /** */
        private readonly StringBuilder _resultBuilder = new StringBuilder();

        /** */
        private readonly List<object> _parameters = new List<object>();

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheQueryExpressionVisitor{TKey, TValue}"/> class.
        /// </summary>
        /// <param name="cache">The cache.</param>
        public CacheQueryExpressionVisitor(ICache<TKey, TValue> cache)
        {
            Debug.Assert(cache != null);

            _cache = cache;
        }

        /// <summary>
        /// Gets the SQL expression.
        /// </summary>
        public QueryData GetSqlExpression()
        {
            return new QueryData(_resultBuilder.ToString(), _parameters);
        }

        /** <inheritdoc /> */
        protected override Expression VisitQuerySourceReference(QuerySourceReferenceExpression expression)
        {
            // TODO: Key/Value direct queries?
            _resultBuilder.Append(expression.ReferencedQuerySource.ItemName);
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
        protected override Expression VisitMember(MemberExpression expression)
        {
            // Field hierarchy is flattened, append as is, do not call Visit.
            // TODO: Look up QueryFieldAttribute
            var fieldName = expression.Member.Name;

            _resultBuilder.Append(fieldName);

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
            // TODO: Other methods - StartsWith, ToLower, ToUpper, ToLowerInvariant, ToUpperInvariant
            var supportedMethod = typeof(string).GetMethod("Contains");
            if (expression.Method.Equals(supportedMethod))
            {
                _resultBuilder.Append("(");
                Visit(expression.Object);
                _resultBuilder.Append(" like '%' + ");
                Visit(expression.Arguments[0]);
                _resultBuilder.Append(" + '%')");
                return expression;
            }

            return base.VisitMethodCall(expression); // throws
        }

        /** <inheritdoc /> */
        protected override Exception CreateUnhandledItemException<T>(T unhandledItem, string visitMethod)
        {
            return new NotSupportedException(
                string.Format("The expression '{0}' (type: {1}) is not supported by this LINQ provider.",
                    unhandledItem, typeof (T)));
        }
    }
}
