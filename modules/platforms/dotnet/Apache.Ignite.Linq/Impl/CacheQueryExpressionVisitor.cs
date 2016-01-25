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
    using System.Diagnostics;
    using System.Linq.Expressions;
    using Apache.Ignite.Core.Cache;
    using Remotion.Linq.Clauses.Expressions;
    using Remotion.Linq.Parsing;

    internal class CacheQueryExpressionVisitor
    {
        public static string GetSqlStatement<TKey, TValue>(ICache<TKey, TValue> cache, Expression linqExpression)
        {
            var visitor = new CacheQueryExpressionVisitor<TKey, TValue>(cache);

            visitor.Visit(linqExpression);

            return visitor.GetSql();
        }


    }

    /// <summary>
    /// Expression visitor, transforms query subexpressions (such as Where clauses) to SQL.
    /// </summary>
    internal class CacheQueryExpressionVisitor<TKey, TValue> : ThrowingExpressionVisitor
    {
        private readonly ICache<TKey, TValue> _cache;

        private readonly StringBuilder _resultBuilder = new StringBuilder();

        public CacheQueryExpressionVisitor(ICache<TKey, TValue> cache)
        {
            Debug.Assert(cache != null);

            _cache = cache;
        }

        public string GetSql()
        {
            return _resultBuilder.ToString();
        }

        protected override Expression VisitQuerySourceReference(QuerySourceReferenceExpression expression)
        {
            _resultBuilder.Append(expression.ReferencedQuerySource.ItemName);
            return expression;
        }

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

        protected override Expression VisitMember(MemberExpression expression)
        {
            /*if (expression.Member.DeclaringType != typeof (ICacheEntry<TKey, TValue>))
            {
                Visit(expression.Expression);

                _resultBuilder.AppendFormat(".{0}", expression.Member.Name);
            }*/

            // Field hierarchy is flattened, append as is, do not call Visit.
            _resultBuilder.Append(expression.Member.Name);

            return expression;
        }

        protected override Expression VisitConstant(ConstantExpression expression)
        {
            // TODO: Maintain parameter order somehow

            _resultBuilder.Append("?");

            return expression;
        }

        protected override Expression VisitMethodCall(MethodCallExpression expression)
        {
            // TODO: Other methods
            var supportedMethod = typeof(string).GetMethod("Contains");
            if (expression.Method.Equals(supportedMethod))
            {
                _resultBuilder.Append("(");
                Visit(expression.Object);
                _resultBuilder.Append(" like '%'+");
                Visit(expression.Arguments[0]);
                _resultBuilder.Append("+'%')");
                return expression;
            }

            return base.VisitMethodCall(expression); // throws
        }

        // Called when a LINQ expression type is not handled above.
        protected override Exception CreateUnhandledItemException<T>(T unhandledItem, string visitMethod)
        {
            return new NotSupportedException(
                string.Format("The expression '{0}' (type: {1}) is not supported by this LINQ provider.",
                    unhandledItem, typeof (T)));
        }
    }
}
