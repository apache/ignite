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
    using System.Linq.Expressions;
    using System.Reflection;

    /// <summary>
    /// MethodCall expression visitor.
    /// </summary>
    internal static class MethodVisitor
    {
        private delegate void VisitMethodDelegate(MethodCallExpression expression, CacheQueryExpressionVisitor visitor);

        private static readonly Dictionary<MethodInfo, VisitMethodDelegate> Delegates = new Dictionary
            <MethodInfo, VisitMethodDelegate>
        {
            {typeof (string).GetMethod("ToLower", new Type[0]), GetFunc("lower")},
            {typeof (string).GetMethod("ToUpper", new Type[0]), GetFunc("upper")},
            {typeof (string).GetMethod("Contains"), (e, v) => VisitSqlLike(e, v, "%{0}%")},
            {typeof (string).GetMethod("StartsWith", new[] {typeof (string)}), (e, v) => VisitSqlLike(e, v, "{0}%")},
            {typeof (string).GetMethod("EndsWith", new[] {typeof (string)}), (e, v) => VisitSqlLike(e, v, "%{0}")},
            {typeof (DateTime).GetMethod("ToString", new[] {typeof (string)}), GetFunc("formatdatetime")},
            {typeof (Math).GetMethod("Abs", new[] {typeof (int)}), GetFunc("abs")},
            {typeof (Math).GetMethod("Abs", new[] {typeof (long)}), GetFunc("abs")},
            {typeof (Math).GetMethod("Abs", new[] {typeof (float)}), GetFunc("abs")},
            {typeof (Math).GetMethod("Abs", new[] {typeof (double)}), GetFunc("abs")},
            {typeof (Math).GetMethod("Abs", new[] {typeof (decimal)}), GetFunc("abs")},
            {typeof (Math).GetMethod("Abs", new[] {typeof (sbyte)}), GetFunc("abs")},
            {typeof (Math).GetMethod("Abs", new[] {typeof (short)}), GetFunc("abs")},
        };

        /// <summary>
        /// Visits the method call expression.
        /// </summary>
        public static void VisitMethodCall(MethodCallExpression expression, CacheQueryExpressionVisitor visitor)
        {
            var method = expression.Method;

            VisitMethodDelegate del;

            if (!Delegates.TryGetValue(method, out del))
                throw new NotSupportedException(string.Format("Method not supported: {0}.({1})",
                    method.DeclaringType == null ? "static" : method.DeclaringType.FullName, method));

            del(expression, visitor);
        }

        /// <summary>
        /// Gets the function.
        /// </summary>
        private static VisitMethodDelegate GetFunc(string func)
        {
            return (e, v) => VisitFunc(e, v, func);
        }

        /// <summary>
        /// Visits the instance function.
        /// </summary>
        private static void VisitFunc(MethodCallExpression expression, CacheQueryExpressionVisitor visitor, string func)
        {
            visitor.ResultBuilder.Append(func).Append("(");

            var isInstanceMethod = expression.Object != null;

            if (isInstanceMethod)
                visitor.Visit(expression.Object);

            for (int i = 0; i < expression.Arguments.Count; i++)
            {
                var arg = expression.Arguments[i];

                if (isInstanceMethod || (i > 0))
                    visitor.ResultBuilder.Append(", ");

                visitor.Visit(arg);
            }

            visitor.ResultBuilder.Append(")");
        }

        /// <summary>
        /// Visits the SQL like expression.
        /// </summary>
        private static void VisitSqlLike(MethodCallExpression expression, CacheQueryExpressionVisitor visitor, string likeFormat)
        {
            visitor.ResultBuilder.Append("(");

            visitor.Visit(expression.Object);

            visitor.ResultBuilder.Append(" like ?) ");

            visitor.Parameters.Add(string.Format(likeFormat, GetConstantValue(expression)));
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
    }
}