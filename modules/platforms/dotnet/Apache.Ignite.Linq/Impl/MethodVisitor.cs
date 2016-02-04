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
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;

    /// <summary>
    /// MethodCall expression visitor.
    /// </summary>
    internal static class MethodVisitor
    {
        public static readonly MemberInfo StringLength = typeof (string).GetProperty("Length");

        private delegate void VisitMethodDelegate(MethodCallExpression expression, CacheQueryExpressionVisitor visitor);

        private static readonly Dictionary<MethodInfo, VisitMethodDelegate> Delegates1 = new List
            <KeyValuePair<MethodInfo, VisitMethodDelegate>>
        {
            GetStringMethod("ToLower", del:GetFunc("lower")),
            GetStringMethod("ToUpper", del:GetFunc("upper")),
            GetStringMethod("Contains", del:(e, v) => VisitSqlLike(e, v, "%{0}%")),
            GetStringMethod("StartsWith", new[] {typeof (string)}, (e, v) => VisitSqlLike(e, v, "{0}%")),
            GetStringMethod("EndsWith", new[] {typeof (string)}, (e, v) => VisitSqlLike(e, v, "{0}%")),
            GetMethod(typeof(DateTime), "ToString", new [] {typeof(string)}, GetFunc("formatdatetime"))
        }.ToDictionary(x => x.Key, x => x.Value);


        private static readonly Dictionary<MethodInfo, VisitMethodDelegate> Delegates = new Dictionary
            <MethodInfo, VisitMethodDelegate>
        {
            {typeof (string).GetMethod("ToLower", new Type[0]), GetFunc("lower")},
            {typeof (string).GetMethod("ToUpper", new Type[0]), GetFunc("upper")},
            {typeof (string).GetMethod("Contains"), (e, v) => VisitSqlLike(e, v, "%{0}%")},
            {typeof (string).GetMethod("StartsWith", new[] {typeof (string)}), (e, v) => VisitSqlLike(e, v, "{0}%")},
            {typeof (string).GetMethod("EndsWith", new[] {typeof (string)}), (e, v) => VisitSqlLike(e, v, "%{0}")},
            {typeof (DateTime).GetMethod("ToString", new[] {typeof (string)}), GetFunc("formatdatetime")},
            {GetMathMethod("Abs", typeof (int)), GetFunc("abs")},
            {GetMathMethod("Abs", typeof (long)), GetFunc("abs")},
            {GetMathMethod("Abs", typeof (float)), GetFunc("abs")},
            {GetMathMethod("Abs", typeof (double)), GetFunc("abs")},
            {GetMathMethod("Abs", typeof (decimal)), GetFunc("abs")},
            {GetMathMethod("Abs", typeof (sbyte)), GetFunc("abs")},
            {GetMathMethod("Abs", typeof (short)), GetFunc("abs")},
            {GetMathMethod("Acos", typeof(double)), GetFunc("acos")},
            {GetMathMethod("Asin", typeof(double)), GetFunc("asin")},
            {GetMathMethod("Atan", typeof(double)), GetFunc("atan")},
            {GetMathMethod("Atan2", typeof(double), typeof(double)), GetFunc("atan2")},
            {GetMathMethod("Ceiling", typeof(double)), GetFunc("ceiling")},
            {GetMathMethod("Ceiling", typeof(decimal)), GetFunc("ceiling")},
            {GetMathMethod("Cos", typeof(double)), GetFunc("cos")},
            {GetMathMethod("Cosh", typeof(double)), GetFunc("cosh")},
            {GetMathMethod("Exp", typeof(double)), GetFunc("exp")},
            {GetMathMethod("Floor", typeof(double)), GetFunc("floor")},
            {GetMathMethod("Floor", typeof(decimal)), GetFunc("floor")},
            {GetMathMethod("Log", typeof(double)), GetFunc("log")},
            {GetMathMethod("Log10", typeof(double)), GetFunc("log10")},
            {GetMathMethod("Pow", typeof(double), typeof(double)), GetFunc("power")},
            {GetMathMethod("Round", typeof(double)), GetFunc("round")},
            {GetMathMethod("Round", typeof(double), typeof(int)), GetFunc("round")},
            {GetMathMethod("Round", typeof(decimal)), GetFunc("round")},
            {GetMathMethod("Round", typeof(decimal), typeof(int)), GetFunc("round")},
            {GetMathMethod("Sign", typeof(double)), GetFunc("sign")},
            {GetMathMethod("Sign", typeof(decimal)), GetFunc("sign")},
            {GetMathMethod("Sign", typeof(float)), GetFunc("sign")},
            {GetMathMethod("Sign", typeof(int)), GetFunc("sign")},
            {GetMathMethod("Sign", typeof(long)), GetFunc("sign")},
            {GetMathMethod("Sign", typeof(short)), GetFunc("sign")},
            {GetMathMethod("Sign", typeof(sbyte)), GetFunc("sign")},
            {GetMathMethod("Sin", typeof(double)), GetFunc("sin")},
            {GetMathMethod("Sinh", typeof(double)), GetFunc("sinh")},
            {GetMathMethod("Sqrt", typeof(double)), GetFunc("sqrt")},
            {GetMathMethod("Tan", typeof(double)), GetFunc("tan")},
            {GetMathMethod("Tanh", typeof(double)), GetFunc("tanh")},
            {GetMathMethod("Truncate", typeof(double)), GetFunc("truncate")},
            {GetMathMethod("Truncate", typeof(decimal)), GetFunc("truncate")},
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

        private static KeyValuePair<MethodInfo, VisitMethodDelegate> GetStringMethod(string name,
            Type[] argTypes = null, VisitMethodDelegate del = null)
        {
            return GetMethod(typeof(string), name, argTypes, del);
        }

        private static KeyValuePair<MethodInfo, VisitMethodDelegate> GetMethod(Type type, string name,
            Type[] argTypes = null, VisitMethodDelegate del = null)
        {
            var method = argTypes == null ? type.GetMethod(name) : type.GetMethod(name, argTypes);

            if (method == null)
                throw new InvalidOperationException("Method not found: " + name);

            return new KeyValuePair<MethodInfo, VisitMethodDelegate>(method, del ?? GetFunc(name));
        }


        /// <summary>
        /// Gets the math method.
        /// </summary>
        private static MethodInfo GetMathMethod(string name, params Type[] argTypes)
        {
            var method = typeof(Math).GetMethod(name, argTypes);

            if (method == null)
                throw new InvalidOperationException("Math method not found: " + name);

            return method;
        }
    }
}