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
    using System.Diagnostics;
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
            GetStringMethod("ToLower", new Type[0], del:GetFunc("lower")),
            GetStringMethod("ToUpper", new Type[0], del:GetFunc("upper")),
            GetStringMethod("Contains", del:(e, v) => VisitSqlLike(e, v, "%{0}%")),
            GetStringMethod("StartsWith", new[] {typeof (string)}, (e, v) => VisitSqlLike(e, v, "{0}%")),
            GetStringMethod("EndsWith", new[] {typeof (string)}, (e, v) => VisitSqlLike(e, v, "{0}%")),
            GetMethod(typeof(DateTime), "ToString", new [] {typeof(string)}, GetFunc("formatdatetime")),

            GetMathMethod1("Abs", typeof (int)),
            GetMathMethod1("Abs", typeof (long)),
            GetMathMethod1("Abs", typeof (float)),
            GetMathMethod1("Abs", typeof (double)),
            GetMathMethod1("Abs", typeof (decimal)),
            GetMathMethod1("Abs", typeof (sbyte)),
            GetMathMethod1("Abs", typeof (short)),
            GetMathMethod1("Acos", typeof(double)),
            GetMathMethod1("Asin", typeof(double)),
            GetMathMethod1("Atan", typeof(double)),
            GetMathMethod1("Atan2", typeof(double), typeof(double)),
            GetMathMethod1("Ceiling", typeof(double)),
            GetMathMethod1("Ceiling", typeof(decimal)),
            GetMathMethod1("Cos", typeof(double)),
            GetMathMethod1("Cosh", typeof(double)),
            GetMathMethod1("Exp", typeof(double)),
            GetMathMethod1("Floor", typeof(double)),
            GetMathMethod1("Floor", typeof(decimal)),
            GetMathMethod1("Log", typeof(double)),
            GetMathMethod1("Log10", typeof(double)),
            GetMathMethod1("Pow", "Power", typeof(double), typeof(double)),
            GetMathMethod1("Round", typeof(double)),
            GetMathMethod1("Round", typeof(double), typeof(int)),
            GetMathMethod1("Round", typeof(decimal)),
            GetMathMethod1("Round", typeof(decimal), typeof(int)),
            GetMathMethod1("Sign", typeof(double)),
            GetMathMethod1("Sign", typeof(decimal)),
            GetMathMethod1("Sign", typeof(float)),
            GetMathMethod1("Sign", typeof(int)),
            GetMathMethod1("Sign", typeof(long)),
            GetMathMethod1("Sign", typeof(short)),
            GetMathMethod1("Sign", typeof(sbyte)),
            GetMathMethod1("Sin", typeof(double)),
            GetMathMethod1("Sinh", typeof(double)),
            GetMathMethod1("Sqrt", typeof(double)),
            GetMathMethod1("Tan", typeof(double)),
            GetMathMethod1("Tanh", typeof(double)),
            GetMathMethod1("Truncate", typeof(double)),
            GetMathMethod1("Truncate", typeof(decimal)),
        }.ToDictionary(x => x.Key, x => x.Value);

        /// <summary>
        /// Visits the method call expression.
        /// </summary>
        public static void VisitMethodCall(MethodCallExpression expression, CacheQueryExpressionVisitor visitor)
        {
            var method = expression.Method;

            VisitMethodDelegate del;

            if (!Delegates1.TryGetValue(method, out del))
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

        private static KeyValuePair<MethodInfo, VisitMethodDelegate> GetMethod(Type type, string name,
            Type[] argTypes = null, VisitMethodDelegate del = null)
        {
            Debug.WriteLine("Looking up method: " + name);

            var method = argTypes == null ? type.GetMethod(name) : type.GetMethod(name, argTypes);

            if (method == null)
                Debug.WriteLine("Null method: " + name);

            return new KeyValuePair<MethodInfo, VisitMethodDelegate>(method, del ?? GetFunc(name));
        }

        private static KeyValuePair<MethodInfo, VisitMethodDelegate> GetStringMethod(string name,
            Type[] argTypes = null, VisitMethodDelegate del = null)
        {
            return GetMethod(typeof(string), name, argTypes, del);
        }

        private static KeyValuePair<MethodInfo, VisitMethodDelegate> GetMathMethod1(string name, string sqlName,
            params Type[] argTypes)
        {
            return GetMethod(typeof(Math), name, argTypes, GetFunc(sqlName));
        }

        private static KeyValuePair<MethodInfo, VisitMethodDelegate> GetMathMethod1(string name, params Type[] argTypes)
        {
            return GetMathMethod1(name, name, argTypes);
        }
    }
}