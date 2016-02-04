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

        private static readonly Dictionary<MethodInfo, VisitMethodDelegate> Delegates = new List
            <KeyValuePair<MethodInfo, VisitMethodDelegate>>
        {
            GetStringMethod("ToLower", new Type[0], GetFunc("lower")),
            GetStringMethod("ToUpper", new Type[0], GetFunc("upper")),
            GetStringMethod("Contains", del: (e, v) => VisitSqlLike(e, v, "%{0}%")),
            GetStringMethod("StartsWith", new[] {typeof (string)}, (e, v) => VisitSqlLike(e, v, "{0}%")),
            GetStringMethod("EndsWith", new[] {typeof (string)}, (e, v) => VisitSqlLike(e, v, "{0}%")),
            GetStringMethod("IndexOf", new[] {typeof (string)}, GetFunc("instr", -1)),
            GetStringMethod("IndexOf", new[] {typeof (string), typeof (int)}, GetFunc("instr", -1)),
            GetStringMethod("Substring", new[] {typeof (int)}, GetFunc("substring", 0, 1)),
            GetStringMethod("Substring", new[] {typeof (int), typeof (int)}, GetFunc("substring", 0, 1)),
            GetStringMethod("Trim", "trim"),
            GetStringMethod("Trim", "trim", typeof(char[])),
            GetStringMethod("TrimStart", "ltrim", typeof(char[])),
            GetStringMethod("TrimEnd", "rtrim", typeof(char[])),

            GetMethod(typeof (DateTime), "ToString", new[] {typeof (string)}, GetFunc("formatdatetime")),

            GetMathMethod("Abs", typeof (int)),
            GetMathMethod("Abs", typeof (long)),
            GetMathMethod("Abs", typeof (float)),
            GetMathMethod("Abs", typeof (double)),
            GetMathMethod("Abs", typeof (decimal)),
            GetMathMethod("Abs", typeof (sbyte)),
            GetMathMethod("Abs", typeof (short)),
            GetMathMethod("Acos", typeof (double)),
            GetMathMethod("Asin", typeof (double)),
            GetMathMethod("Atan", typeof (double)),
            GetMathMethod("Atan2", typeof (double), typeof (double)),
            GetMathMethod("Ceiling", typeof (double)),
            GetMathMethod("Ceiling", typeof (decimal)),
            GetMathMethod("Cos", typeof (double)),
            GetMathMethod("Cosh", typeof (double)),
            GetMathMethod("Exp", typeof (double)),
            GetMathMethod("Floor", typeof (double)),
            GetMathMethod("Floor", typeof (decimal)),
            GetMathMethod("Log", typeof (double)),
            GetMathMethod("Log10", typeof (double)),
            GetMathMethod("Pow", "Power", typeof (double), typeof (double)),
            GetMathMethod("Round", typeof (double)),
            GetMathMethod("Round", typeof (double), typeof (int)),
            GetMathMethod("Round", typeof (decimal)),
            GetMathMethod("Round", typeof (decimal), typeof (int)),
            GetMathMethod("Sign", typeof (double)),
            GetMathMethod("Sign", typeof (decimal)),
            GetMathMethod("Sign", typeof (float)),
            GetMathMethod("Sign", typeof (int)),
            GetMathMethod("Sign", typeof (long)),
            GetMathMethod("Sign", typeof (short)),
            GetMathMethod("Sign", typeof (sbyte)),
            GetMathMethod("Sin", typeof (double)),
            GetMathMethod("Sinh", typeof (double)),
            GetMathMethod("Sqrt", typeof (double)),
            GetMathMethod("Tan", typeof (double)),
            GetMathMethod("Tanh", typeof (double)),
            GetMathMethod("Truncate", typeof (double)),
            GetMathMethod("Truncate", typeof (decimal)),
        }.ToDictionary(x => x.Key, x => x.Value);

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
        private static VisitMethodDelegate GetFunc(string func, params int[] adjust)
        {
            return (e, v) => VisitFunc(e, v, func, adjust);
        }

        /// <summary>
        /// Visits the instance function.
        /// </summary>
        private static void VisitFunc(MethodCallExpression expression, CacheQueryExpressionVisitor visitor, 
            string func, params int[] adjust)
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

                AppendAdjustment(visitor, adjust, i + 1);
            }

            visitor.ResultBuilder.Append(")");

            AppendAdjustment(visitor, adjust, 0);
        }

        private static void AppendAdjustment(CacheQueryExpressionVisitor visitor, int[] adjust, int idx)
        {
            if (idx < adjust.Length)
            {
                var delta = adjust[idx];

                if (delta > 0)
                    visitor.ResultBuilder.AppendFormat(" + {0}", delta);
                else if (delta < 0)
                    visitor.ResultBuilder.AppendFormat(" {0}", delta);
            }
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

        private static KeyValuePair<MethodInfo, VisitMethodDelegate> GetStringMethod(string name, string sqlName,
            params Type[] argTypes)
        {
            return GetMethod(typeof(string), name, argTypes, GetFunc(sqlName));
        }

        private static KeyValuePair<MethodInfo, VisitMethodDelegate> GetMathMethod(string name, string sqlName,
            params Type[] argTypes)
        {
            return GetMethod(typeof(Math), name, argTypes, GetFunc(sqlName));
        }

        private static KeyValuePair<MethodInfo, VisitMethodDelegate> GetMathMethod(string name, params Type[] argTypes)
        {
            return GetMathMethod(name, name, argTypes);
        }
    }
}