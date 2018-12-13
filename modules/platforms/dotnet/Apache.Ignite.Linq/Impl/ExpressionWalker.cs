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
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Apache.Ignite.Core.Impl.Common;
    using Remotion.Linq.Clauses;
    using Remotion.Linq.Clauses.Expressions;

    /// <summary>
    /// Walks expression trees to extract query and table name info.
    /// </summary>
    internal static class ExpressionWalker
    {
        /** SQL quote */
        private const string SqlQuote = "\"";

        /** Compiled member readers. */
        private static readonly CopyOnWriteConcurrentDictionary<MemberInfo, Func<object, object>> MemberReaders =
            new CopyOnWriteConcurrentDictionary<MemberInfo, Func<object, object>>();

        /// <summary>
        /// Gets the cache queryable.
        /// </summary>
        public static ICacheQueryableInternal GetCacheQueryable(IFromClause fromClause, bool throwWhenNotFound = true)
        {
            return GetCacheQueryable(fromClause.FromExpression, throwWhenNotFound);
        }

        /// <summary>
        /// Gets the cache queryable.
        /// </summary>
        public static ICacheQueryableInternal GetCacheQueryable(JoinClause joinClause, bool throwWhenNotFound = true)
        {
            return GetCacheQueryable(joinClause.InnerSequence, throwWhenNotFound);
        }

        /// <summary>
        /// Gets the cache queryable.
        /// </summary>
        public static ICacheQueryableInternal GetCacheQueryable(Expression expression, bool throwWhenNotFound = true)
        {
            var subQueryExp = expression as SubQueryExpression;

            if (subQueryExp != null)
                return GetCacheQueryable(subQueryExp.QueryModel.MainFromClause, throwWhenNotFound);

            var srcRefExp = expression as QuerySourceReferenceExpression;

            if (srcRefExp != null)
            {
                var fromSource = srcRefExp.ReferencedQuerySource as IFromClause;

                if (fromSource != null)
                    return GetCacheQueryable(fromSource, throwWhenNotFound);

                var joinSource = srcRefExp.ReferencedQuerySource as JoinClause;

                if (joinSource != null)
                    return GetCacheQueryable(joinSource, throwWhenNotFound);

                throw new NotSupportedException("Unexpected query source: " + srcRefExp.ReferencedQuerySource);
            }

            var memberExpr = expression as MemberExpression;

            if (memberExpr != null)
            {
                if (memberExpr.Type.IsGenericType &&
                    memberExpr.Type.GetGenericTypeDefinition() == typeof (IQueryable<>))
                    return EvaluateExpression<ICacheQueryableInternal>(memberExpr);

                return GetCacheQueryable(memberExpr.Expression, throwWhenNotFound);
            }

            var constExpr = expression as ConstantExpression;

            if (constExpr != null)
            {
                var queryable = constExpr.Value as ICacheQueryableInternal;

                if (queryable != null)
                    return queryable;
            }

            var callExpr = expression as MethodCallExpression;

            if (callExpr != null)
            {
                // This is usually a nested query with a call to AsCacheQueryable().
                return (ICacheQueryableInternal) Expression.Lambda(callExpr).Compile().DynamicInvoke();
            }

            if (throwWhenNotFound)
                throw new NotSupportedException("Unexpected query source: " + expression);

            return null;
        }

        /// <summary>
        /// Tries to find QuerySourceReferenceExpression
        /// </summary>
        public static QuerySourceReferenceExpression GetQuerySourceReference(Expression expression,
            // ReSharper disable once ParameterOnlyUsedForPreconditionCheck.Global
            bool throwWhenNotFound = true)
        {
            var reference = expression as QuerySourceReferenceExpression;
            if (reference != null)
            {
                return reference;
            }

            var unary = expression as UnaryExpression;
            if (unary != null)
            {
                return GetQuerySourceReference(unary.Operand, false);
            }

            var binary = expression as BinaryExpression;
            if (binary != null)
            {
                return GetQuerySourceReference(binary.Left, false) ?? GetQuerySourceReference(binary.Right, false);
            }

            if (throwWhenNotFound)
            {
                throw new NotSupportedException("Unexpected query source: " + expression);
            }

            return null;
        }

        /// <summary>
        /// Evaluates the expression.
        /// </summary>
        public static T EvaluateExpression<T>(Expression expr)
        {
            var constExpr = expr as ConstantExpression;

            if (constExpr != null)
                return (T)constExpr.Value;

            var memberExpr = expr as MemberExpression;

            if (memberExpr != null)
            {
                var targetExpr = memberExpr.Expression as ConstantExpression;

                if (memberExpr.Expression == null || targetExpr != null)
                {
                    // Instance or static member
                    var target = targetExpr == null ? null : targetExpr.Value;

                    Func<object, object> reader;
                    if (MemberReaders.TryGetValue(memberExpr.Member, out reader))
                        return (T) reader(target);

                    return (T) MemberReaders.GetOrAdd(memberExpr.Member, x => CompileMemberReader(memberExpr))(target);
                }
            }

            // Case for compiled queries: return unchanged.
            // ReSharper disable once CanBeReplacedWithTryCastAndCheckForNull
            if (expr is ParameterExpression)
                return (T) (object) expr;

            throw new NotSupportedException("Expression not supported: " + expr);
        }

        /// <summary>
        /// Gets the values from IEnumerable expression
        /// </summary>
        public static IEnumerable<object> EvaluateEnumerableValues(Expression fromExpression)
        {
            IEnumerable result;
            switch (fromExpression.NodeType)
            {
                case ExpressionType.MemberAccess:
                    var memberExpression = (MemberExpression)fromExpression;
                    result = EvaluateExpression<IEnumerable>(memberExpression);
                    break;
                case ExpressionType.ListInit:
                    var listInitExpression = (ListInitExpression)fromExpression;
                    result = listInitExpression.Initializers
                        .SelectMany(init => init.Arguments)
                        .Select(EvaluateExpression<object>);
                    break;
                case ExpressionType.NewArrayInit:
                    var newArrayExpression = (NewArrayExpression)fromExpression;
                    result = newArrayExpression.Expressions
                        .Select(EvaluateExpression<object>);
                    break;
                case ExpressionType.Parameter:
                    // This should happen only when 'IEnumerable.Contains' is called on parameter of compiled query
                    throw new NotSupportedException("'Contains' clause on compiled query parameter is not supported.");
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
        /// Compiles the member reader.
        /// </summary>
        private static Func<object, object> CompileMemberReader(MemberExpression memberExpr)
        {
            // Field or property
            var fld = memberExpr.Member as FieldInfo;

            if (fld != null)
                return DelegateConverter.CompileFieldGetter(fld);

            var prop = memberExpr.Member as PropertyInfo;

            if (prop != null)
                return DelegateConverter.CompilePropertyGetter(prop);

            throw new NotSupportedException("Expression not supported: " + memberExpr);
        }

        /// <summary>
        /// Gets the table name with schema.
        /// </summary>
        public static string GetTableNameWithSchema(ICacheQueryableInternal queryable)
        {
            Debug.Assert(queryable != null);

            var cacheCfg = queryable.CacheConfiguration;

            var tableName = queryable.TableName;
            if (cacheCfg.SqlEscapeAll)
            {
                tableName = string.Format("{0}{1}{0}", SqlQuote, tableName);
            }

            var schemaName = NormalizeSchemaName(cacheCfg.Name, cacheCfg.SqlSchema);

            return string.Format("{0}.{1}", schemaName, tableName);
        }

        /// <summary>
        /// Normalizes SQL schema name, see
        /// <c>org.apache.ignite.internal.processors.query.QueryUtils#normalizeSchemaName</c>
        /// </summary>
        private static string NormalizeSchemaName(string cacheName, string schemaName)
        {
            if (schemaName == null)
            {
                // If schema name is not set explicitly, we will use escaped cache name. The reason is that cache name
                // could contain weird characters, such as underscores, dots or non-Latin stuff, which are invalid from
                // SQL syntax perspective. We do not want node to fail on startup due to this.
                return string.Format("{0}{1}{0}", SqlQuote, cacheName);
            }

            if (schemaName.StartsWith(SqlQuote, StringComparison.Ordinal)
                && schemaName.EndsWith(SqlQuote, StringComparison.Ordinal))
            {
                return schemaName;
            }

            return NormalizeObjectName(schemaName, false);
        }

        /// <summary>
        /// Normalizes SQL object name, see
        /// <c>org.apache.ignite.internal.processors.query.QueryUtils#normalizeObjectName</c>
        /// </summary>
        private static string NormalizeObjectName(string name, bool replace)
        {
            if (string.IsNullOrEmpty(name))
            {
                return name;
            }

            if (replace)
            {
                name = name.Replace('.', '_').Replace('$', '_');
            }

            return name.ToUpperInvariant();
        }
    }
}
