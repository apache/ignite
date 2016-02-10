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
    using System.Diagnostics;
    using System.Linq.Expressions;
    using Apache.Ignite.Core.Cache;
    using Remotion.Linq.Clauses;
    using Remotion.Linq.Clauses.Expressions;

    /// <summary>
    /// Table name mapper.
    /// </summary>
    internal static class ExpressionWalker
    {
        public static ICacheQueryable GetCacheQueryable(QuerySourceReferenceExpression expression)
        {
            Debug.Assert(expression != null);

            var fromSource = expression.ReferencedQuerySource as IFromClause; 

            if (fromSource != null)
                return GetCacheQueryable(fromSource);

            var joinSource = expression.ReferencedQuerySource as JoinClause;

            if (joinSource != null)
                return GetCacheQueryable(joinSource);

            throw new NotSupportedException("Unexpected query source: " + expression.ReferencedQuerySource);
        }

        public static ICacheQueryable GetCacheQueryable(MemberExpression expression)
        {
            Debug.Assert(expression != null);

            var querySrc = expression.Expression as QuerySourceReferenceExpression;

            if (querySrc != null)
                return GetCacheQueryable(querySrc);

            var innerMember = expression.Expression as MemberExpression;

            if (innerMember != null)
                return GetCacheQueryable(innerMember);

            throw new NotSupportedException("Unexpected member expression, cannot find query source: " + expression);
        }

        public static ICacheQueryable GetCacheQueryable(IFromClause fromClause)
        {
            return GetCacheQueryable(fromClause.FromExpression);
        }

        public static ICacheQueryable GetCacheQueryable(JoinClause joinClause)
        {
            return GetCacheQueryable(joinClause.InnerSequence);
        }

        private static ICacheQueryable GetCacheQueryable(Expression expression)
        {
            var subQueryExp = expression as SubQueryExpression;

            if (subQueryExp != null)
                return GetCacheQueryable(subQueryExp.QueryModel.MainFromClause);

            var srcRefExp = expression as QuerySourceReferenceExpression;

            if (srcRefExp != null)
                return GetCacheQueryable(srcRefExp);

            var constExpr = expression as ConstantExpression;

            if (constExpr == null)
                throw new NotSupportedException("Unexpected query source: " + expression);

            var cacheQuery = (ICacheQueryable) constExpr.Value;

            return cacheQuery;

            //return string.Format("\"{0}\".{1}", cacheQuery.CacheName, 
            //    GetTableNameFromEntryType(cacheQuery.ElementType));
        }

        public static string GetTableNameWithSchema(ICacheQueryable queryable)
        {
            Debug.Assert(queryable != null);

            return string.Format("\"{0}\".{1}", queryable.CacheName,
                GetTableNameFromEntryType(queryable.ElementType));
        }

        private static string GetTableNameFromEntryValueType(Type entryValueType)
        {
            Debug.Assert(entryValueType != null);

            return entryValueType.Name;
        }

        private static string GetTableNameFromEntryType(Type cacheEntryType)
        {
            Debug.Assert(cacheEntryType != null);

            if (!(cacheEntryType.IsGenericType && cacheEntryType.GetGenericTypeDefinition() == typeof(ICacheEntry<,>)))
                throw new NotSupportedException("Unexpected cache query entry type: " + cacheEntryType);

            return GetTableNameFromEntryValueType(cacheEntryType.GetGenericArguments()[1]);
        }
    }
}
