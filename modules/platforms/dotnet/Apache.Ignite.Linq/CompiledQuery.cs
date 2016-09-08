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

namespace Apache.Ignite.Linq
{
    using System;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Linq.Impl;

    /// <summary>
    /// Represents a compiled cache query.
    /// </summary>
    [Obsolete("Use CompiledQuery2 class.")]
    public static class CompiledQuery
    {
        /// <summary>
        /// Creates a new delegate that represents the compiled cache query.
        /// </summary>
        /// <param name="query">The query to compile.</param>
        /// <returns>Delegate that represents the compiled cache query.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0", 
            Justification = "Invalid warning, validation is present.")]
        public static Func<IQueryCursor<T>> Compile<T>(Func<IQueryable<T>> query)
        {
            IgniteArgumentCheck.NotNull(query, "query");

            var compiledQuery = GetCompiledQuery(query(), query);

            return () => compiledQuery(new object[0]);
        }

        /// <summary>
        /// Creates a new delegate that represents the compiled cache query.
        /// </summary>
        /// <param name="query">The query to compile.</param>
        /// <returns>Delegate that represents the compiled cache query.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0", 
            Justification = "Invalid warning, validation is present.")]
        public static Func<T1, IQueryCursor<T>> Compile<T, T1>(Func<T1, IQueryable<T>> query)
        {
            IgniteArgumentCheck.NotNull(query, "query");

            var compiledQuery = GetCompiledQuery(query(default(T1)), query);

            return x => compiledQuery(new object[] {x});
        }

        /// <summary>
        /// Creates a new delegate that represents the compiled cache query.
        /// </summary>
        /// <param name="query">The query to compile.</param>
        /// <returns>Delegate that represents the compiled cache query.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0", 
            Justification = "Invalid warning, validation is present.")]
        public static Func<T1, T2, IQueryCursor<T>> Compile<T, T1, T2>(Func<T1, T2, 
            IQueryable<T>> query)
        {
            IgniteArgumentCheck.NotNull(query, "query");

            var compiledQuery = GetCompiledQuery(query(default(T1), default(T2)), query);

            return (x, y) => compiledQuery(new object[] {x, y});
        }

        /// <summary>
        /// Creates a new delegate that represents the compiled cache query.
        /// </summary>
        /// <param name="query">The query to compile.</param>
        /// <returns>Delegate that represents the compiled cache query.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0", 
            Justification = "Invalid warning, validation is present.")]
        public static Func<T1, T2, T3, IQueryCursor<T>> Compile<T, T1, T2, T3>(Func<T1, T2, T3,
            IQueryable<T>> query)
        {
            IgniteArgumentCheck.NotNull(query, "query");

            var compiledQuery = GetCompiledQuery(query(default(T1), default(T2), default(T3)), query);

            return (x, y, z) => compiledQuery(new object[] {x, y, z});
        }

        /// <summary>
        /// Creates a new delegate that represents the compiled cache query.
        /// </summary>
        /// <param name="query">The query to compile.</param>
        /// <returns>Delegate that represents the compiled cache query.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0", 
            Justification = "Invalid warning, validation is present.")]
        public static Func<T1, T2, T3, T4, IQueryCursor<T>> Compile<T, T1, T2, T3, T4>(Func<T1, T2, T3, T4,
            IQueryable<T>> query)
        {
            IgniteArgumentCheck.NotNull(query, "query");

            var compiledQuery = GetCompiledQuery(query(default(T1), default(T2), default(T3), default(T4)), query);

            return (x, y, z, a) => compiledQuery(new object[] {x, y, z, a});
        }

        /// <summary>
        /// Creates a new delegate that represents the compiled cache query.
        /// </summary>
        /// <param name="query">The query to compile.</param>
        /// <returns>Delegate that represents the compiled cache query.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0", 
            Justification = "Invalid warning, validation is present.")]
        public static Func<T1, T2, T3, T4, T5, IQueryCursor<T>> Compile<T, T1, T2, T3, T4, T5>(
            Func<T1, T2, T3, T4, T5, IQueryable<T>> query)
        {
            IgniteArgumentCheck.NotNull(query, "query");

            var compiledQuery =
                GetCompiledQuery(query(default(T1), default(T2), default(T3), default(T4), default(T5)), query);

            return (x, y, z, a, b) => compiledQuery(new object[] {x, y, z, a, b});
        }

        /// <summary>
        /// Creates a new delegate that represents the compiled cache query.
        /// </summary>
        /// <param name="query">The query to compile.</param>
        /// <returns>Delegate that represents the compiled cache query.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0", 
            Justification = "Invalid warning, validation is present.")]
        public static Func<T1, T2, T3, T4, T5, T6, IQueryCursor<T>> Compile<T, T1, T2, T3, T4, T5, T6>(
            Func<T1, T2, T3, T4, T5, T6, IQueryable<T>> query)
        {
            IgniteArgumentCheck.NotNull(query, "query");

            var compiledQuery = GetCompiledQuery(query(default(T1), default(T2), default(T3), default(T4), 
                default(T5), default(T6)), query);

            return (x, y, z, a, b, c) => compiledQuery(new object[] {x, y, z, a, b, c});
        }

        /// <summary>
        /// Creates a new delegate that represents the compiled cache query.
        /// </summary>
        /// <param name="query">The query to compile.</param>
        /// <returns>Delegate that represents the compiled cache query.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0", 
            Justification = "Invalid warning, validation is present.")]
        public static Func<T1, T2, T3, T4, T5, T6, T7, IQueryCursor<T>> Compile<T, T1, T2, T3, T4, T5, T6, T7>(
            Func<T1, T2, T3, T4, T5, T6, T7, IQueryable<T>> query)
        {
            IgniteArgumentCheck.NotNull(query, "query");

            var compiledQuery = GetCompiledQuery(query(default(T1), default(T2), default(T3), default(T4), 
                default(T5), default(T6), default(T7)), query);

            return (x, y, z, a, b, c, d) => compiledQuery(new object[] {x, y, z, a, b, c, d});
        }

        /// <summary>
        /// Creates a new delegate that represents the compiled cache query.
        /// </summary>
        /// <param name="query">The query to compile.</param>
        /// <returns>Delegate that represents the compiled cache query.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0", 
            Justification = "Invalid warning, validation is present.")]
        public static Func<T1, T2, T3, T4, T5, T6, T7, T8, IQueryCursor<T>> Compile<T, T1, T2, T3, T4, T5, T6, T7, T8>(
            Func<T1, T2, T3, T4, T5, T6, T7, T8, IQueryable<T>> query)
        {
            IgniteArgumentCheck.NotNull(query, "query");

            var compiledQuery = GetCompiledQuery(query(default(T1), default(T2), default(T3), default(T4), 
                default(T5), default(T6), default(T7), default(T8)), query);

            return (x, y, z, a, b, c, d, e) => compiledQuery(new object[] {x, y, z, a, b, c, d, e});
        }

        /// <summary>
        /// Gets the compiled query.
        /// </summary>
        private static Func<object[], IQueryCursor<T>> GetCompiledQuery<T>(IQueryable<T> queryable, 
            Delegate queryCaller)
        {
            Debug.Assert(queryCaller != null);

            var cacheQueryable = queryable as ICacheQueryableInternal;

            if (cacheQueryable == null)
                throw new ArgumentException(
                    string.Format("{0} can only compile cache queries produced by AsCacheQueryable method. " +
                                  "Provided query is not valid: '{1}'", typeof (CompiledQuery).FullName, queryable));

            Debug.WriteLine(queryable);

            return cacheQueryable.CompileQuery<T>(queryCaller);
        }
    }
}
