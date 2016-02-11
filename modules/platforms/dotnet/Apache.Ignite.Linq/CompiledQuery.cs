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
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Linq.Impl;

    /// <summary>
    /// Represents a compiled cache query.
    /// </summary>
    public static class CompiledQuery
    {
        /// <summary>
        /// Creates a new delegate that represents the compiled cache query with abritrary number of arguments.
        /// </summary>
        /// <param name="query">The query to compile.</param>
        /// <returns>Delegate that represents the compiled cache query.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0", 
            Justification = "Invalid warning, validation is present.")]
        public static Func<object[], IQueryCursor<T>> Compile<T>(IQueryable<T> query)
        {
            IgniteArgumentCheck.NotNull(query, "query");

            var compiledQuery = GetCompiledQuery(query);

            return x => compiledQuery(new object[] {x});
        }

        /// <summary>
        /// Creates a new delegate that represents the compiled cache query.
        /// </summary>
        /// <param name="query">The query to compile.</param>
        /// <returns>Delegate that represents the compiled cache query.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0", 
            Justification = "Invalid warning, validation is present.")]
        public static Func<TArg1, IQueryCursor<T>> Compile<T, TArg1>(Func<TArg1, IQueryable<T>> query)
        {
            IgniteArgumentCheck.NotNull(query, "query");

            var compiledQuery = GetCompiledQuery(query(default(TArg1)));

            return x => compiledQuery(new object[] {x});
        }

        /// <summary>
        /// Creates a new delegate that represents the compiled cache query.
        /// </summary>
        /// <param name="query">The query to compile.</param>
        /// <returns>Delegate that represents the compiled cache query.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0", 
            Justification = "Invalid warning, validation is present.")]
        public static Func<TArg1, TArg2, IQueryCursor<T>> Compile<T, TArg1, TArg2>(Func<TArg1, TArg2, 
            IQueryable<T>> query)
        {
            IgniteArgumentCheck.NotNull(query, "query");

            var compiledQuery = GetCompiledQuery(query(default(TArg1), default(TArg2)));

            return (x, y) => compiledQuery(new object[] {x, y});
        }

        /// <summary>
        /// Gets the compiled query.
        /// </summary>
        private static Func<object[], IQueryCursor<T>> GetCompiledQuery<T>(IQueryable<T> queryable)
        {
            var cacheQueryable = queryable as ICacheQueryableInternal;

            if (cacheQueryable == null)
                throw new ArgumentException(
                    string.Format("{0} can only compile cache queries produced by AsCacheQueryable method. " +
                                  "Provided query is not valid: '{1}'", typeof (CompiledQuery).FullName, queryable));

            return cacheQueryable.CompileQuery<T>();
        }
    }
}
