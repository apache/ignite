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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Common;
    using Remotion.Linq;

    /// <summary>
    /// Fields query executor.
    /// </summary>
    internal class CacheFieldsQueryExecutor : ICacheQueryExecutor
    {
        /** */
        private readonly ICacheQueryProxy _cache;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheFieldsQueryExecutor" /> class.
        /// </summary>
        /// <param name="cache">The executor function.</param>
        public CacheFieldsQueryExecutor(ICacheQueryProxy cache)
        {
            Debug.Assert(cache != null);

            _cache = cache;
        }

        /** <inheritdoc /> */
        public T ExecuteScalar<T>(QueryModel queryModel)
        {
            return ExecuteSingle<T>(queryModel, false);
        }

        /** <inheritdoc /> */
        public T ExecuteSingle<T>(QueryModel queryModel, bool returnDefaultWhenEmpty)
        {
            var collection = ExecuteCollection<T>(queryModel);

            return returnDefaultWhenEmpty ? collection.SingleOrDefault() : collection.Single();
        }

        /** <inheritdoc /> */
        public IEnumerable<T> ExecuteCollection<T>(QueryModel queryModel)
        {
            var queryData = GetQueryData(queryModel);

            var query = new SqlFieldsQuery(queryData.QueryText, queryData.Parameters.ToArray());

            Debug.WriteLine("\nFields Query: {0} | {1}", queryData.QueryText,
                string.Join(", ", queryData.Parameters.Select(x => x == null ? "null" : x.ToString())));

            var selector = GetResultSelector<T>(queryModel.SelectClause.Selector);

            var queryCursor = _cache.QueryFields(query, selector);

            return queryCursor;
        }

        /** <inheritdoc /> */
        public QueryData GetQueryData(QueryModel queryModel)
        {
            return new CacheQueryModelVisitor().GenerateQuery(queryModel);
        }

        /// <summary>
        /// Gets the result selector.
        /// </summary>
        private static Func<IBinaryRawReader, int, T> GetResultSelector<T>(Expression selectorExpression)
        {
            var newExpr = selectorExpression as NewExpression;

            if (newExpr != null)
            {
                var ctor = DelegateConverter.CompileCtor<T>(newExpr.Constructor, GetCacheEntryCtor);

                return (reader, count) => ctor(reader);
            }

            var entryCtor = GetCacheEntryCtor<T>();

            if (entryCtor != null)
                return (reader, count) => entryCtor(reader);

            return ConvertSingleField<T>;
        }

        /// <summary>
        /// Converts the single field from the list to specified type.
        /// </summary>
        private static T ConvertSingleField<T>(IBinaryRawReader reader, int count)
        {
            if (count != 1)
                throw new InvalidOperationException("Single-field query returned unexpected number of values: " + 
                    count);

            return reader.ReadObject<T>();
        }

        private static Func<IBinaryRawReader, T> GetCacheEntryCtor<T>()
        {
            var ctor = GetCacheEntryCtor(typeof (T));

            return ctor == null ? null : DelegateConverter.CompileCtor<T>(ctor, null);
        }

        private static ConstructorInfo GetCacheEntryCtor(Type entryType)
        {
            // TODO: Cache ctor somewhere in Core, because this is a common task. Probably CacheEntry.CreateInstance(Type, Type) or something.
            if (!entryType.IsGenericType || entryType.GetGenericTypeDefinition() != typeof(ICacheEntry<,>))
                return null;

            var args = entryType.GetGenericArguments();

            var targetType = typeof (CacheEntry<,>).MakeGenericType(args);

            return targetType.GetConstructors().Single();
        }
    }
}