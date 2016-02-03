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
                // TODO: Compile func
                return (reader, count) => 
                (T) newExpr.Constructor.Invoke(GetArguments(reader, count, newExpr.Arguments));
            }

            var entryCtor = GetCacheEntryCtor(typeof (T));

            if (entryCtor != null)
            {
                return (reader, count) => (T) entryCtor(reader.ReadObject<object>(), reader.ReadObject<object>());
            }

            return ConvertSingleField<T>;
        }

        /// <summary>
        /// Gets the arguments.
        /// </summary>
        private static object[] GetArguments(IBinaryRawReader reader, int count, ICollection<Expression> arguments)
        {
            var result = new List<object>(count);

            foreach (var arg in arguments)
            {
                if (arg.Type.IsGenericType && arg.Type.GetGenericTypeDefinition() == typeof (ICacheEntry<,>))
                {
                    // Construct cache entry from key and value
                    var entryType = typeof (CacheEntry<,>).MakeGenericType(arg.Type.GetGenericArguments());

                    var entry = Activator.CreateInstance(entryType, reader.ReadObject<object>(), reader.ReadObject<object>());

                    result.Add(entry);
                }
                else
                    result.Add(reader.ReadObject<object>());
            }

            return result.ToArray();
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

        private static Func<object, object, object> GetCacheEntryCtor(Type entryType)
        {
            // TODO: Cache ctor somewhere in Core, because this is a common task. Probably CacheEntry.CreateInstance(Type, Type) or something.
            if (!entryType.IsGenericType || entryType.GetGenericTypeDefinition() != typeof (ICacheEntry<,>))
                return null;

            var args = entryType.GetGenericArguments();

            var targetType = typeof (CacheEntry<,>).MakeGenericType(args);

            return DelegateConverter.CompileCtor<Func<object, object, object>>(targetType, args);
        }
    }
}