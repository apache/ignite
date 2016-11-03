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
    using System.Diagnostics.CodeAnalysis;
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
    internal class CacheFieldsQueryExecutor : IQueryExecutor
    {
        /** */
        private readonly ICacheInternal _cache;

        /** */
        private static readonly CopyOnWriteConcurrentDictionary<ConstructorInfo, object> CtorCache =
            new CopyOnWriteConcurrentDictionary<ConstructorInfo, object>();

        /** */
        private readonly bool _local;

        /** */
        private readonly int _pageSize;

        /** */
        private readonly bool _enableDistributedJoins;

        /** */
        private readonly bool _enforceJoinOrder;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheFieldsQueryExecutor" /> class.
        /// </summary>
        /// <param name="cache">The executor function.</param>
        /// <param name="local">Local flag.</param>
        /// <param name="pageSize">Size of the page.</param>
        /// <param name="enableDistributedJoins">Distributed joins flag.</param>
        /// <param name="enforceJoinOrder">Enforce join order flag.</param>
        public CacheFieldsQueryExecutor(ICacheInternal cache, bool local, int pageSize, bool enableDistributedJoins,
            bool enforceJoinOrder)
        {
            Debug.Assert(cache != null);

            _cache = cache;
            _local = local;
            _pageSize = pageSize;
            _enableDistributedJoins = enableDistributedJoins;
            _enforceJoinOrder = enforceJoinOrder;
        }

        /// <summary>
        /// Gets the local flag.
        /// </summary>
        public bool Local
        {
            get { return _local; }
        }

        /// <summary>
        /// Gets the size of the page.
        /// </summary>
        public int PageSize
        {
            get { return _pageSize; }
        }

        /// <summary>
        /// Gets a value indicating whether distributed joins are enabled.
        /// </summary>
        public bool EnableDistributedJoins
        {
            get { return _enableDistributedJoins; }
        }

        /// <summary>
        /// Gets a value indicating whether join order should be enforced.
        /// </summary>
        public bool EnforceJoinOrder
        {
            get { return _enforceJoinOrder; }
        }

        /** <inheritdoc /> */
        public T ExecuteScalar<T>(QueryModel queryModel)
        {
            return ExecuteSingle<T>(queryModel, false);
        }

        /** <inheritdoc /> */
        public T ExecuteSingle<T>(QueryModel queryModel, bool returnDefaultWhenEmpty)
        {
            var col = ExecuteCollection<T>(queryModel);

            return returnDefaultWhenEmpty ? col.SingleOrDefault() : col.Single();
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public IEnumerable<T> ExecuteCollection<T>(QueryModel queryModel)
        {
            Debug.Assert(queryModel != null);

            var qryData = GetQueryData(queryModel);

            Debug.WriteLine("\nFields Query: {0} | {1}", qryData.QueryText,
                string.Join(", ", qryData.Parameters.Select(x => x == null ? "null" : x.ToString())));

            var qry = GetFieldsQuery(qryData.QueryText, qryData.Parameters.ToArray());

            var selector = GetResultSelector<T>(queryModel.SelectClause.Selector);

            return _cache.QueryFields(qry, selector);
        }

        /// <summary>
        /// Compiles the query (old method, does not support some scenarios).
        /// </summary>
        public Func<object[], IQueryCursor<T>> CompileQuery<T>(QueryModel queryModel, Delegate queryCaller)
        {
            Debug.Assert(queryModel != null);
            Debug.Assert(queryCaller != null);

            var qryData = GetQueryData(queryModel);

            var qryText = qryData.QueryText;

            var selector = GetResultSelector<T>(queryModel.SelectClause.Selector);

            // Compiled query is a delegate with query parameters
            // Delegate parameters order and query parameters order may differ

            // These are in order of usage in query
            var qryOrderParams = qryData.ParameterExpressions.OfType<MemberExpression>()
                .Select(x => x.Member.Name).ToList();

            // These are in order they come from user
            var userOrderParams = queryCaller.Method.GetParameters().Select(x => x.Name).ToList();

            if ((qryOrderParams.Count != qryData.Parameters.Count) ||
                (qryOrderParams.Count != userOrderParams.Count))
                throw new InvalidOperationException("Error compiling query: all compiled query arguments " +
                    "should come from enclosing delegate parameters.");

            var indices = qryOrderParams.Select(x => userOrderParams.IndexOf(x)).ToArray();

            // Check if user param order is already correct
            if (indices.SequenceEqual(Enumerable.Range(0, indices.Length)))
                return args => _cache.QueryFields(GetFieldsQuery(qryText, args), selector);

            // Return delegate with reorder
            return args => _cache.QueryFields(GetFieldsQuery(qryText,
                args.Select((x, i) => args[indices[i]]).ToArray()), selector);
        }
        /// <summary>
        /// Compiles the query without regard to number or order of arguments.
        /// </summary>
        public Func<object[], IQueryCursor<T>> CompileQuery<T>(QueryModel queryModel)
        {
            Debug.Assert(queryModel != null);

            var qryText = GetQueryData(queryModel).QueryText;
            var selector = GetResultSelector<T>(queryModel.SelectClause.Selector);

            return args => _cache.QueryFields(GetFieldsQuery(qryText, args), selector);
        }

        /// <summary>
        /// Compiles the query.
        /// </summary>
        /// <typeparam name="T">Result type.</typeparam>
        /// <param name="queryModel">The query model.</param>
        /// <param name="queryLambdaModel">The query model generated from lambda body.</param>
        /// <param name="queryLambda">The query lambda.</param>
        /// <returns>Compiled query func.</returns>
        public Func<object[], IQueryCursor<T>> CompileQuery<T>(QueryModel queryModel, QueryModel queryLambdaModel,
            LambdaExpression queryLambda)
        {
            Debug.Assert(queryModel != null);

            // Get model from lambda to map arguments properly.
            var qryData = GetQueryData(queryLambdaModel);

            var qryText = GetQueryData(queryModel).QueryText;
            var qryTextLambda = qryData.QueryText;

            if (qryText != qryTextLambda)
            {
                Debug.WriteLine(qryText);
                Debug.WriteLine(qryTextLambda);

                throw new InvalidOperationException("Error compiling query: entire LINQ expression should be " +
                                                    "specified within lambda passed to Compile method. " +
                                                    "Part of the query can't be outside the Compile method call.");
            }

            var selector = GetResultSelector<T>(queryModel.SelectClause.Selector);

            var qryParams = qryData.Parameters.ToArray();

            // Compiled query is a delegate with query parameters
            // Delegate parameters order and query parameters order may differ

            // Simple case: lambda with no parameters. Only embedded parameters are used.
            if (!queryLambda.Parameters.Any())
            {
                return argsUnused => _cache.QueryFields(GetFieldsQuery(qryText, qryParams), selector);
            }

            // These are in order of usage in query
            var qryOrderArgs = qryParams.OfType<ParameterExpression>().Select(x => x.Name).ToArray();

            // These are in order they come from user
            var userOrderArgs = queryLambda.Parameters.Select(x => x.Name).ToList();

            // Simple case: all query args directly map to the lambda args in the same order
            if (qryOrderArgs.Length == qryParams.Length
                && qryOrderArgs.SequenceEqual(userOrderArgs))
            {
                return args => _cache.QueryFields(GetFieldsQuery(qryText, args), selector);
            }

            // General case: embedded args and lambda args are mixed; same args can be used multiple times.
            // Produce a mapping that defines where query arguments come from.
            var mapping = qryParams.Select(x =>
            {
                var pe = x as ParameterExpression;

                if (pe != null)
                    return userOrderArgs.IndexOf(pe.Name);

                return -1;
            }).ToArray();

            return args => _cache.QueryFields(
                GetFieldsQuery(qryText, MapQueryArgs(args, qryParams, mapping)), selector);
        }

        /// <summary>
        /// Maps the query arguments.
        /// </summary>
        private static object[] MapQueryArgs(object[] userArgs, object[] embeddedArgs, int[] mapping)
        {
            var mappedArgs = new object[embeddedArgs.Length];

            for (var i = 0; i < mappedArgs.Length; i++)
            {
                var map = mapping[i];

                mappedArgs[i] = map < 0 ? embeddedArgs[i] : userArgs[map];
            }

            return mappedArgs;
        }

        /// <summary>
        /// Gets the fields query.
        /// </summary>
        private SqlFieldsQuery GetFieldsQuery(string text, object[] args)
        {
            return new SqlFieldsQuery(text, _local, args)
            {
                EnableDistributedJoins = _enableDistributedJoins,
                PageSize = _pageSize,
                EnforceJoinOrder = _enforceJoinOrder
            };
        }

        /** <inheritdoc /> */
        public static QueryData GetQueryData(QueryModel queryModel)
        {
            Debug.Assert(queryModel != null);

            return new CacheQueryModelVisitor().GenerateQuery(queryModel);
        }

        /// <summary>
        /// Gets the result selector.
        /// </summary>
        private static Func<IBinaryRawReader, int, T> GetResultSelector<T>(Expression selectorExpression)
        {
            var newExpr = selectorExpression as NewExpression;

            if (newExpr != null)
                return GetCompiledCtor<T>(newExpr.Constructor);

            var entryCtor = GetCacheEntryCtorInfo(typeof(T));

            if (entryCtor != null)
                return GetCompiledCtor<T>(entryCtor);

            if (typeof(T) == typeof(bool))
                return ReadBool<T>;

            return (reader, count) => reader.ReadObject<T>();
        }

        /// <summary>
        /// Reads the bool. Actual data may be bool or int/long.
        /// </summary>
        private static T ReadBool<T>(IBinaryRawReader reader, int count)
        {
            var obj = reader.ReadObject<object>();

            if (obj is bool)
                return (T) obj;

            if (obj is long)
                return TypeCaster<T>.Cast((long) obj != 0);

            if (obj is int)
                return TypeCaster<T>.Cast((int) obj != 0);

            throw new InvalidOperationException("Expected bool, got: " + obj);
        }

        /// <summary>
        /// Gets the cache entry constructor.
        /// </summary>
        private static ConstructorInfo GetCacheEntryCtorInfo(Type entryType)
        {
            if (!entryType.IsGenericType || entryType.GetGenericTypeDefinition() != typeof(ICacheEntry<,>))
                return null;

            var args = entryType.GetGenericArguments();

            var targetType = typeof (CacheEntry<,>).MakeGenericType(args);

            return targetType.GetConstructors().Single();
        }

        /// <summary>
        /// Gets the compiled constructor.
        /// </summary>
        private static Func<IBinaryRawReader, int, T> GetCompiledCtor<T>(ConstructorInfo ctorInfo)
        {
            object result;

            if (CtorCache.TryGetValue(ctorInfo, out result))
                return (Func<IBinaryRawReader, int, T>) result;

            return (Func<IBinaryRawReader, int, T>) CtorCache.GetOrAdd(ctorInfo, x =>
            {
                var innerCtor1 = DelegateConverter.CompileCtor<T>(x, GetCacheEntryCtorInfo);

                return (Func<IBinaryRawReader, int, T>) ((r, c) => innerCtor1(r));
            });
        }
    }
}