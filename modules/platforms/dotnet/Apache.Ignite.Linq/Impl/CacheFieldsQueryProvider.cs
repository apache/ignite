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
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Remotion.Linq;
    using Remotion.Linq.Clauses.StreamedData;
    using Remotion.Linq.Parsing.Structure;
    using Remotion.Linq.Utilities;

    /// <summary>
    /// Query provider for fields queries (projections).
    /// </summary>
    internal class CacheFieldsQueryProvider : IQueryProvider
    {
        /** */
        private static readonly MethodInfo GenericCreateQueryMethod =
            typeof (CacheFieldsQueryProvider).GetMethods().Single(m => m.Name == "CreateQuery" && m.IsGenericMethod);

        /** */
        private readonly IQueryParser _parser;
        
        /** */
        private readonly CacheFieldsQueryExecutor _executor;

        /** */
        private readonly IIgnite _ignite;

        /** */
        private readonly CacheConfiguration _cacheConfiguration;

        /** */
        private readonly string _tableName;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheFieldsQueryProvider"/> class.
        /// </summary>
        public CacheFieldsQueryProvider(IQueryParser queryParser, CacheFieldsQueryExecutor executor, IIgnite ignite, 
            CacheConfiguration cacheConfiguration, string tableName, Type cacheValueType) 
        {
            Debug.Assert(queryParser != null);
            Debug.Assert(executor != null);
            Debug.Assert(cacheConfiguration != null);
            Debug.Assert(cacheValueType != null);

            _parser = queryParser;
            _executor = executor;
            _ignite = ignite;
            _cacheConfiguration = cacheConfiguration;

            if (tableName != null)
            {
                _tableName = tableName;

                ValidateTableName();
            }
            else
                _tableName = InferTableName(cacheValueType);
        }

        /// <summary>
        /// Gets the ignite.
        /// </summary>
        [Obsolete("Deprecated, null for thin client, only used for ICacheQueryable.")]
        public IIgnite Ignite
        {
            get { return _ignite; }
        }

        /// <summary>
        /// Gets the name of the cache.
        /// </summary>
        public CacheConfiguration CacheConfiguration
        {
            get { return _cacheConfiguration; }
        }

        /// <summary>
        /// Gets the name of the table.
        /// </summary>
        public string TableName
        {
            get { return _tableName; }
        }

        /// <summary>
        /// Gets the executor.
        /// </summary>
        public CacheFieldsQueryExecutor Executor
        {
            get { return _executor; }
        }

        /// <summary>
        /// Generates the query model.
        /// </summary>
        public QueryModel GenerateQueryModel(Expression expression)
        {
            return _parser.GetParsedQuery(expression);
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public IQueryable CreateQuery(Expression expression)
        {
            Debug.Assert(expression != null);

            var elementType = GetItemTypeOfClosedGenericIEnumerable(expression.Type, "expression");

            // Slow, but this method is never called during normal LINQ usage with generics
            return (IQueryable) GenericCreateQueryMethod.MakeGenericMethod(elementType)
                .Invoke(this, new object[] {expression});
        }

        /** <inheritdoc /> */
        public IQueryable<T> CreateQuery<T>(Expression expression)
        {
            return new CacheFieldsQueryable<T>(this, expression);
        }

        /** <inheritdoc /> */
        object IQueryProvider.Execute(Expression expression)
        {
            return Execute(expression);
        }

        /** <inheritdoc /> */
        public TResult Execute<TResult>(Expression expression)
        {
            return (TResult) Execute(expression).Value;
        }

        /// <summary>
        /// Executes the specified expression.
        /// </summary>
        private IStreamedData Execute(Expression expression)
        {
            var model = GenerateQueryModel(expression);

            return model.Execute(_executor);
        }

        /// <summary>
        /// Validates the name of the table.
        /// </summary>
        private void ValidateTableName()
        {
            var validTableNames = GetValidTableNames().Select(x => EscapeTableName(x)).ToArray();

            if (!validTableNames.Contains(_tableName, StringComparer.OrdinalIgnoreCase))
            {
                throw new CacheException(string.Format("Invalid table name specified for CacheQueryable: '{0}'; " +
                                                       "configured table names are: {1}",
                                                        _tableName,
                                                        validTableNames.Aggregate((x, y) => x + ", " + y)));
            }
        }

        /// <summary>
        /// Gets the valid table names for current cache.
        /// </summary>
        private string[] GetValidTableNames()
        {
            // Split on '.' to throw away Java type namespace
            var validTableNames = _cacheConfiguration.QueryEntities == null
                ? null
                : _cacheConfiguration.QueryEntities.Select(GetTableName).ToArray();

            if (validTableNames == null || !validTableNames.Any())
                throw new CacheException(string.Format("Queries are not configured for cache '{0}'",
                    _cacheConfiguration.Name ?? "null"));

            return validTableNames;
        }

        /// <summary>
        /// Gets the name of the SQL table.
        /// </summary>
        private static string GetTableName(QueryEntity e)
        {
            return e.TableName ?? e.ValueTypeName;
        }

        /// <summary>
        /// Infers the name of the table from cache configuration.
        /// </summary>
        /// <param name="cacheValueType"></param>
        private string InferTableName(Type cacheValueType)
        {
            var validTableNames = GetValidTableNames();

            if (validTableNames.Length == 1)
            {
                return EscapeTableName(validTableNames[0]);
            }

            // Try with full type name (this works when TableName is not set).
            var valueTypeName = cacheValueType.FullName;

            if (valueTypeName != null)
            {
                if (validTableNames.Contains(valueTypeName, StringComparer.OrdinalIgnoreCase))
                {
                    return EscapeTableName(valueTypeName);
                }

                // Remove namespace and nested class qualification and try again.
                valueTypeName = EscapeTableName(valueTypeName);

                if (validTableNames.Contains(valueTypeName, StringComparer.OrdinalIgnoreCase))
                {
                    return valueTypeName;
                }
            }

            throw new CacheException(string.Format("Table name cannot be inferred for cache '{0}', " +
                                                   "please use AsCacheQueryable overload with tableName parameter. " +
                                                   "Valid table names: {1}", _cacheConfiguration.Name ?? "null",
                                                    validTableNames.Aggregate((x, y) => x + ", " + y)));
        }

        /// <summary>
        /// Escapes the name of the table: strips namespace and nested class qualifiers.
        /// </summary>
        private static string EscapeTableName(string valueTypeName)
        {
            var nsIndex = Math.Max(valueTypeName.LastIndexOf('.'), valueTypeName.LastIndexOf('+'));

            return nsIndex > 0 ? valueTypeName.Substring(nsIndex + 1) : valueTypeName;
        }

        /// <summary>
        /// Gets the item type of closed generic i enumerable.
        /// </summary>
        private static Type GetItemTypeOfClosedGenericIEnumerable(Type enumerableType, string argumentName)
        {
            Type itemType;

            if (!ItemTypeReflectionUtility.TryGetItemTypeOfClosedGenericIEnumerable(enumerableType, out itemType))
            {
                var message = string.Format("Expected a closed generic type implementing IEnumerable<T>, " +
                                            "but found '{0}'.", enumerableType);

                throw new ArgumentException(message, argumentName);
            }

            return itemType;
        }
    }
}