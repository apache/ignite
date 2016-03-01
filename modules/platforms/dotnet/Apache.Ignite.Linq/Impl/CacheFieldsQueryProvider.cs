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
    using System.Linq;
    using System.Linq.Expressions;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Remotion.Linq;
    using Remotion.Linq.Parsing.Structure;

    /// <summary>
    /// Query provider for fields queries (projections).
    /// </summary>
    internal class CacheFieldsQueryProvider : QueryProviderBase
    {
        /** */
        private readonly IIgnite _ignite;

        /** */
        private readonly CacheConfiguration _cacheConfiguration;

        /** */
        private readonly string _tableName;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheFieldsQueryProvider"/> class.
        /// </summary>
        public CacheFieldsQueryProvider(IQueryParser queryParser, IQueryExecutor executor, IIgnite ignite, 
            CacheConfiguration cacheConfiguration, string tableName, Type cacheValueType) 
            : base(queryParser, executor)
        {
            Debug.Assert(ignite != null);
            Debug.Assert(cacheConfiguration != null);
            Debug.Assert(cacheValueType != null);

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

        /** <inheritdoc /> */
        public override IQueryable<T> CreateQuery<T>(Expression expression)
        {
            return new CacheFieldsQueryable<T>(this, expression);
        }

        /// <summary>
        /// Validates the name of the table.
        /// </summary>
        private void ValidateTableName()
        {
            var validTableNames = GetValidTableNames();

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
                : _cacheConfiguration.QueryEntities.Select(e => e.ValueTypeName.Split('.').Last()).ToArray();

            if (validTableNames == null || !validTableNames.Any())
                throw new CacheException(string.Format("Queries are not configured for cache '{0}'",
                    _cacheConfiguration.Name ?? "null"));

            return validTableNames;
        }

        /// <summary>
        /// Infers the name of the table from cache configuration.
        /// </summary>
        /// <param name="cacheValueType"></param>
        private string InferTableName(Type cacheValueType)
        {
            var validTableNames = GetValidTableNames();

            if (validTableNames.Length == 1)
                return validTableNames[0];

            var valueTypeName = cacheValueType.Name;

            if (validTableNames.Contains(valueTypeName, StringComparer.OrdinalIgnoreCase))
                return valueTypeName;

            throw new CacheException(string.Format("Table name cannot be inferred for cache '{0}', " +
                                                   "please use AsCacheQueryable overload with tableName parameter. " +
                                                   "Valid table names: {1}", _cacheConfiguration.Name ?? "null",
                                                    validTableNames.Aggregate((x, y) => x + ", " + y)));
        }
    }
}