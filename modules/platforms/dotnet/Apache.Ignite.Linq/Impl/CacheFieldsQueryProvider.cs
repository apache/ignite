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
    using System.Diagnostics;
    using System.Linq;
    using System.Linq.Expressions;
    using Apache.Ignite.Core;
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
            CacheConfiguration cacheConfiguration, string tableName) : base(queryParser, executor)
        {
            Debug.Assert(ignite != null);
            Debug.Assert(cacheConfiguration != null);

            _ignite = ignite;
            _cacheConfiguration = cacheConfiguration;

            // TODO: Check against config
            _tableName = tableName;
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
    }
}