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
    using System.Linq;
    using System.Linq.Expressions;
    using Apache.Ignite.Core;
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
        private readonly string _cacheName;
        
        /** */
        private readonly string _tableName;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheFieldsQueryProvider"/> class.
        /// </summary>
        public CacheFieldsQueryProvider(IQueryParser queryParser, IQueryExecutor executor, IIgnite ignite, 
            string cacheName, string tableName) : base(queryParser, executor)
        {
            _ignite = ignite;
            _cacheName = cacheName;
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
        public string CacheName
        {
            get { return _cacheName; }
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