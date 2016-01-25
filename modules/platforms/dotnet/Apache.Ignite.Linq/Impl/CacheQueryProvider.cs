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
    using Apache.Ignite.Core.Cache;
    using Remotion.Linq;

    /// <summary>
    /// Cache query provider.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    internal class CacheQueryProvider<TKey, TValue> : QueryProviderBase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CacheQueryProvider{TKey, TValue}" /> class.
        /// </summary>
        /// <param name="cache">The cache.</param>
        /// <param name="queryTypeName">Name of the query type.</param>
        public CacheQueryProvider(ICache<TKey, TValue> cache, string queryTypeName) 
            : base(Remotion.Linq.Parsing.Structure.QueryParser.CreateDefault(), 
                  new CacheQueryExecutor<TKey, TValue>(cache, queryTypeName))
        {
            // No-op.
        }

        /// <summary>
        /// Constructs an <see cref="T:System.Linq.IQueryable`1" /> object that can evaluate the query 
        /// represented by a specified expression tree. This method is
        /// called by the standard query operators defined by the <see cref="T:System.Linq.Queryable" /> class.
        /// </summary>
        /// <typeparam name="T">Element type.</typeparam>
        /// <param name="expression">An expression tree that represents a LINQ query.</param>
        /// <returns>
        /// An <see cref="T:System.Linq.IQueryable`1" /> that can evaluate the query represented 
        /// by the specified expression tree.
        /// </returns>
        public override IQueryable<T> CreateQuery<T>(Expression expression)
        {
            return (IQueryable<T>) new CacheQueryable<TKey, TValue>(this, expression);
        }
    }
}