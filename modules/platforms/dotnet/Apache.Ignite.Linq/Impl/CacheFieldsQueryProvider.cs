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
    using Remotion.Linq;
    using Remotion.Linq.Parsing.Structure;

    /// <summary>
    /// Query provider for fields queries (projections).
    /// </summary>
    internal class CacheFieldsQueryProvider : QueryProviderBase
    {
        //  TODO: Do we even need this class?

        public CacheFieldsQueryProvider(IQueryParser queryParser, IQueryExecutor executor) : base(queryParser, executor)
        {
            // No-op.
        }

        public override IQueryable<T> CreateQuery<T>(Expression expression)
        {
            return new CacheFieldsQueryable<T>(this, expression);
        }
    }
}