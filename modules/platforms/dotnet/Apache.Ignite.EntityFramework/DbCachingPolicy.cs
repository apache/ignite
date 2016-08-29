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

namespace Apache.Ignite.EntityFramework
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Data.Entity.Core.Metadata.Edm;

    /// <summary>
    /// Caching policy: defines which queries should be cached.
    /// </summary>
    public class DbCachingPolicy
    {
        /// <summary>
        /// Determines whether the specified query can be cached.
        /// </summary>
        /// <param name="affectedEntitySets">Entity sets affected by the query.</param>
        /// <param name="sql">SQL statement for the query.</param>
        /// <param name="parameters">Query parameters.</param>
        /// <returns>
        /// <c>true</c> if the specified query can be cached; otherwise, <c>false</c>.
        /// </returns>
        protected internal virtual bool CanBeCached(ICollection<EntitySetBase> affectedEntitySets, string sql,
            DbParameterCollection parameters)
        {
            return true;
        }

        /// <summary>
        /// Determines whether specified number of rows should be cached.
        /// </summary>
        /// <param name="affectedEntitySets">Entity sets affected by the query.</param>
        /// <param name="sql">SQL statement for the query.</param>
        /// <param name="parameters">Query parameters.</param>
        /// <param name="rowCount">The count of fetched rows.</param>
        protected internal virtual bool CanBeCached(ICollection<EntitySetBase> affectedEntitySets, string sql,
            DbParameterCollection parameters, int rowCount)
        {
            return true;
        }

        /// <summary>
        /// Gets the absolute expiration timeout for a given query.
        /// </summary>
        /// <param name="affectedEntitySets">Entity sets affected by the command.</param>
        /// <param name="sql">SQL statement for the query.</param>
        /// <param name="parameters">Query parameters.</param>
        protected internal virtual TimeSpan GetExpirationTimeout(ICollection<EntitySetBase> affectedEntitySets, 
            string sql, DbParameterCollection parameters)
        {
            return TimeSpan.MaxValue;
        }

        /// <summary>
        /// Gets the caching strategy for a give query.
        /// </summary>
        /// <param name="affectedEntitySets">Entity sets affected by the query.</param>
        /// <param name="sql">SQL statement for the query.</param>
        /// <param name="parameters">Query parameters.</param>
        /// <returns></returns>
        protected internal virtual DbCachingStrategy GetCachingStrategy(ICollection<EntitySetBase> affectedEntitySets, 
            string sql, DbParameterCollection parameters)
        {
            return DbCachingStrategy.ReadWrite;
        }
    }
}
