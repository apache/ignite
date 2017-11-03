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

    /// <summary>
    /// Default caching policy implementation: everything is cached with <see cref="DbCachingMode.ReadWrite"/>, 
    /// no expiration.
    /// </summary>
    // ReSharper disable once ClassWithVirtualMembersNeverInherited.Global
    public class DbCachingPolicy : IDbCachingPolicy
    {
        /// <summary>
        /// Determines whether the specified query can be cached.
        /// </summary>
        /// <param name="queryInfo">The query information.</param>
        /// <returns>
        ///   <c>true</c> if the specified query can be cached; otherwise, <c>false</c>.
        /// </returns>
        public virtual bool CanBeCached(DbQueryInfo queryInfo)
        {
            return true;
        }

        /// <summary>
        /// Determines whether specified number of rows should be cached.
        /// </summary>
        /// <param name="queryInfo">The query information.</param>
        /// <param name="rowCount">The count of fetched rows.</param>
        /// <returns></returns>
        public virtual bool CanBeCached(DbQueryInfo queryInfo, int rowCount)
        {
            return true;
        }

        /// <summary>
        /// Gets the absolute expiration timeout for a given query.
        /// </summary>
        /// <param name="queryInfo">The query information.</param>
        /// <returns>Expiration timeout. <see cref="TimeSpan.MaxValue"/> for no expiration.</returns>
        public virtual TimeSpan GetExpirationTimeout(DbQueryInfo queryInfo)
        {
            return TimeSpan.MaxValue;
        }

        /// <summary>
        /// Gets the caching strategy for a give query.
        /// </summary>
        /// <param name="queryInfo">The query information.</param>
        /// <returns>Caching strategy for the query.</returns>
        public virtual DbCachingMode GetCachingMode(DbQueryInfo queryInfo)
        {
            return DbCachingMode.ReadWrite;
        }
    }
}