/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
    /// Caching policy: defines which queries should be cached.
    /// </summary>
    public interface IDbCachingPolicy
    {
        /// <summary>
        /// Determines whether the specified query can be cached.
        /// </summary>
        /// <param name="queryInfo">The query information.</param>
        /// <returns>
        ///   <c>true</c> if the specified query can be cached; otherwise, <c>false</c>.
        /// </returns>
        bool CanBeCached(DbQueryInfo queryInfo);

        /// <summary>
        /// Determines whether specified number of rows should be cached.
        /// </summary>
        /// <param name="queryInfo">The query information.</param>
        /// <param name="rowCount">The count of fetched rows.</param>
        /// <returns></returns>
        bool CanBeCached(DbQueryInfo queryInfo, int rowCount);

        /// <summary>
        /// Gets the absolute expiration timeout for a given query.
        /// </summary>
        /// <param name="queryInfo">The query information.</param>
        /// <returns>Expiration timeout. <see cref="TimeSpan.MaxValue"/> for no expiration.</returns>
        TimeSpan GetExpirationTimeout(DbQueryInfo queryInfo);

        /// <summary>
        /// Gets the caching strategy for a give query.
        /// </summary>
        /// <param name="queryInfo">The query information.</param>
        /// <returns>Caching strategy for the query.</returns>
        DbCachingMode GetCachingMode(DbQueryInfo queryInfo);
    }
}