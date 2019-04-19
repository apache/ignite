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

namespace Apache.Ignite.Linq
{
    using System;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache.Query;

    /// <summary>
    /// Common interface for cache queryables.
    /// </summary>
    public interface ICacheQueryable
    {
        /// <summary>
        /// Gets the name of the cache that is associated with this query.
        /// </summary>
        /// <value>
        /// The name of the cache.
        /// </value>
        string CacheName { get; }

        /// <summary>
        /// Gets the Ignite instance associated with this query.
        /// </summary>
        [Obsolete("Deprecated, null for thin client.")]
        IIgnite Ignite { get; }

        /// <summary>
        /// Returns fields query that represents current queryable.
        /// </summary>
        /// <returns>Fields query that represents current queryable.</returns>
        SqlFieldsQuery GetFieldsQuery();

        /// <summary>
        /// Gets the type of the element.
        /// </summary>
        Type ElementType { get; }
    }
}