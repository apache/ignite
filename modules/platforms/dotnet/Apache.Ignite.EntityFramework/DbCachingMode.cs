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
    using System.Data.Entity;

    /// <summary>
    /// Represents a second-level caching strategy.
    /// </summary>
    public enum DbCachingMode
    {
        /// <summary>
        /// Read-only mode, never invalidates.
        /// <para />
        /// Database updates are ignored in this mode. Once query results have been cached, they are kept in cache 
        /// until expired (forever when no expiration is specified).
        /// <para />
        /// This mode is suitable for data that is not expected to change 
        /// (like a list of countries and other dictionary data).
        /// </summary>
        ReadOnly,

        /// <summary>
        /// Read-write mode. Cached data is invalidated when underlying entity set changes.
        /// <para />
        /// This is "normal" cache mode which always provides correct query results.
        /// <para />
        /// Keep in mind that this mode works correctly only when all database changes are performed 
        /// via <see cref="DbContext"/> with Ignite caching configured. Other database updates are not tracked.
        /// </summary>
        ReadWrite
    }
}
