﻿/*
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

namespace Apache.Ignite.Linq
{
    using Apache.Ignite.Core;

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
        IIgnite Ignite { get; }

        /// <summary>
        /// Returns the commands to execute against the data source.
        /// </summary>
        /// <returns>Commands to execute against the data source.</returns>
        string ToTraceString();
    }
}