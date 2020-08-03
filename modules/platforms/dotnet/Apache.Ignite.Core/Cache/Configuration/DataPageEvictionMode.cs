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

namespace Apache.Ignite.Core.Cache.Configuration
{
    using System;

    /// <summary>
    /// Memory page eviction mode.
    /// Only data pages, that store key-value entries, are eligible for eviction.
    /// The other types of pages, like index or system pages, are not evictable.
    /// </summary>
    [Obsolete("Use Apache.Ignite.Core.Configuration.DataPageEvictionMode")]
    public enum DataPageEvictionMode
    {
        /// <summary>
        /// Eviction is disabled.
        /// </summary>
        Disabled,

        /// <summary>
        /// Random-LRU algorithm.
        /// <para />
        /// Once a memory region defined by a memory policy is configured, an off-heap array is allocated to track
        /// last usage timestamp for every individual data page. The size of the array equals to
        /// <see cref="MemoryPolicyConfiguration.MaxSize"/> / <see cref="MemoryConfiguration.PageSize"/>.
        /// <para />
        /// When a data page is accessed, its timestamp gets updated in the tracking array. The page index in the
        /// tracking array equals to pageAddress / <see cref="MemoryPolicyConfiguration.MaxSize"/>.
        /// <para />
        /// When some pages need to be evicted, the algorithm randomly chooses 5 indexes from the tracking array and
        /// evicts a page with the latest timestamp. If some of the indexes point to non-data pages
        /// (index or system pages) then the algorithm picks other pages.
        /// </summary>
        RandomLru,

        /// <summary>
        /// Activates Random-2-LRU algorithm which is a scan resistant version of Random-LRU.
        /// <para />
        /// This algorithm differs from Random-LRU only in a way that two latest access timestamps are stored for every
        /// data page. At the eviction time, a minimum between two latest timestamps is taken for further
        /// comparison with minimums of other pages that might be evicted. LRU-2 outperforms LRU by
        /// resolving "one-hit wonder" problem - if a data page is accessed rarely, but accidentally accessed once,
        /// its protected from eviction for a long time.
        /// </summary>
        Random2Lru
    }
}