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

namespace Apache.Ignite.ExamplesDll.Datagrid
{
    using System;
    using Apache.Ignite.Core.Cache;

    /// <summary>
    /// EntryProcessor that creates new cache entry.
    /// </summary>
    [Serializable]
    public class CachePutEntryProcessor : ICacheEntryProcessor<int, int, int, object>
    {
        /// <summary>
        /// Process an entry.
        /// </summary>
        /// <param name="entry">The entry to process.</param>
        /// <param name="arg">The argument.</param>
        /// <returns>
        /// Processing result.
        /// </returns>
        public object Process(IMutableCacheEntry<int, int> entry, int arg)
        {
            if (!entry.Exists)
                entry.Value = entry.Key * arg;

            return null;
        }
    }
}
