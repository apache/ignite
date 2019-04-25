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
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.ExamplesDll.Binary;

    /// <summary>
    /// Example cache entry predicate.
    /// </summary>
    public class EmployeeStorePredicate : ICacheEntryFilter<int, Employee>
    {
        /// <summary>
        /// Returns a value indicating whether provided cache entry satisfies this predicate.
        /// </summary>
        /// <param name="entry">Cache entry.</param>
        /// <returns>Value indicating whether provided cache entry satisfies this predicate.</returns>
        public bool Invoke(ICacheEntry<int, Employee> entry)
        {
            return entry.Key == 1;
        }
    }
}