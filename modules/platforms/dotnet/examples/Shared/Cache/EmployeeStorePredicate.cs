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

namespace Apache.Ignite.Examples.Shared.Cache
{
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Examples.Shared.Models;

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
