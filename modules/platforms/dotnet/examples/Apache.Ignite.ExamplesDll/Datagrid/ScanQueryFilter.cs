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
    using Apache.Ignite.ExamplesDll.Binary;
    
    /// <summary>
    /// Filter for scan query example.
    /// </summary>
    [Serializable]
    public class ScanQueryFilter : ICacheEntryFilter<int, Employee>
    {
        /** Zip code to filter on. */
        private readonly int _zipCode;

        /// <summary>
        /// Initializes a new instance of the <see cref="ScanQueryFilter"/> class.
        /// </summary>
        /// <param name="zipCode">The zip code.</param>
        public ScanQueryFilter(int zipCode)
        {
            _zipCode = zipCode;
        }

        /// <summary>
        /// Returns a value indicating whether provided cache entry satisfies this predicate.
        /// </summary>
        public bool Invoke(ICacheEntry<int, Employee> entry)
        {
            return entry.Value.Address.Zip == _zipCode;
        }
    }
}
