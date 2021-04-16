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

namespace Apache.Ignite.Benchmarks.Interop
{
    using System.Collections.Generic;
    using Apache.Ignite.Benchmarks.Model;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Query;

    /// <summary>
    /// Cache GetAll benchmark.
    /// </summary>
    internal sealed class ScanQueryBenchmark : PlatformBenchmarkBase
    {
        /** Cache. */
        private ICache<int, Employee> _cache;

        /** Cache with platform cache enabled. */
        private ICache<int, Employee> _cacheWithPlatform;

        /** <inheritDoc /> */
        protected override void OnStarted()
        {
            base.OnStarted();

            _cache = Node.GetCache<int, Employee>("cache");
            _cacheWithPlatform = Node.GetCache<int, Employee>("cachePlatform");

            for (var i = 0; i < Emps.Length; i++)
            {
                _cache.Put(i, Emps[i]);
                _cacheWithPlatform.Put(i, Emps[i]);
            }
        }
        
        /** <inheritDoc /> */
        protected override void GetDescriptors(ICollection<BenchmarkOperationDescriptor> descs)
        {
            descs.Add(BenchmarkOperationDescriptor.Create("ScanQueryMatchNone", _ => Scan(_cache, false), 1));
            descs.Add(BenchmarkOperationDescriptor.Create("ScanQueryMatchAll", _ => Scan(_cache, true), 1));
            descs.Add(BenchmarkOperationDescriptor.Create("ScanQueryPlatformMatchNone", _ => Scan(_cacheWithPlatform, false), 1));
            descs.Add(BenchmarkOperationDescriptor.Create("ScanQueryPlatformMatchAll", _ => Scan(_cacheWithPlatform, true), 1));
        }

        /// <summary>
        /// Scan.
        /// </summary>
        private static void Scan(ICache<int, Employee> cache, bool shouldMatch)
        {
            var filter = new Filter {ShouldMatch = shouldMatch};
            cache.Query(new ScanQuery<int, Employee>(filter)).GetAll();
        }
        
        /// <summary>
        /// Scan query filter.
        /// </summary>
        private class Filter : ICacheEntryFilter<int, Employee>
        {
            /// <summary>
            /// Gets or sets a value indicating whether this filter should match all entries or none.
            /// </summary>
            public bool ShouldMatch { get; set; }

            /** <inheritdoc /> */
            public bool Invoke(ICacheEntry<int, Employee> entry)
            {
                return entry.Value.Age > int.MinValue && ShouldMatch;
            }
        }
    }
}