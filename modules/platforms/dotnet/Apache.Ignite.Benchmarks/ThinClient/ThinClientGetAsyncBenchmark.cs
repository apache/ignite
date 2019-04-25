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

namespace Apache.Ignite.Benchmarks.ThinClient
{
    using System.Collections.Generic;
    using Apache.Ignite.Benchmarks.Interop;
    using Apache.Ignite.Benchmarks.Model;
    using Apache.Ignite.Core.Client.Cache;

    /// <summary>
    /// Cache get async benchmark.
    /// </summary>
    internal class ThinClientGetAsyncBenchmark : PlatformBenchmarkBase
    {
        /** Cache name. */
        private const string CacheName = "cache";

        /** Native cache wrapper. */
        private ICacheClient<int, Employee> _cache;

        /** <inheritDoc /> */
        protected override void OnStarted()
        {
            base.OnStarted();

            _cache = GetClient().GetCache<int, Employee>(CacheName);

            for (int i = 0; i < Emps.Length; i++)
                _cache.Put(i, Emps[i]);
        }

        /** <inheritDoc /> */
        protected override void GetDescriptors(ICollection<BenchmarkOperationDescriptor> descs)
        {
            descs.Add(BenchmarkOperationDescriptor.Create("ThinClientGetAsync", GetAsync, 1));
        }

        /// <summary>
        /// Cache get.
        /// </summary>
        private void GetAsync(BenchmarkState state)
        {
            var idx = BenchmarkUtils.GetRandomInt(Dataset);

            _cache.GetAsync(idx).Wait();
        }
    }
}