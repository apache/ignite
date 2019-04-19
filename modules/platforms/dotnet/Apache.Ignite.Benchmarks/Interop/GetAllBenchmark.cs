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

namespace Apache.Ignite.Benchmarks.Interop
{
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Benchmarks.Model;
    using Apache.Ignite.Core.Cache;

    /// <summary>
    /// Cache GetAll benchmark.
    /// </summary>
    internal class GetAllBenchmark : PlatformBenchmarkBase
    {
        /** Cache name. */
        private const string CacheName = "doubles";

        /** Native cache wrapper. */
        protected ICache<int, Doubles> Cache;

        /** GetAll batch size */
        protected const int DatasetBatchSize = 100;

        /** <inheritDoc /> */
        protected override void OnStarted()
        {
            Dataset = 1000;

            base.OnStarted();

            Cache = Node.GetOrCreateCache<int, Doubles>(CacheName);

            var array = Doubles.GetInstances(Dataset);

            for (int i = 0; i < array.Length; i++)
                Cache.Put(i, array[i]);
        }

        /** <inheritDoc /> */
        protected override void GetDescriptors(ICollection<BenchmarkOperationDescriptor> descs)
        {
            descs.Add(BenchmarkOperationDescriptor.Create("GetAll", GetAll, 1));
        }

        /// <summary>
        /// Cache GetAll.
        /// </summary>
        private void GetAll(BenchmarkState state)
        {
            var idx = BenchmarkUtils.GetRandomInt(Dataset - DatasetBatchSize);
            var keys = Enumerable.Range(idx, DatasetBatchSize);

            Cache.GetAll(keys);
        }
    }
}
