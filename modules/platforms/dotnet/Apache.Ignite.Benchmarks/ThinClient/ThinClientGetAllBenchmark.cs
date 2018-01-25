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

namespace Apache.Ignite.Benchmarks.ThinClient
{
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Benchmarks.Interop;
    using Apache.Ignite.Benchmarks.Model;
    using Apache.Ignite.Core.Client.Cache;

    /// <summary>
    /// Cache GetAll benchmark.
    /// </summary>
    internal class ThinClientGetAllBenchmark : PlatformBenchmarkBase
    {
        /** Cache name. */
        private const string CacheName = "cache";

        /** Native cache wrapper. */
        protected ICacheClient<int, Employee> Cache;

        /** GetAll batch size */
        protected const int DatasetBatchSize = 100;

        /** <inheritDoc /> */
        protected override void OnStarted()
        {
            base.OnStarted();

            Cache = GetClient().GetCache<int, Employee>(CacheName);

            for (var i = 0; i < Emps.Length; i++)
                Cache.Put(i, Emps[i]);
        }

        /** <inheritDoc /> */
        protected override void GetDescriptors(ICollection<BenchmarkOperationDescriptor> descs)
        {
            descs.Add(BenchmarkOperationDescriptor.Create("ThinClientGetAll", GetAll, 1));
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
