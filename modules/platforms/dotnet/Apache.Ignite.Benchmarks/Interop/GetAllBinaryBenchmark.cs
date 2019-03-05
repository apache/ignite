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
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;

    /// <summary>
    /// Cache GetAll binary benchmark.
    /// </summary>
    internal class GetAllBinaryBenchmark : GetAllBenchmark
    {
        /** Binary cache wrapper. */
        private ICache<int, IBinaryObject> _binaryCache;

        /** <inheritDoc /> */
        protected override void OnStarted()
        {
            base.OnStarted();

            _binaryCache = Cache.WithKeepBinary<int, IBinaryObject>();
        }

        /** <inheritDoc /> */
        protected override void GetDescriptors(ICollection<BenchmarkOperationDescriptor> descs)
        {
            descs.Add(BenchmarkOperationDescriptor.Create("GetAllBinary", GetAllBinary, 1));
        }

        /// <summary>
        /// Cache GetAll.
        /// </summary>
        private void GetAllBinary(BenchmarkState state)
        {
            var idx = BenchmarkUtils.GetRandomInt(Dataset - DatasetBatchSize);
            var keys = Enumerable.Range(idx, DatasetBatchSize);

            _binaryCache.GetAll(keys);
        }
    }
}
