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

namespace Apache.Ignite.BenchmarkDotNet.ThinClient
{
    using Apache.Ignite.Core.Client.Cache;
    using global::BenchmarkDotNet.Attributes;

    /// <summary>
    /// Cache get benchmarks.
    /// <para />
    /// |   Method |     Mean |    Error |   StdDev | Ratio | RatioSD |
    /// |--------- |---------:|---------:|---------:|------:|--------:|
    /// |      Get | 25.97 us | 0.514 us | 0.953 us |  1.00 |    0.00 |
    /// | GetAsync | 32.90 us | 0.638 us | 0.935 us |  1.27 |    0.06 |
    /// </summary>
    public class ThinClientCacheGetBenchmark : ThinClientBenchmarkBase
    {
        /** */
        private ICacheClient<int, int> _cache;
        
        /** <inheritdoc /> */
        public override void GlobalSetup()
        {
            base.GlobalSetup();

            _cache = Client.GetOrCreateCache<int, int>("c");
            _cache[1] = 1;
        }

        /// <summary>
        /// Get benchmark.
        /// </summary>
        [Benchmark(Baseline = true)]
        public void Get()
        {
            _cache.Get(1);
        }

        /// <summary>
        /// GetAsync benchmark.
        /// </summary>
        [Benchmark]
        public void GetAsync()
        {
            _cache.GetAsync(1).Wait();
        }
    }
}