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

namespace Apache.Ignite.BenchmarkDotNet.DataStreamer
{
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using global::BenchmarkDotNet.Attributes;

    public class DataStreamerBatchSizeBenchmark
    {
        private const int BaseCount = 1024;
        private const int Count = BaseCount * 100;

        public IIgnite Ignite { get; set; }

        public ICache<int, int> Cache { get; set; }

        /// <summary>
        /// Sets up the benchmark.
        /// </summary>
        [GlobalSetup]
        public virtual void GlobalSetup()
        {
            Ignite = Ignition.Start(Utils.GetIgniteConfiguration());
            Cache = Ignite.GetOrCreateCache<int, int>("c");
        }

        /// <summary>
        /// Cleans up the benchmark.
        /// </summary>
        [GlobalCleanup]
        public virtual void GlobalCleanup()
        {
            Ignite.Dispose();
        }

        [Benchmark]
        public void OneStreamerManyBatches()
        {
            using (var streamer = Ignite.GetDataStreamer<int, int>(Cache.Name))
            {
                for (int i = 0; i < Count; i++)
                {
                    streamer.Add(i, i);
                }
            }
        }

        [Benchmark]
        public void OneBatchManyStreamers()
        {
            const int batchSize = BaseCount * 10;

            for (int i = 0; i < (Count / batchSize); i++)
            {
                using (var streamer = Ignite.GetDataStreamer<int, int>(Cache.Name))
                {
                    var offs = i * batchSize;

                    for (int j = 0; j < batchSize; j++)
                    {
                        streamer.Add(offs + j, offs + j);
                    }
                }
            }
        }
    }
}
