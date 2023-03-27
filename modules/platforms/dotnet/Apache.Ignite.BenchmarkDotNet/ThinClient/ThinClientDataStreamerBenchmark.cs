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
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using global::BenchmarkDotNet.Attributes;

    /// <summary>
    /// Thin vs Thick client data streamer benchmark.
    /// <para />
    /// Results on Core i7-9700K, Ubuntu 20.04, .NET 5.0.5:
    /// |            Method |     Mean |   Error |  StdDev | Ratio | RatioSD |     Gen 0 | Gen 1 | Gen 2 | Allocated |
    /// |------------------ |---------:|--------:|--------:|------:|--------:|----------:|------:|------:|----------:|
    /// |  StreamThinClient | 106.5 ms | 3.25 ms | 9.52 ms |  0.99 |    0.08 | 2000.0000 |     - |     - |  17.14 MB |
    /// | StreamThickClient | 109.7 ms | 2.19 ms | 4.17 ms |  1.00 |    0.00 | 2000.0000 |     - |     - |  13.61 MB |
    /// </summary>
    [MemoryDiagnoser]
    public class ThinClientDataStreamerBenchmark : ThinClientBenchmarkBase
    {
        /** */
        private const string CacheName = "c";

        /** */
        private const int EntryCount = 150000;

        /** */
        public IIgnite ThickClient { get; set; }

        /** */
        public ICache<int,int> Cache { get; set; }

        /** <inheritdoc /> */
        public override void GlobalSetup()
        {
            base.GlobalSetup();

            // 3 servers in total.
            Ignition.Start(Utils.GetIgniteConfiguration());
            Ignition.Start(Utils.GetIgniteConfiguration());

            ThickClient = Ignition.Start(Utils.GetIgniteConfiguration(client: true));

            Cache = ThickClient.CreateCache<int, int>(CacheName);
        }

        [IterationSetup]
        public void Setup()
        {
            Cache.Clear();
        }

        /// <summary>
        /// Benchmark: thin client streamer.
        /// </summary>
        [Benchmark]
        public void StreamThinClient()
        {
            using (var streamer = Client.GetDataStreamer<int, int>(CacheName))
            {
                for (var i = 0; i < EntryCount; i++)
                {
                    streamer.Add(i, -i);
                }
            }
        }

        /// <summary>
        /// Benchmark: thick client streamer.
        /// </summary>
        [Benchmark(Baseline = true)]
        public void StreamThickClient()
        {
            using (var streamer = ThickClient.GetDataStreamer<int, int>(CacheName))
            {
                for (var i = 0; i < EntryCount; i++)
                {
                    streamer.Add(i, -i);
                }
            }
        }
    }
}
