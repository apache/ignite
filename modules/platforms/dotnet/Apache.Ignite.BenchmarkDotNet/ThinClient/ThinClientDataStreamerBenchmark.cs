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
    /// Results on Core i7-9700K, Ubuntu 20.04, .NET Core 5.0.5:
    /// Thin Client: new streamer for every batch.
    /// |            Method |     Mean |   Error |  StdDev | Ratio | RatioSD |     Gen 0 |     Gen 1 | Gen 2 | Allocated |
    /// |------------------ |---------:|--------:|--------:|------:|--------:|----------:|----------:|------:|----------:|
    /// |  StreamThinClient | 104.7 ms | 3.24 ms | 9.54 ms |  0.95 |    0.09 | 5000.0000 | 1000.0000 |     - |  29.54 MB |
    /// | StreamThickClient | 112.0 ms | 2.22 ms | 4.54 ms |  1.00 |    0.00 | 2000.0000 |         - |     - |  13.61 MB |
    /// After per-node buffers and non-blocking flush:
    /// |            Method |     Mean |   Error |  StdDev | Ratio | RatioSD |     Gen 0 |     Gen 1 | Gen 2 | Allocated |
    /// |------------------ |---------:|--------:|--------:|------:|--------:|----------:|----------:|------:|----------:|
    /// |  StreamThinClient | 107.0 ms | 3.02 ms | 8.80 ms |  0.99 |    0.09 | 4000.0000 | 1000.0000 |     - |  24.98 MB |
    /// | StreamThickClient | 110.2 ms | 2.16 ms | 3.89 ms |  1.00 |    0.00 | 2000.0000 |         - |     - |  13.61 MB |
    /// Wait for prev batch completion:
    /// |            Method |     Mean |   Error |  StdDev | Ratio | RatioSD |     Gen 0 |     Gen 1 | Gen 2 | Allocated |
    /// |------------------ |---------:|--------:|--------:|------:|--------:|----------:|----------:|------:|----------:|
    /// |  StreamThinClient | 102.8 ms | 2.92 ms | 8.52 ms |  0.96 |    0.08 | 4000.0000 | 1000.0000 |     - |  25.11 MB |
    /// | StreamThickClient | 109.2 ms | 2.17 ms | 4.07 ms |  1.00 |    0.00 | 2000.0000 |         - |     - |  13.61 MB |
    /// Semaphore with 8x CPUs (1x CPUs is 13% slower):
    /// |           Method |     Mean |   Error |   StdDev |     Gen 0 |     Gen 1 | Gen 2 | Allocated |
    /// |----------------- |---------:|--------:|---------:|----------:|----------:|------:|----------:|
    /// | StreamThinClient | 104.1 ms | 3.46 ms | 10.04 ms | 4000.0000 | 1000.0000 |     - |  25.11 MB |
    /// Semaphore with 4x CPUs, CAS buffers
    /// |           Method |     Mean |   Error |   StdDev |     Gen 0 |     Gen 1 | Gen 2 | Allocated |
    /// |----------------- |---------:|--------:|---------:|----------:|----------:|------:|----------:|
    /// | StreamThinClient | 105.4 ms | 3.26 ms | 9.50 ms | 4000.0000 | 1000.0000 |     - |  25.17 MB |
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
        // [Benchmark(Baseline = true)]
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
