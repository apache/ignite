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
    using Apache.Ignite.Core.Client.Datastream;
    using Apache.Ignite.Core.Datastream;
    using global::BenchmarkDotNet.Attributes;

    /// <summary>
    /// Thin vs Thick client data streamer flush benchmark:
    /// measure warmed-up streamer flush performance, excluding open/close.
    /// <para />
    /// Results on Core i7-9700K, Ubuntu 20.04, .NET Core 5.0.5:
    /// </summary>
    [MemoryDiagnoser]
    public class ThinClientDataStreamerFlushBenchmark : ThinClientBenchmarkBase
    {
        /** */
        private const string CacheName = "c";

        /** */
        private const int EntryCount = 250000;

        /** */
        public IIgnite ThickClient { get; set; }

        /** */
        public ICache<int,int> Cache { get; set; }

        /** */
        public IDataStreamer<int,int> ThickStreamer { get; set; }

        /** */
        public IDataStreamerClient<int,int> ThinStreamer { get; set; }

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
            // Create streamers and warm them up.
            ThinStreamer = Client.GetDataStreamer<int, int>(CacheName);
            ThickStreamer = ThickClient.GetDataStreamer<int, int>(CacheName);

            for (int i = 0; i < DataStreamerDefaults.DefaultPerNodeBufferSize * 10; i++)
            {
                ThinStreamer.Add(-i, -i);
                ThickStreamer.Add(i, i);
            }

            ThickStreamer.Flush();
            ThinStreamer.Flush();

            Cache.Clear();
        }

        [IterationCleanup]
        public void Cleanup()
        {
            ThickStreamer.Dispose();
            ThinStreamer.Dispose();
        }

        /// <summary>
        /// Benchmark: thin client streamer.
        /// </summary>
        [Benchmark]
        public void StreamThinClient()
        {
            for (var i = 0; i < EntryCount; i++)
            {
                ThinStreamer.Add(i, -i);
            }

            ThinStreamer.Flush();
        }

        /// <summary>
        /// Benchmark: thick client streamer.
        /// </summary>
        [Benchmark(Baseline = true)]
        public void StreamThickClient()
        {
            for (var i = 0; i < EntryCount; i++)
            {
                ThickStreamer.Add(i, -i);
            }

            ThickStreamer.Flush();
        }
    }
}
