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
    using System;
    using Apache.Ignite.Core;
    using global::BenchmarkDotNet.Attributes;

    /// <summary>
    /// Data streamer benchmark.
    /// </summary>
    public class DataStreamerBenchmark
    {
        /** */
        private const string CacheName = "c";

        /** */
        private const int EntryCount = 50000;

        /** */
        private IIgnite Ignite { get; set; }

        /** */
        private IIgnite Client { get; set; }

        /// <summary>
        /// Sets up the benchmark.
        /// </summary>
        [GlobalSetup]
        public void GlobalSetup()
        {
            Ignite = Ignition.Start(Utils.GetIgniteConfiguration());
            Client = Ignition.Start(new IgniteConfiguration(Utils.GetIgniteConfiguration())
            {
                ClientMode = true,
                IgniteInstanceName = "Client"
            });

            Ignite.CreateCache<int, int>(CacheName);
        }

        /// <summary>
        /// Cleans up the benchmark.
        /// </summary>
        [GlobalCleanup]
        public void GlobalCleanup()
        {
            Client.Dispose();
            Ignite.Dispose();
        }

        /// <summary>
        /// Streamer benchmark.
        /// </summary>
        [Benchmark(Baseline = true)]
        public void Streamer()
        {
            StreamData(false);
        }

        /// <summary>
        /// Streamer benchmark.
        /// </summary>
        [Benchmark]
        public void StreamerAllowOverwrite()
        {
            StreamData(true);
        }

        /** */
        private void StreamData(bool allowOverwrite)
        {
            using (var streamer = Ignite.GetDataStreamer<int, Guid>(CacheName))
            {
                streamer.AllowOverwrite = allowOverwrite;

                for (var i = 0; i < EntryCount; i++)
                {
                    streamer.Add(i, Guid.NewGuid());
                }
            }
        }
    }
}
