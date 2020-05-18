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
    using Apache.Ignite.Core.Client;
    using global::BenchmarkDotNet.Attributes;

    /// <summary>
    /// Thin client compute benchmarks.
    /// </summary>
    public class ThinClientComputeBenchmark
    {
        /** */
        private const string TaskName = "org.apache.ignite.platform.PlatformComputeEchoTask";
        
        /** */
        private IIgnite _ignite;

        /** */
        private IIgniteClient _client;

        /// <summary>
        /// Sets up the benchmark.
        /// </summary>
        [GlobalSetup]
        public void GlobalSetup()
        {
            _ignite = Ignition.Start(Utils.GetIgniteConfiguration());
            _client = Ignition.StartClient(Utils.GetIgniteClientConfiguration());
        }

        /// <summary>
        /// Cleans up the benchmark.
        /// </summary>
        [GlobalCleanup]
        public void GlobalCleanup()
        {
            _client.Dispose();
            _ignite.Dispose();
        }
        
        /// <summary>
        /// Benchmark: simple Java task.
        /// </summary>
        [Benchmark]
        public void ExecuteJavaTask()
        {
            _client.GetCompute().ExecuteJavaTask<object>(TaskName, 0);
        }
    }
}