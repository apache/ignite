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
    using System.Threading;
    using Apache.Ignite.BenchmarkDotNet.Models;
    using Apache.Ignite.Core.Client.Cache;
    using global::BenchmarkDotNet.Attributes;

    /// <summary>
    /// Cache put benchmarks.
    /// <para />
    /// |                Method |     Mean |    Error |   StdDev | Ratio | RatioSD |
    /// |---------------------- |---------:|---------:|---------:|------:|--------:|
    /// |          PutPrimitive | 45.47 us | 0.907 us | 1.330 us |  1.00 |    0.00 |
    /// |     PutPrimitiveAsync | 66.28 us | 1.317 us | 1.617 us |  1.45 |    0.05 |
    /// |  PutClassWithIntField | 68.34 us | 1.275 us | 1.130 us |  1.49 |    0.05 |
    /// | PutClassWithEnumField | 71.13 us | 1.419 us | 2.734 us |  1.57 |    0.08 |
    /// </summary>
    public class ThinClientCachePutBenchmark : ThinClientBenchmarkBase
    {
        /** */
        private ICacheClient<int, object> _cache;

        /** */
        private static int _counter;
        
        /** <inheritdoc /> */
        public override void GlobalSetup()
        {
            base.GlobalSetup();

            _cache = Client.GetOrCreateCache<int, object>("c");
        }

        /// <summary>
        /// Put primitive benchmark.
        /// </summary>
        [Benchmark(Baseline = true)]
        public void PutPrimitive()
        {
            _cache.Put(1, 1);
        }

        /// <summary>
        /// PutAsync benchmark.
        /// </summary>
        [Benchmark]
        public void PutPrimitiveAsync()
        {
            _cache.PutAsync(1, GetNextInt()).Wait();
        }

        /// <summary>
        /// Class with int field benchmark.
        /// </summary>
        [Benchmark]
        public void PutClassWithIntField()
        {
            var obj = new ClassWithIntField
            {
                Foo = GetNextInt()
            };
            
            _cache.PutAsync(1, obj).Wait();
        }

        /// <summary>
        /// Class with int field benchmark.
        /// </summary>
        [Benchmark]
        public void PutClassWithEnumField()
        {
            var obj = new ClassWithEnumField
            {
                BenchmarkEnum = (BenchmarkEnum)(GetNextInt() % 3)
            };
            
            _cache.PutAsync(1, obj).Wait();
        }

        /// <summary>
        /// Gets the next integer.
        /// </summary>
        private static int GetNextInt()
        {
            return Interlocked.Increment(ref _counter);
        }
    }
}