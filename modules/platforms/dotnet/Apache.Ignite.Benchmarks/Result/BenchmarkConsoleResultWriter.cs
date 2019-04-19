/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Benchmarks.Result
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;

    /// <summary>
    /// Benchmark console result writer.
    /// </summary>
    internal class BenchmarkConsoleResultWriter : IBenchmarkResultWriter
    {
        /** Cached culture. */
        private static readonly CultureInfo Culture = new CultureInfo("en-US");

        /** Benchmark. */
        private volatile BenchmarkBase _benchmark;

        /** <inheritdoc/> */
        public void Initialize(BenchmarkBase benchmark, ICollection<string> opNames)
        {
            _benchmark = benchmark;
        }

        /** <inheritdoc/> */
        public void WriteThroughput(string opName, long duration, long opCount)
        {
            opCount = opCount * _benchmark.BatchSize;

            double durSec = (double)duration / Stopwatch.Frequency;

            Console.WriteLine(opName + ": operations=" + opCount
                + ", duration=" + durSec.ToString("F2", Culture) + "s"
                + ", throughput=" + (opCount / durSec) + " (op/s)"
                + ", latency=" + (durSec * 1000 / opCount).ToString("F3", Culture) + "ms"
                );
        }

        /** <inheritdoc/> */
        public void WritePercentiles(string opName, long interval, long[] slots)
        {
            // No-op.
        }

        /** <inheritdoc/> */
        public void Commit()
        {
            // No-op.
        }
    }
}
