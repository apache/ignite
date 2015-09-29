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

namespace Apache.Ignite.Benchmarks.Result
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;

    /// <summary>
    /// Benchmark console result writer.
    /// </summary>
    internal class GridClientBenchmarkConsoleResultWriter : IIgniteClientBenchmarkResultWriter
    {
        /** Cached culture. */
        private static readonly CultureInfo CULTURE = new CultureInfo("en-US");

        /** Benchmark. */
        private volatile GridClientAbstractBenchmark benchmark;

        /** <inheritdoc/> */
        public void Initialize(GridClientAbstractBenchmark benchmark, ICollection<string> opNames)
        {
            this.benchmark = benchmark;
        }

        /** <inheritdoc/> */
        public void WriteThroughput(string opName, long dur, long cnt)
        {
            cnt = cnt * benchmark.BatchSize;

            double durSec = (double)dur / Stopwatch.Frequency;

            Console.WriteLine(opName + ": operations=" + cnt
                + ", duration=" + durSec.ToString("F2", CULTURE) + "s"
                + ", throughput=" + (cnt / durSec) + " (op/s)"
                + ", latency=" + (durSec * 1000 / cnt).ToString("F3", CULTURE) + "ms"
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
