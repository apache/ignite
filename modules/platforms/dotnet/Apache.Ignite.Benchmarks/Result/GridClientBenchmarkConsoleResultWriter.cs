/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Benchmark.Result
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;

    using GridGain.Client.Benchmark;

    /// <summary>
    /// Benchmark console result writer.
    /// </summary>
    internal class GridClientBenchmarkConsoleResultWriter : IGridClientBenchmarkResultWriter
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
