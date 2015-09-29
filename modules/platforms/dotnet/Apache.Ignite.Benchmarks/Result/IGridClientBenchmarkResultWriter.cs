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
    using System.Collections.Generic;

    using GridGain.Client.Benchmark;

    /// <summary>
    /// Result writer interface.
    /// </summary>
    internal interface IGridClientBenchmarkResultWriter
    {
        /// <summary>
        /// Initialize writer.
        /// </summary>
        /// <param name="ctx">Benchmark.</param>
        /// <param name="opNames">Operation names.</param>
        void Initialize(GridClientAbstractBenchmark benchmark, ICollection<string> opNames);

        /// <summary>
        /// Write throughput results.
        /// </summary>
        /// <param name="opName">Opeartion name.</param>
        /// <param name="threadCnt">Thread count.</param>
        /// <param name="dur">Duration.</param>
        /// <param name="cnt">Operations count.</param>
        void WriteThroughput(string opName, long dur, long cnt);

        /// <summary>
        /// Write percentile results.
        /// </summary>
        /// <param name="opName">Operation name.</param>
        /// <param name="interval">Slot interval.</param>
        /// <param name="slots">Slots.</param>
        void WritePercentiles(string opName, long interval, long[] slots);

        /// <summary>
        /// Commit all results releasing all associated resources.
        /// </summary>
        void Commit();
    }
}
