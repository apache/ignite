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
    using System.Collections.Generic;

    /// <summary>
    /// Result writer interface.
    /// </summary>
    internal interface IBenchmarkResultWriter
    {
        /// <summary>
        /// Initialize writer.
        /// </summary>
        /// <param name="benchmark">Benchmark.</param>
        /// <param name="opNames">Operation names.</param>
        void Initialize(BenchmarkBase benchmark, ICollection<string> opNames);

        /// <summary>
        /// Write throughput results.
        /// </summary>
        /// <param name="opName">Operation name.</param>
        /// <param name="duration">Duration.</param>
        /// <param name="opCount">Operations count.</param>
        void WriteThroughput(string opName, long duration, long opCount);

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
