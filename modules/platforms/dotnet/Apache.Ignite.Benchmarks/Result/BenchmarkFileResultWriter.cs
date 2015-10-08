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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Threading;

    internal class BenchmarkFileResultWriter : IBenchmarkResultWriter
    {
        /** Probe file name: percentile. */
        private const string ProbePercentile = "PercentileProbe.csv";

        /** Probe file name: throughput. */
        private const string ProbeThroughput = "ThroughputLatencyProbe.csv";

        /** File header: percentile probe. */
        private const string HdrPercentile = "**\"Latency, microseconds\",\"Operations, %\"";

        /** File header: throughput probe. */

        private const string HdrThroughput =
            "**\"Time, sec\",\"Operations/sec (more is better)\",\"Latency, nsec (less is better)\"";

        /** Cached culture. */
        private static readonly CultureInfo Culture = new CultureInfo("en-US");

        /** Writer. */
        private readonly Writer _writer = new Writer();

        /** Benchmarks. */
        private volatile IDictionary<string, BenchmarkTask> _benchmarks;

        /** <inheritdoc/> */
        public void Initialize(BenchmarkBase benchmark, ICollection<string> opNames)
        {
            _benchmarks = new Dictionary<string, BenchmarkTask>(opNames.Count);

            // 1. Create folder for results.
            var now = DateTime.Now;

            var suffix = "-t=" + benchmark.Threads + "-d=" + benchmark.Duration +
                         "-w=" + benchmark.Warmup;

            var path = benchmark.ResultFolder + "\\" + now.ToString("yyyyMMdd-HHmmss", Culture) + "-" +
                       benchmark.GetType().Name + suffix;

            if (Directory.Exists(path))
                Directory.Delete(path, true);

            Directory.CreateDirectory(path);

            var dateStr = "--Created " + DateTime.Now.ToString("yyyyMMdd-HHmmss", Culture);
            var cfgStr = "--Benchmark config: " + benchmark;

            // 2. For each operation create separate folder and initialize probe files there.
            foreach (var opName in opNames)
            {
                var opDesc = benchmark.GetType().Name + "-" + opName + suffix;
                var opPath = path + "\\" + opDesc;

                Directory.CreateDirectory(opPath);

                var task = new BenchmarkTask(opPath + "\\" + ProbePercentile,
                    opPath + "\\" + ProbeThroughput);

                _benchmarks[opName] = task;

                File.AppendAllText(task.FilePercentile, dateStr + "\n");
                File.AppendAllText(task.FilePercentile, cfgStr + "\n");
                File.AppendAllText(task.FilePercentile, "--Description: " + opDesc + "\n");
                File.AppendAllText(task.FilePercentile, "@@" + benchmark.GetType().Name + "\n");
                File.AppendAllText(task.FilePercentile, HdrPercentile + "\n");

                File.AppendAllText(task.FileThroughput, dateStr + "\n");
                File.AppendAllText(task.FileThroughput, cfgStr + "\n");
                File.AppendAllText(task.FileThroughput, "--Description: " + opDesc + "\n");
                File.AppendAllText(task.FileThroughput, "@@" + benchmark.GetType().Name + "\n");
                File.AppendAllText(task.FileThroughput, HdrThroughput + "\n");
            }

            // 3. Start writer thread.
            new Thread(_writer.Run).Start();
        }

        /** <inheritdoc/> */
        public void WriteThroughput(string opName, long duration, long opCount)
        {
            var benchmark = _benchmarks[opName];

            if (!benchmark.FirstThroughput())
            {
                var sec = benchmark.Counter();

                var ops = (float) opCount;
                var latency = (float) duration*1000000000/(opCount*Stopwatch.Frequency);

                var text = sec + "," + ops.ToString("F2", Culture) + "," + latency.ToString("F2", Culture);

                Write0(benchmark.FileThroughput, text);
            }
        }

        /** <inheritdoc/> */
        public void WritePercentiles(string opName, long interval, long[] slots)
        {
            var benchmark = _benchmarks[opName];

            var total = slots.Sum();

            long time = 0;

            foreach (var slot in slots)
            {
                var val = (float) slot/total;

                Write0(benchmark.FilePercentile, time + "," + val.ToString("F2", Culture));

                time += interval;
            }
        }

        /** <inheritdoc/> */
        public void Commit()
        {
            _writer.Add(new StopTask().Run);

            _writer.AwaitStop();
        }

        /// <summary>
        /// Internal write routine.
        /// </summary>
        /// <param name="path">Path.</param>
        /// <param name="text">Text.</param>
        private void Write0(string path, string text)
        {
            _writer.Add(new WriteTask(path, text).Run);
        }

        /// <summary>
        /// Writer.
        /// </summary>
        private class Writer
        {
            /** Queue. */
            private readonly BlockingCollection<Task> _queue = new BlockingCollection<Task>();

            /** Stop flag. */
            private volatile bool _stop;

            /// <summary>
            /// Runner method.
            /// </summary>
            public void Run()
            {
                while (!_stop)
                {
                    var task = _queue.Take();

                    task.Invoke(this);
                }
            }

            /// <summary>
            /// Add task to queue.
            /// </summary>
            /// <param name="task">Task.</param>
            public void Add(Task task)
            {
                _queue.Add(task);
            }

            /// <summary>
            /// Mark writer stopped.
            /// </summary>
            public void Stop()
            {
                lock (this)
                {
                    _stop = true;

                    Monitor.PulseAll(this);
                }
            }

            /// <summary>
            /// Await for writer stop.
            /// </summary>
            /// <returns></returns>
            public void AwaitStop()
            {
                lock (this)
                {
                    while (!_stop)
                        Monitor.Wait(this);
                }
            }
        }

        /// <summary>
        /// Writer task.
        /// </summary>
        /// <param name="writer">Writer.</param>
        private delegate void Task(Writer writer);

        /// <summary>
        /// Writer write task.
        /// </summary>
        private class WriteTask
        {
            /** File name. */
            private readonly string _fileName;

            /** Text. */
            private readonly string _text;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="fileName">File name.</param>
            /// <param name="text">Text.</param>
            public WriteTask(string fileName, string text)
            {
                _fileName = fileName;
                _text = text;
            }

            /// <summary>
            /// Runner.
            /// </summary>
            public void Run(Writer writer)
            {
                File.AppendAllText(_fileName, _text + "\n");
            }
        }

        /// <summary>
        /// Writer stop task.
        /// </summary>
        private class StopTask
        {
            /// <summary>
            /// Runner.
            /// </summary>
            public void Run(Writer writer)
            {
                writer.Stop();
            }
        }

        /// <summary>
        /// Benchmark task.
        /// </summary>
        private class BenchmarkTask
        {
            /** Counter. */
            private int _ctr;

            /** First throughput flag. */
            private int _first;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="filePercentile">File percentile.</param>
            /// <param name="fileThroughput">File throughput.</param>
            public BenchmarkTask(string filePercentile, string fileThroughput)
            {
                FilePercentile = filePercentile;
                FileThroughput = fileThroughput;
            }

            /// <summary>
            /// File percentile.
            /// </summary>
            public string FilePercentile { get; private set; }

            /// <summary>
            /// File throughput.
            /// </summary>
            public string FileThroughput { get; private set; }

            /// <summary>
            /// Get counter value.
            /// </summary>
            /// <returns>Counter value.</returns>
            public int Counter()
            {
                return Interlocked.Increment(ref _ctr);
            }

            /// <summary>
            /// Check whether this is the first throughput task.
            /// </summary>
            /// <returns>True if first.</returns>
            public bool FirstThroughput()
            {
                return Interlocked.CompareExchange(ref _first, 1, 0) == 0;
            }
        }
    }
}
