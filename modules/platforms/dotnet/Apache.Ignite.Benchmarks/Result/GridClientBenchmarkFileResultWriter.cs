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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.Threading;

    using GridGain.Client.Benchmark;

    internal class GridClientBenchmarkFileResultWriter : IGridClientBenchmarkResultWriter
    {
        /** Probe file name: percentile. */
        private const string PROBE_PERCENTILE = "PercentileProbe.csv";

        /** Probe file name: throughput. */
        private const string PROBE_THROUGHPUT = "ThroughputLatencyProbe.csv";

        /** File header: percentile probe. */
        private const string HDR_PERCENTILE = "**\"Latency, microseconds\",\"Operations, %\"";

        /** File header: throughput probe. */
        private const string HDR_THROUGHPUT = "**\"Time, sec\",\"Operations/sec (more is better)\",\"Latency, nsec (less is better)\"";

        /** Cached culture. */
        private static readonly CultureInfo CULTURE = new CultureInfo("en-US");

        /** Writer. */
        private readonly Writer writer = new Writer();

        /** Benchmarks. */
        private volatile IDictionary<string, BenchmarkTask> benchmarks;

        /** Amount of threads. */
        private volatile int threadCnt;

        /** <inheritdoc/> */
        public void Initialize(GridClientAbstractBenchmark benchmark, ICollection<string> opNames)
        {
            threadCnt = benchmark.Threads;

            benchmarks = new Dictionary<string, BenchmarkTask>(opNames.Count);

            // 1. Create folder for results.
            DateTime now = DateTime.Now;

            String suffix = "-t=" + benchmark.Threads + "-d=" + benchmark.Duration +
                "-w=" + benchmark.Warmup;

            String path = benchmark.ResultFolder + "\\" + now.ToString("yyyyMMdd-HHmmss", CULTURE) + "-" +
                benchmark.GetType().Name + suffix;

            if (Directory.Exists(path))
                Directory.Delete(path, true);

            Directory.CreateDirectory(path);

            String dateStr = "--Created " + DateTime.Now.ToString("yyyyMMdd-HHmmss", CULTURE);
            String cfgStr = "--Benchmark config: " + benchmark;

            // 2. For each operation create separate folder and initialize probe files there.
            foreach (string opName in opNames)
            {
                String opDesc = benchmark.GetType().Name + "-" + opName + suffix;
                String opPath = path + "\\" + opDesc;

                Directory.CreateDirectory(opPath);

                BenchmarkTask task = new BenchmarkTask(opPath + "\\" + PROBE_PERCENTILE,
                    opPath + "\\" + PROBE_THROUGHPUT);

                benchmarks[opName] = task;

                File.AppendAllText(task.FilePercentile, dateStr + "\n");
                File.AppendAllText(task.FilePercentile, cfgStr + "\n");
                File.AppendAllText(task.FilePercentile, "--Description: " + opDesc + "\n");
                File.AppendAllText(task.FilePercentile, "@@" + benchmark.GetType().Name + "\n");
                File.AppendAllText(task.FilePercentile, HDR_PERCENTILE + "\n");

                File.AppendAllText(task.FileThroughput, dateStr + "\n");
                File.AppendAllText(task.FileThroughput, cfgStr + "\n");
                File.AppendAllText(task.FileThroughput, "--Description: " + opDesc + "\n");
                File.AppendAllText(task.FileThroughput, "@@" + benchmark.GetType().Name + "\n");
                File.AppendAllText(task.FileThroughput, HDR_THROUGHPUT + "\n");
            }

            // 3. Start writer thread.
            new Thread(writer.Run).Start();
        }

        /** <inheritdoc/> */
        public void WriteThroughput(string opName, long dur, long cnt)
        {
            BenchmarkTask benchmark = benchmarks[opName];

            if (!benchmark.FirstThroughput())
            {
                int sec = benchmark.Counter();

                float ops = (float)cnt;
                float latency = (float)dur * 1000000000 / (cnt * Stopwatch.Frequency);

                string text = sec + "," + ops.ToString("F2", CULTURE) + "," + latency.ToString("F2", CULTURE);

                Write0(benchmark.FileThroughput, text);
            }
        }

        /** <inheritdoc/> */
        public void WritePercentiles(string opName, long interval, long[] slots)
        {
            BenchmarkTask benchmark = benchmarks[opName];

            long total = 0;

            foreach (long slot in slots)
                total += slot;

            long time = 0;

            foreach (long slot in slots)
            {
                float val = (float)slot / total;

                Write0(benchmark.FilePercentile, time + "," + val.ToString("F2", CULTURE));

                time += interval;
            }
        }

        /** <inheritdoc/> */
        public void Commit()
        {
            writer.Add(new StopTask().Run);

            writer.AwaitStop();
        }

        /// <summary>
        /// Internal write routine.
        /// </summary>
        /// <param name="path">Path.</param>
        /// <param name="text">Text.</param>
        private void Write0(string path, string text)
        {
            writer.Add(new WriteTask(path, text).Run);
        }

        /// <summary>
        /// Writer.
        /// </summary>
        private class Writer
        {
            /** Queue. */
            private readonly BlockingCollection<Task> queue = new BlockingCollection<Task>();

            /** Stop flag. */
            private volatile bool stop;

            /// <summary>
            /// Runner method.
            /// </summary>
            public void Run()
            {
                while (!stop)
                {
                    Task task = queue.Take();

                    task.Invoke(this);
                }
            }

            /// <summary>
            /// Add task to queue.
            /// </summary>
            /// <param name="task">Task.</param>
            public void Add(Task task)
            {
                queue.Add(task);
            }

            /// <summary>
            /// Mark writer stopped.
            /// </summary>
            public void Stop()
            {
                lock (this)
                {
                    stop = true;

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
                    while (!stop)
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
            private readonly string fileName;

            /** Text. */
            private readonly string text;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="fileName">File name.</param>
            /// <param name="text">Text.</param>
            public WriteTask(string fileName, string text)
            {
                this.fileName = fileName;
                this.text = text;
            }

            /// <summary>
            /// Runner.
            /// </summary>
            public void Run(Writer writer)
            {
                File.AppendAllText(fileName, text + "\n");
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
            private int ctr;

            /** First throughput flag. */
            private int first;

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
            public string FilePercentile
            {
                get;
                private set;
            }

            /// <summary>
            /// File throughput.
            /// </summary>
            public string FileThroughput
            {
                get;
                private set;
            }

            /// <summary>
            /// Get counter value.
            /// </summary>
            /// <returns>Counter value.</returns>
            public int Counter()
            {
                return Interlocked.Increment(ref ctr);
            }

            /// <summary>
            /// Check whether this is the first throughput task.
            /// </summary>
            /// <returns>True if first.</returns>
            public bool FirstThroughput()
            {
                return Interlocked.CompareExchange(ref first, 1, 0) == 0;
            }
        }
    }
}
