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

namespace GridGain.Client.Benchmark
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Reflection;
    using System.Threading;
    using System.Text;

    using GridGain.Client.Benchmark;
    using GridGain.Client.Benchmark.Result;

    using BU = GridGain.Client.Benchmark.GridClientBenchmarkUtils;

    /// <summary>
    /// Benchmark base class.
    /// </summary>
    internal abstract class GridClientAbstractBenchmark
    {
        /** Result writer type: console. */
        protected const string RES_WRITER_CONSOLE = "console";

        /** Result writer type: file. */
        protected const string RES_WRITER_FILE = "file";
        
        /** Default duration. */
        private const int DFLT_DUR = 60;

        /** Default maximum errors count. */
        private const int DFLT_MAX_ERRS = 100;

        /** Default percentile result buckets count. */
        private const int DFLT_RES_BUCKET_CNT = 10000;

        /** Default percentile result bucket interval. */
        private const int DFLT_RES_BUCKET_INTERVAL = 100;

        /** Default batch size. */
        private const int DFLT_BATCH_SIZE = 1;

        /** Default result wrier. */
        private const string DFLT_RES_WRITER = RES_WRITER_CONSOLE;
        
        /** Start flag. */
        private volatile bool start;

        /** Stop flag. */
        private volatile bool stop;

        /** Warmup flag. */
        private volatile bool warmup = true;

        /** Ready threads. */
        private int readyThreads;

        /** Finished threads. */
        private volatile int finishedThreads;

        /** Descriptors. */
        private volatile ICollection<GridClientBenchmarkOperationDescriptor> descs;

        /** Benchmark tasks. */
        private volatile ICollection<BenchmarkTask> tasks;

        /** Percentile results. */
        private volatile IDictionary<string, long[]> percentiles;
        
        /** Currently completed operations. */
        private long curOps;
        
        /** Error count. */
        private long errs;

        /** Watches to count total execution time. */
        private readonly Stopwatch totalWatch = new Stopwatch();

        /** Warmup barrier. */
        private Barrier barrier;

        /** Benchmark result writer. */
        private IIgniteClientBenchmarkResultWriter writer;

        /// <summary>
        /// Default constructor.
        /// </summary>
        protected GridClientAbstractBenchmark()
        {
            Duration = DFLT_DUR;
            MaxErrors = DFLT_MAX_ERRS;
            ResultBucketCount = DFLT_RES_BUCKET_CNT;
            ResultBucketInterval = DFLT_RES_BUCKET_INTERVAL;
            BatchSize = DFLT_BATCH_SIZE;
            ResultWriter = DFLT_RES_WRITER;
        }

        /// <summary>
        /// Run the benchmark.
        /// </summary>
        public void Run()
        {
            PrintDebug("Started benchmark: " + this);

            ValidateArguments();

            if (ResultWriter.ToLower().Equals(RES_WRITER_CONSOLE))
                writer = new GridClientBenchmarkConsoleResultWriter();
            else
                writer = new GridClientBenchmarkFileResultWriter();

            OnStarted();

            PrintDebug("Benchmark setup finished.");

            try
            {
                descs = new List<GridClientBenchmarkOperationDescriptor>();

                Descriptors(descs);

                if (descs.Count == 0)
                    throw new Exception("No tasks provided for benchmark.");

                // Initialize writer.
                ICollection<string> opNames = new List<string>(descs.Count);

                foreach (GridClientBenchmarkOperationDescriptor desc in descs)
                    opNames.Add(desc.Name);

                PrintDebug(() =>
                {
                    StringBuilder sb = new StringBuilder("Operations: ");

                    foreach (string opName in opNames)
                        sb.Append(opName).Append(" ");

                    return sb.ToString();
                });

                writer.Initialize(this, opNames);

                PrintDebug("Initialized result writer.");

                // Start worker threads.
                tasks = new List<BenchmarkTask>(Threads);

                PrintDebug("Starting worker threads: " + Threads);

                for (int i = 0; i < Threads; i++)
                {
                    BenchmarkTask task = new BenchmarkTask(this, descs);

                    tasks.Add(task);

                    new Thread(task.Run).Start();
                }

                PrintDebug("Waiting worker threads to start: " + Threads);

                // Await for all threads to start in spin loop.
                while (Thread.VolatileRead(ref readyThreads) < Threads)
                    Thread.Sleep(10);

                PrintDebug("Worker threads started: " + Threads);

                // Start throughput writer thread.
                Thread writerThread = new Thread(new ThroughputTask(this).Run);

                writerThread.IsBackground = true;

                writerThread.Start();

                PrintDebug("Started throughput writer thread.");

                // Start warmup thread if needed.
                if (Warmup > 0)
                {
                    Thread thread = new Thread(new WarmupTask(this, Warmup).Run);

                    thread.IsBackground = true;

                    thread.Start();

                    PrintDebug("Started warmup timeout thread: " + Warmup);
                }
                else
                    this.warmup = false;

                barrier = new Barrier(Threads, (b) =>
                {
                    Console.WriteLine("Warmup finished.");

                    totalWatch.Start();
                });

                // Start timeout thread if needed.
                if (Duration > 0)
                {
                    if (Operations > 0)
                        PrintDebug("Duration argument is ignored because operations number is set: " +
                            Operations);
                    else
                    {
                        Thread thread = new Thread(new TimeoutTask(this, Warmup + Duration).Run);

                        thread.IsBackground = true;

                        thread.Start();

                        PrintDebug("Started duration timeout thread: " + Duration);
                    }
                }

                // Let workers start execution.
                start = true;

                // Await workers completion.
                PrintDebug("Awaiting worker threads completion.");

                Monitor.Enter(this);

                try
                {
                    while (finishedThreads < Threads)
                        Monitor.Wait(this);
                }
                finally
                {
                    Monitor.Exit(this);
                }

                PrintDebug("Worker threads completed.");
            }
            finally
            {
                OnFinished();

                totalWatch.Stop();

                PrintDebug("Tear down invoked.");

                if (PrintThroughputInfo())
                {
                    long avgThroughput = totalWatch.ElapsedMilliseconds == 0 ?
                        0 : this.curOps * 1000 / totalWatch.ElapsedMilliseconds;

                    double avgLatency = this.curOps == 0 ?
                        0 : (double)totalWatch.ElapsedMilliseconds * Threads / this.curOps;

                    Console.WriteLine("Finishing benchmark [name=" + GetType().Name +
                        ", time=" + totalWatch.ElapsedMilliseconds +
                        "ms, ops=" + this.curOps +
                        ", threads=" + Threads +
                        ", avgThroughput=" + avgThroughput +
                        ", avgLatency=" + String.Format("{0:0.000}ms", avgLatency) + ']');
                }
                else
                {
                    Console.WriteLine("Finishing benchmark [name=" + GetType().Name +
                        ", time=" + totalWatch.ElapsedMilliseconds +
                        "ms, ops=" + Operations +
                        ", threads=" + Threads + ']');
                }
            }

            percentiles = new Dictionary<string, long[]>(descs.Count);

            foreach (GridClientBenchmarkOperationDescriptor desc in descs)
                percentiles[desc.Name] = new long[ResultBucketCount];

            foreach (BenchmarkTask task in tasks)
                task.CollectPercentiles(percentiles);

            foreach (KeyValuePair<string, long[]> percentile in percentiles)
                writer.WritePercentiles(percentile.Key, ResultBucketInterval, percentile.Value);

            writer.Commit();

            PrintDebug("Results committed to output writer.");
        }

        /// <summary>
        /// Consumes passed argument.
        /// </summary>
        /// <param name="name">Argument name.</param>
        /// <param name="val">Value.</param>
        /// <returns>True if argument was consumed.</returns>
        public bool Configure(string name, string val)
        {
            PropertyInfo prop = BU.FindProperty(this, name);

            if (prop != null)
            {
                BU.SetProperty(this, prop, val);

                return true;
            }
            else
                return false;
        }

        /// <summary>
        /// Start callback.
        /// </summary>
        protected virtual void OnStarted()
        {
            // No-op.
        }

        /// <summary>
        /// Warmup finished callback. Executed by each worker thread once.
        /// </summary>
        protected virtual void OnWarmupFinished()
        {
            // No-op.
        }

        /// <summary>
        /// Batch execution started callback.
        /// </summary>
        /// <param name="state">State.</param>
        protected virtual void OnBatchStarted(GridClientBenchmarkState state)
        {
            // No-op.
        }

        /// <summary>
        /// Batch execution finished callback.
        /// </summary>
        /// <param name="state">State.</param>
        /// <param name="dur">Duration.</param>
        /// <returns>True if this result must be counted.</returns>
        protected virtual bool OnBatchFinished(GridClientBenchmarkState state, long dur)
        {
            return true;
        }

        /// <summary>
        /// Benchmarh finished callback.
        /// </summary>
        protected virtual void OnFinished()
        {
            // No-op.
        }        

        /// <returns>Flag indicating whether benchmark should print throughput information.</returns>
        protected virtual bool PrintThroughputInfo()
        {
            return true;
        }

        /// <summary>
        /// Internal arguments validation routine.
        /// </summary>
        /// <returns>True if base class must validate common arguments, false otherwise.</returns>
        protected virtual bool ValidateArgumentsEx()
        {
            return true;
        }

        /// <summary>
        /// Print debug to console.
        /// </summary>
        /// <param name="msg">Message</param>
        protected void PrintDebug(string msg)
        {
            if (Debug)
                Console.WriteLine("[DEBUG] " + Thread.CurrentThread.ManagedThreadId + ": " + msg);
        }

        /// <summary>
        /// Print debug to console.
        /// </summary>
        /// <param name="msg">Message delegate.</param>
        protected void PrintDebug(Func<string> msgFunc)
        {
            if (Debug)
                PrintDebug(msgFunc.Invoke());
        }

        /// <summary>
        /// Add operation descriptors.
        /// </summary>
        /// <param name="descs">Collection where operation descriptors must be added.</param>
        protected abstract void Descriptors(ICollection<GridClientBenchmarkOperationDescriptor> descs);

        /// <summary>
        /// Invoked when single thread is ready to actual execution.
        /// </summary>
        private void OnThreadReady()
        {
            Interlocked.Increment(ref readyThreads);
        }

        /// <summary>
        /// Invoked when single thread finished execution.
        /// </summary>
        private void OnThreadFinished()
        {
            Monitor.Enter(this);

            try
            {
                finishedThreads++;

                Monitor.PulseAll(this);
            }
            finally
            {
                Monitor.Exit(this);
            }

            PrintDebug("Worker thread finished.");
        }

        /// <summary>
        /// Validate arguments.
        /// </summary>
        private void ValidateArguments()
        {
            if (ValidateArgumentsEx())
            {
                if (Threads <= 0)
                    throw new Exception("Threads must be positive: " + Threads);

                if (Warmup < 0)
                    throw new Exception("Warmup cannot be negative: " + Warmup);

                if (Duration < 0)
                    throw new Exception("Duration cannot be negative: " + Duration);

                if (BatchSize <= 0)
                    throw new Exception("BatchSize must be positive: " + BatchSize);

                if (MaxErrors < 0)
                    throw new Exception("MaxErrors cannot be negative: " + MaxErrors);

                if (ResultWriter == null || !ResultWriter.ToLower().Equals(RES_WRITER_CONSOLE) 
                    && !ResultWriter.ToLower().Equals(RES_WRITER_FILE))
                    throw new Exception("Invalid ResultWriter: " + ResultWriter);

                if (ResultWriter.ToLower().Equals(RES_WRITER_FILE) && ResultFolder == null)
                    throw new Exception("ResultFolder must be set for file result writer.");

                if (ResultBucketCount <= 0)
                    throw new Exception("ResultBucketCount must be positive: " + ResultBucketCount);

                if (ResultBucketInterval <= 0)
                    throw new Exception("ResultBucketInterval must be positive: " + ResultBucketInterval);
            }
        }

        /// <summary>
        /// Get current throughput across all currenlty running threads.
        /// </summary>
        /// <returns>Current throughput.</returns>
        private IDictionary<string, Tuple<long, long>> CurrentThroughput()
        {
            IDictionary<string, Tuple<long, long>> total = new Dictionary<string, Tuple<long, long>>(descs.Count);

            foreach (GridClientBenchmarkOperationDescriptor desc in descs)
                total[desc.Name] = new Tuple<long, long>(0, 0);

            foreach (BenchmarkTask task in tasks)
                task.CollectThroughput(total);

            return total;
        }

        /** <inheritDoc /> */
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder(GetType().Name).Append('[');

            bool first = true;

            PropertyInfo[] props = BU.ListProperties(this);

            foreach (PropertyInfo prop in props)
            {
                if (first)
                    first = false;
                else
                    sb.Append(", ");

                sb.Append(prop.Name).Append('=').Append(prop.GetValue(this, null));
            }

            sb.Append(']');

            return sb.ToString();
        }

        /* COMMON PUBLIC PROPERTIES. */

        /// <summary>
        /// Amount of worker threads.
        /// </summary>
        public int Threads
        {
            get;
            set;
        }

        /// <summary>
        /// Warmup duration in secnods.
        /// </summary>
        public int Warmup
        {
            get;
            set;
        }

        /// <summary>
        /// Duration in seconds.
        /// </summary>
        public int Duration
        {
            get;
            set;
        }

        /// <summary>
        /// Maximum amount of operations to perform.
        /// </summary>
        public int Operations
        {
            get;
            set;
        }

        /// <summary>
        /// Single measurement batch size.
        /// </summary>
        public int BatchSize
        {
            get;
            set;
        }

        /// <summary>
        /// Maximum amount of errors before benchmark exits.
        /// </summary>
        public int MaxErrors
        {
            get;
            set;
        }

        /// <summary>
        /// Debug flag.
        /// </summary>
        public bool Debug
        {
            get;
            set;
        }

        /// <summary>
        /// Result writer type.
        /// </summary>
        public string ResultWriter
        {
            get;
            set;
        }

        /// <summary>
        /// Result output folder.
        /// </summary>
        public string ResultFolder
        {
            get;
            set;
        }

        /// <summary>
        /// Percentile result buckets count.
        /// </summary>
        public int ResultBucketCount
        {
            get;
            set;
        }

        /// <summary>
        /// Percnetile result bucket interval in microseconds.
        /// </summary>
        public long ResultBucketInterval
        {
            get;
            set;
        }

        /* INNER CLASSES. */

        /// <summary>
        /// Benchmark worker task.
        /// </summary>
        private class BenchmarkTask
        {
            /** Benchmark. */
            private readonly GridClientAbstractBenchmark benchmark;

            /** Descriptors. */
            private GridClientBenchmarkOperationDescriptor[] descs;

            /** Results. */
            private readonly IDictionary<string, Result> results;

            /** Stop watch. */
            private readonly Stopwatch watch = new Stopwatch();

            /** Benchmark state. */
            private readonly GridClientBenchmarkState state;

            /** Thread ID. */
            public volatile int threadId;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="benchmark">Benchmark.</param>
            public BenchmarkTask(GridClientAbstractBenchmark benchmark, 
                ICollection<GridClientBenchmarkOperationDescriptor> descList)
            {
                this.benchmark = benchmark;
                
                state = new GridClientBenchmarkState();

                results = new Dictionary<string, Result>(descList.Count);

                int totalWeight = 0;

                long ticksPerSlot = benchmark.ResultBucketInterval * Stopwatch.Frequency / 1000000;

                if (ticksPerSlot == 0)
                    throw new Exception("Too low bucket interval: " + benchmark.ResultBucketInterval);

                foreach (GridClientBenchmarkOperationDescriptor desc in descList)
                {
                    results[desc.Name] = new Result(benchmark.ResultBucketCount, ticksPerSlot);

                    totalWeight += desc.Weight;
                }

                descs = new GridClientBenchmarkOperationDescriptor[totalWeight];

                int idx = 0;

                foreach (GridClientBenchmarkOperationDescriptor desc in descList)
                {
                    for (int i = 0; i < desc.Weight; i++)
                        descs[idx++] = desc;
                }
            }

            /// <summary>
            /// Task routine.
            /// </summary>
            public void Run()
            {
                threadId = Thread.CurrentThread.ManagedThreadId;

                try
                {
                    benchmark.OnThreadReady();

                    benchmark.PrintDebug("Worker thread ready.");

                    while (!benchmark.start)
                        Thread.Sleep(10);

                    benchmark.PrintDebug("Worker thread started benchmark execution.");

                    bool warmupIteration = true;

                    long maxDur = 0;

                    long maxOps = benchmark.Operations;

                    while (!benchmark.stop)
                    {
                        if (warmupIteration && !benchmark.warmup)
                        {
                            warmupIteration = false;

                            benchmark.OnWarmupFinished();

                            state.StopWarmup();
                            
                            benchmark.barrier.SignalAndWait();
                        }

                        if (!warmupIteration)
                        {
                            if (maxOps > 0 && Interlocked.Read(ref benchmark.curOps) > maxOps)
                                break;
                        }

                        GridClientBenchmarkOperationDescriptor desc = descs.Length == 1 ? 
                            descs[0] : descs[BU.RandomInt(descs.Length)];

                        bool res = true;

                        benchmark.OnBatchStarted(state);

                        watch.Start();

                        try
                        {
                            for (int i = 0; i < benchmark.BatchSize; i++)
                            {
                                desc.Operation(state);

                                state.IncrementCounter();
                            }

                            if (!warmupIteration)
                                Interlocked.Add(ref benchmark.curOps, benchmark.BatchSize);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("Exception: " + e);

                            res = false;

                            if (benchmark.MaxErrors > 0 && Interlocked.Increment(ref benchmark.errs) > benchmark.MaxErrors)
                            {
                                lock (benchmark)
                                {
                                    Console.WriteLine("Benchmark is stopped due to too much errors: " +
                                        benchmark.MaxErrors);

                                    Environment.Exit(-1);
                                }
                            }
                        }
                        finally
                        {
                            watch.Stop();

                            long curDur = watch.ElapsedTicks;

                            if (res)
                                res = benchmark.OnBatchFinished(state, curDur);

                            state.Reset();

                            if (curDur > maxDur)
                            {
                                maxDur = curDur;

                                benchmark.PrintDebug("The longest execution [warmup=" + warmupIteration +
                                    ", dur(nsec)=" + maxDur * 1000000000 / Stopwatch.Frequency + ']');
                            }

                            watch.Reset();

                            if (!warmupIteration && res)
                                results[desc.Name].Add(curDur);
                        }
                    }
                }
                finally
                {
                    benchmark.PrintDebug("Worker thread stopped.");

                    benchmark.OnThreadFinished();
                }
            }

            /// <summary>
            /// Collect throughput for the current task.
            /// </summary>
            /// <param name="total">Total result.</param>
            public void CollectThroughput(IDictionary<string, Tuple<long, long>> total)
            {
                foreach (KeyValuePair<string, Result> result in results)
                {
                    Tuple<long, long> old = total[result.Key];

                    total[result.Key] = new Tuple<long, long>(old.Item1 + result.Value.dur,
                        old.Item2 + result.Value.cnt);
                }
            }

            /// <summary>
            /// Collect percnetiles for the current task.
            /// </summary>
            /// <param name="total"></param>
            public void CollectPercentiles(IDictionary<string, long[]> total)
            {
                foreach (KeyValuePair<string, Result> result in results)
                {
                    long[] arr = total[result.Key];

                    for (int i = 0; i < arr.Length; i++)
                        arr[i] += result.Value.slots[i];
                }
            }
        }

        /// <summary>
        /// Timeout task to stop execution.
        /// </summary>
        private class TimeoutTask
        {
            /** Benchmark. */
            private readonly GridClientAbstractBenchmark benchmark;

            /** Duration. */
            private readonly long dur;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="benchmark">Benchmark.</param>
            /// <param name="dur">Duration.</param>
            public TimeoutTask(GridClientAbstractBenchmark benchmark, long dur)
            {
                this.benchmark = benchmark;
                this.dur = dur;
            }

            /// <summary>
            /// Task routine.
            /// </summary>
            public void Run()
            {
                try
                {
                    Thread.Sleep(TimeSpan.FromSeconds(dur));
                }
                finally
                {
                    benchmark.stop = true;
                }
            }
        }

        /// <summary>
        /// Warmup task to clear warmup flag.
        /// </summary>
        private class WarmupTask
        {
            /** Benchmark. */
            private readonly GridClientAbstractBenchmark benchmark;

            /** Duration. */
            private readonly long dur;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="benchmark">Benchmark.</param>
            /// <param name="dur">Duration.</param>
            public WarmupTask(GridClientAbstractBenchmark benchmark, long dur)
            {
                this.benchmark = benchmark;
                this.dur = dur;
            }

            /// <summary>
            /// Task routine.
            /// </summary>
            public void Run()
            {
                try
                {
                    Thread.Sleep(TimeSpan.FromSeconds(dur));
                }
                finally
                {
                    benchmark.warmup = false;
                }
            }
        }

        /// <summary>
        /// Throughput write task.
        /// </summary>
        private class ThroughputTask
        {
            /** Benchmark. */
            private readonly GridClientAbstractBenchmark benchmark;

            /** Last recorded result. */
            private IDictionary<string, Tuple<long, long>> lastResults;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="benchmark">Benchmark.</param>
            public ThroughputTask(GridClientAbstractBenchmark benchmark)
            {
                this.benchmark = benchmark;
            }

            public void Run()
            {
                while (!benchmark.stop)
                {
                    Thread.Sleep(1000);

                    if (benchmark.start && !benchmark.warmup)
                    {
                        IDictionary<string, Tuple<long, long>> results = benchmark.CurrentThroughput();

                        if (benchmark.finishedThreads > 0)
                            return; // Threads are stopping, do not collect any more.

                        foreach (KeyValuePair<string, Tuple<long, long>> pair in results)
                        {
                            Tuple<long, long> old;

                            if (lastResults != null && lastResults.TryGetValue(pair.Key, out old))
                                benchmark.writer.WriteThroughput(pair.Key, pair.Value.Item1 - old.Item1,
                                    pair.Value.Item2 - old.Item2);
                            else
                                benchmark.writer.WriteThroughput(pair.Key, pair.Value.Item1,
                                    pair.Value.Item2);
                        }

                        lastResults = results;
                    }
                }
            }
        }

        /// <summary>
        /// Benchmark result. Specific for each operation.
        /// </summary>
        private class Result
        {
            /** Slots. */
            public readonly long[] slots;

            /** Slot duration in ticks. */
            private readonly long slotDur;

            /** Total operations count. */
            public long cnt;

            /** Total duration. */
            public long dur;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="slotCnt">Slot count.</param>
            /// <param name="slotDur">Slot duration in ticks.</param>
            public Result(long slotCnt, long slotDur)
            {
                slots = new long[slotCnt];

                this.slotDur = slotDur;
            }

            /// <summary>
            /// Add result.
            /// </summary>
            /// <param name="curDur">Current duration in ticks.</param>
            public void Add(long curDur)
            {
                int idx = (int)(curDur / slotDur);

                if (idx >= slots.Length)
                    idx = slots.Length - 1;

                slots[idx] += 1;

                cnt++;
                dur += curDur;
            }
        }
    }
}
