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

namespace Apache.Ignite.Benchmarks
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using Apache.Ignite.Benchmarks.Result;

    /// <summary>
    /// Benchmark base class.
    /// </summary>
    internal abstract class BenchmarkBase
    {
        /** Result writer type: console. */
        private const string ResultWriterConsole = "console";

        /** Result writer type: file. */
        private const string ResultWriterFile = "file";

        /** Default duration. */
        private const int DefaultDuration = 60;

        /** Default maximum errors count. */
        private const int DefaultMaxErrors = 100;

        /** Default percentile result buckets count. */
        private const int DEfaultResultBucketCount = 10000;

        /** Default percentile result bucket interval. */
        private const int DefaultResultBucketInterval = 100;

        /** Default batch size. */
        private const int DefaultBatchSize = 1;

        /** Default result wrier. */
        private const string DefaultResultWriter = ResultWriterConsole;

        /** Start flag. */
        private volatile bool _start;

        /** Stop flag. */
        private volatile bool _stop;

        /** Warmup flag. */
        private volatile bool _warmup = true;

        /** Ready threads. */
        private int _readyThreads;

        /** Finished threads. */
        private volatile int _finishedThreads;

        /** Descriptors. */
        private volatile ICollection<BenchmarkOperationDescriptor> _descs;

        /** Benchmark tasks. */
        private volatile ICollection<BenchmarkTask> _tasks;

        /** Percentile results. */
        private volatile IDictionary<string, long[]> _percentiles;

        /** Currently completed operations. */
        private long _curOps;

        /** Error count. */
        private long _errorCount;

        /** Watches to count total execution time. */
        private readonly Stopwatch _totalWatch = new Stopwatch();

        /** Warmup barrier. */
        private Barrier _barrier;

        /** Benchmark result writer. */
        private IBenchmarkResultWriter _writer;

        /// <summary>
        /// Default constructor.
        /// </summary>
        protected BenchmarkBase()
        {
            Duration = DefaultDuration;
            MaxErrors = DefaultMaxErrors;
            ResultBucketCount = DEfaultResultBucketCount;
            ResultBucketInterval = DefaultResultBucketInterval;
            BatchSize = DefaultBatchSize;
            ResultWriter = DefaultResultWriter;
        }

        /// <summary>
        /// Run the benchmark.
        /// </summary>
        public void Run()
        {
            PrintDebug("Started benchmark: " + this);

            ValidateArguments();

            if (ResultWriter.ToLower().Equals(ResultWriterConsole))
                _writer = new BenchmarkConsoleResultWriter();
            else
                _writer = new BenchmarkFileResultWriter();

            OnStarted();

            PrintDebug("Benchmark setup finished.");

            try
            {
                _descs = new List<BenchmarkOperationDescriptor>();

                GetDescriptors(_descs);

                if (_descs.Count == 0)
                    throw new Exception("No tasks provided for benchmark.");

                // Initialize writer.
                var opNames = new List<string>(_descs.Select(desc => desc.Name));

                PrintDebug(() =>
                {
                    var sb = new StringBuilder("Operations: ");

                    foreach (var opName in opNames)
                        sb.Append(opName).Append(" ");

                    return sb.ToString();
                });

                _writer.Initialize(this, opNames);

                PrintDebug("Initialized result writer.");

                // Start worker threads.
                _tasks = new List<BenchmarkTask>(Threads);

                PrintDebug("Starting worker threads: " + Threads);

                for (var i = 0; i < Threads; i++)
                {
                    var task = new BenchmarkTask(this, _descs);

                    _tasks.Add(task);

                    new Thread(task.Run).Start();
                }

                PrintDebug("Waiting worker threads to start: " + Threads);

                // Await for all threads to start in spin loop.
                while (Thread.VolatileRead(ref _readyThreads) < Threads)
                    Thread.Sleep(10);

                PrintDebug("Worker threads started: " + Threads);

                // Start throughput writer thread.
                var writerThread = new Thread(new ThroughputTask(this).Run) {IsBackground = true};

                writerThread.Start();

                PrintDebug("Started throughput writer thread.");

                // Start warmup thread if needed.
                if (Warmup > 0)
                {
                    var thread = new Thread(new WarmupTask(this, Warmup).Run) {IsBackground = true};

                    thread.Start();

                    PrintDebug("Started warmup timeout thread: " + Warmup);
                }
                else
                    _warmup = false;

                _barrier = new Barrier(Threads, b =>
                {
                    Console.WriteLine("Warmup finished.");

                    _totalWatch.Start();
                });

                // Start timeout thread if needed.
                if (Duration > 0)
                {
                    if (Operations > 0)
                        PrintDebug("Duration argument is ignored because operations number is set: " +
                                   Operations);
                    else
                    {
                        var thread = new Thread(new TimeoutTask(this, Warmup + Duration).Run) {IsBackground = true};

                        thread.Start();

                        PrintDebug("Started duration timeout thread: " + Duration);
                    }
                }

                // Let workers start execution.
                _start = true;

                // Await workers completion.
                PrintDebug("Awaiting worker threads completion.");

                Monitor.Enter(this);

                try
                {
                    while (_finishedThreads < Threads)
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

                _totalWatch.Stop();

                PrintDebug("Tear down invoked.");

                if (PrintThroughputInfo())
                {
                    var avgThroughput = _totalWatch.ElapsedMilliseconds == 0
                        ? 0
                        : _curOps*1000/_totalWatch.ElapsedMilliseconds;

                    var avgLatency = _curOps == 0
                        ? 0
                        : (double) _totalWatch.ElapsedMilliseconds*Threads/_curOps;

                    Console.WriteLine("Finishing benchmark [name=" + GetType().Name +
                                      ", time=" + _totalWatch.ElapsedMilliseconds +
                                      "ms, ops=" + _curOps +
                                      ", threads=" + Threads +
                                      ", avgThroughput=" + avgThroughput +
                                      ", avgLatency=" + string.Format("{0:0.000}ms", avgLatency) + ']');
                }
                else
                {
                    Console.WriteLine("Finishing benchmark [name=" + GetType().Name +
                                      ", time=" + _totalWatch.ElapsedMilliseconds +
                                      "ms, ops=" + Operations +
                                      ", threads=" + Threads + ']');
                }
            }

            _percentiles = new Dictionary<string, long[]>(_descs.Count);

            foreach (var desc in _descs)
                _percentiles[desc.Name] = new long[ResultBucketCount];

            foreach (var task in _tasks)
                task.CollectPercentiles(_percentiles);

            foreach (var percentile in _percentiles)
                _writer.WritePercentiles(percentile.Key, ResultBucketInterval, percentile.Value);

            _writer.Commit();

            PrintDebug("Results committed to output writer.");
        }

        /// <summary>
        /// Consumes passed argument.
        /// </summary>
        /// <param name="name">Argument name.</param>
        /// <param name="val">Value.</param>
        /// <returns>True if argument was consumed.</returns>
        public void Configure(string name, string val)
        {
            var prop = BenchmarkUtils.GetProperty(this, name);

            if (prop != null)
                BenchmarkUtils.SetProperty(this, prop, val);
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
        protected virtual void OnBatchStarted(BenchmarkState state)
        {
            // No-op.
        }

        /// <summary>
        /// Batch execution finished callback.
        /// </summary>
        /// <param name="state">State.</param>
        /// <param name="duration">Duration.</param>
        /// <returns>True if this result must be counted.</returns>
        protected virtual bool OnBatchFinished(BenchmarkState state, long duration)
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
        /// <param name="msgFunc">Message delegate.</param>
        protected void PrintDebug(Func<string> msgFunc)
        {
            if (Debug)
                PrintDebug(msgFunc.Invoke());
        }

        /// <summary>
        /// Add operation descriptors.
        /// </summary>
        /// <param name="descs">Collection where operation descriptors must be added.</param>
        protected abstract void GetDescriptors(ICollection<BenchmarkOperationDescriptor> descs);

        /// <summary>
        /// Invoked when single thread is ready to actual execution.
        /// </summary>
        private void OnThreadReady()
        {
            Interlocked.Increment(ref _readyThreads);
        }

        /// <summary>
        /// Invoked when single thread finished execution.
        /// </summary>
        private void OnThreadFinished()
        {
            Monitor.Enter(this);

            try
            {
                _finishedThreads++;

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

                if (ResultWriter == null || !ResultWriter.ToLower().Equals(ResultWriterConsole)
                    && !ResultWriter.ToLower().Equals(ResultWriterFile))
                    throw new Exception("Invalid ResultWriter: " + ResultWriter);

                if (ResultWriter.ToLower().Equals(ResultWriterFile) && ResultFolder == null)
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
        private IDictionary<string, Tuple<long, long>> GetCurrentThroughput()
        {
            var total = new Dictionary<string, Tuple<long, long>>(_descs.Count);

            foreach (var desc in _descs)
                total[desc.Name] = new Tuple<long, long>(0, 0);

            foreach (var task in _tasks)
                task.CollectThroughput(total);

            return total;
        }

        /** <inheritDoc /> */
        public override string ToString()
        {
            var sb = new StringBuilder(GetType().Name).Append('[');

            var first = true;

            var props = BenchmarkUtils.GetProperties(this);

            foreach (var prop in props)
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
        public int Threads { get; set; }

        /// <summary>
        /// Warmup duration in secnods.
        /// </summary>
        public int Warmup { get; set; }

        /// <summary>
        /// Duration in seconds.
        /// </summary>
        public int Duration { get; set; }

        /// <summary>
        /// Maximum amount of operations to perform.
        /// </summary>
        public int Operations { get; set; }

        /// <summary>
        /// Single measurement batch size.
        /// </summary>
        public int BatchSize { get; set; }

        /// <summary>
        /// Maximum amount of errors before benchmark exits.
        /// </summary>
        public int MaxErrors { get; set; }

        /// <summary>
        /// Debug flag.
        /// </summary>
        public bool Debug { get; set; }

        /// <summary>
        /// Result writer type.
        /// </summary>
        public string ResultWriter { get; set; }

        /// <summary>
        /// Result output folder.
        /// </summary>
        public string ResultFolder { get; set; }

        /// <summary>
        /// Percentile result buckets count.
        /// </summary>
        public int ResultBucketCount { get; set; }

        /// <summary>
        /// Percnetile result bucket interval in microseconds.
        /// </summary>
        public long ResultBucketInterval { get; set; }

        /* INNER CLASSES. */

        /// <summary>
        /// Benchmark worker task.
        /// </summary>
        private class BenchmarkTask
        {
            /** Benchmark. */
            private readonly BenchmarkBase _benchmark;

            /** Descriptors. */
            private readonly BenchmarkOperationDescriptor[] _descs;

            /** Results. */
            private readonly IDictionary<string, Result> _results;

            /** Stop watch. */
            private readonly Stopwatch _watch = new Stopwatch();

            /** Benchmark state. */
            private readonly BenchmarkState _state;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="benchmark">Benchmark.</param>
            /// <param name="descList">Descriptor list.</param>
            public BenchmarkTask(BenchmarkBase benchmark,
                ICollection<BenchmarkOperationDescriptor> descList)
            {
                _benchmark = benchmark;

                _state = new BenchmarkState();

                _results = new Dictionary<string, Result>(descList.Count);

                var totalWeight = 0;

                var ticksPerSlot = benchmark.ResultBucketInterval*Stopwatch.Frequency/1000000;

                if (ticksPerSlot == 0)
                    throw new Exception("Too low bucket interval: " + benchmark.ResultBucketInterval);

                foreach (var desc in descList)
                {
                    _results[desc.Name] = new Result(benchmark.ResultBucketCount, ticksPerSlot);

                    totalWeight += desc.Weight;
                }

                _descs = new BenchmarkOperationDescriptor[totalWeight];

                var idx = 0;

                foreach (var desc in descList)
                {
                    for (var i = 0; i < desc.Weight; i++)
                        _descs[idx++] = desc;
                }
            }

            /// <summary>
            /// Task routine.
            /// </summary>
            public void Run()
            {
                try
                {
                    _benchmark.OnThreadReady();

                    _benchmark.PrintDebug("Worker thread ready.");

                    while (!_benchmark._start)
                        Thread.Sleep(10);

                    _benchmark.PrintDebug("Worker thread started benchmark execution.");

                    var warmupIteration = true;

                    long maxDur = 0;

                    long maxOps = _benchmark.Operations;

                    while (!_benchmark._stop)
                    {
                        if (warmupIteration && !_benchmark._warmup)
                        {
                            warmupIteration = false;

                            _benchmark.OnWarmupFinished();

                            _state.StopWarmup();

                            _benchmark._barrier.SignalAndWait();
                        }

                        if (!warmupIteration)
                        {
                            if (maxOps > 0 && Interlocked.Read(ref _benchmark._curOps) > maxOps)
                                break;
                        }

                        var desc = _descs.Length == 1
                            ? _descs[0]
                            : _descs[BenchmarkUtils.GetRandomInt(_descs.Length)];

                        var res = true;

                        _benchmark.OnBatchStarted(_state);

                        _watch.Start();

                        try
                        {
                            for (var i = 0; i < _benchmark.BatchSize; i++)
                            {
                                desc.Operation(_state);

                                _state.IncrementCounter();
                            }

                            if (!warmupIteration)
                                Interlocked.Add(ref _benchmark._curOps, _benchmark.BatchSize);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("Exception: " + e);

                            res = false;

                            if (_benchmark.MaxErrors > 0 &&
                                Interlocked.Increment(ref _benchmark._errorCount) > _benchmark.MaxErrors)
                            {
                                lock (_benchmark)
                                {
                                    Console.WriteLine("Benchmark is stopped due to too much errors: " +
                                                      _benchmark.MaxErrors);

                                    Environment.Exit(-1);
                                }
                            }
                        }
                        finally
                        {
                            _watch.Stop();

                            var curDur = _watch.ElapsedTicks;

                            if (res)
                                res = _benchmark.OnBatchFinished(_state, curDur);

                            _state.Reset();

                            if (curDur > maxDur)
                            {
                                maxDur = curDur;

                                _benchmark.PrintDebug("The longest execution [warmup=" + warmupIteration +
                                                     ", dur(nsec)=" + maxDur*1000000000/Stopwatch.Frequency + ']');
                            }

                            _watch.Reset();

                            if (!warmupIteration && res)
                                _results[desc.Name].Add(curDur);
                        }
                    }
                }
                finally
                {
                    _benchmark.PrintDebug("Worker thread stopped.");

                    _benchmark.OnThreadFinished();
                }
            }

            /// <summary>
            /// Collect throughput for the current task.
            /// </summary>
            /// <param name="total">Total result.</param>
            public void CollectThroughput(IDictionary<string, Tuple<long, long>> total)
            {
                foreach (var result in _results)
                {
                    var old = total[result.Key];

                    total[result.Key] = new Tuple<long, long>(old.Item1 + result.Value.Duration,
                        old.Item2 + result.Value.OpCount);
                }
            }

            /// <summary>
            /// Collect percnetiles for the current task.
            /// </summary>
            /// <param name="total"></param>
            public void CollectPercentiles(IDictionary<string, long[]> total)
            {
                foreach (var result in _results)
                {
                    var arr = total[result.Key];

                    for (var i = 0; i < arr.Length; i++)
                        arr[i] += result.Value.Slots[i];
                }
            }
        }

        /// <summary>
        /// Timeout task to stop execution.
        /// </summary>
        private class TimeoutTask
        {
            /** Benchmark. */
            private readonly BenchmarkBase _benchmark;

            /** Duration. */
            private readonly long _dur;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="benchmark">Benchmark.</param>
            /// <param name="dur">Duration.</param>
            public TimeoutTask(BenchmarkBase benchmark, long dur)
            {
                _benchmark = benchmark;
                _dur = dur;
            }

            /// <summary>
            /// Task routine.
            /// </summary>
            public void Run()
            {
                try
                {
                    Thread.Sleep(TimeSpan.FromSeconds(_dur));
                }
                finally
                {
                    _benchmark._stop = true;
                }
            }
        }

        /// <summary>
        /// Warmup task to clear warmup flag.
        /// </summary>
        private class WarmupTask
        {
            /** Benchmark. */
            private readonly BenchmarkBase _benchmark;

            /** Duration. */
            private readonly long _dur;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="benchmark">Benchmark.</param>
            /// <param name="dur">Duration.</param>
            public WarmupTask(BenchmarkBase benchmark, long dur)
            {
                _benchmark = benchmark;
                _dur = dur;
            }

            /// <summary>
            /// Task routine.
            /// </summary>
            public void Run()
            {
                try
                {
                    Thread.Sleep(TimeSpan.FromSeconds(_dur));
                }
                finally
                {
                    _benchmark._warmup = false;
                }
            }
        }

        /// <summary>
        /// Throughput write task.
        /// </summary>
        private class ThroughputTask
        {
            /** Benchmark. */
            private readonly BenchmarkBase _benchmark;

            /** Last recorded result. */
            private IDictionary<string, Tuple<long, long>> _lastResults;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="benchmark">Benchmark.</param>
            public ThroughputTask(BenchmarkBase benchmark)
            {
                _benchmark = benchmark;
            }

            public void Run()
            {
                while (!_benchmark._stop)
                {
                    Thread.Sleep(1000);

                    if (_benchmark._start && !_benchmark._warmup)
                    {
                        var results = _benchmark.GetCurrentThroughput();

                        if (_benchmark._finishedThreads > 0)
                            return; // Threads are stopping, do not collect any more.

                        foreach (var pair in results)
                        {
                            Tuple<long, long> old;

                            if (_lastResults != null && _lastResults.TryGetValue(pair.Key, out old))
                                _benchmark._writer.WriteThroughput(pair.Key, pair.Value.Item1 - old.Item1,
                                    pair.Value.Item2 - old.Item2);
                            else
                                _benchmark._writer.WriteThroughput(pair.Key, pair.Value.Item1,
                                    pair.Value.Item2);
                        }

                        _lastResults = results;
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
            public readonly long[] Slots;

            /** Slot duration in ticks. */
            private readonly long _slotDuration;

            /** Total operations count. */
            public long OpCount;

            /** Total duration. */
            public long Duration;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="slotCnt">Slot count.</param>
            /// <param name="slotDuration">Slot duration in ticks.</param>
            public Result(long slotCnt, long slotDuration)
            {
                Slots = new long[slotCnt];

                _slotDuration = slotDuration;
            }

            /// <summary>
            /// Add result.
            /// </summary>
            /// <param name="curDur">Current duration in ticks.</param>
            public void Add(long curDur)
            {
                var idx = (int) (curDur/_slotDuration);

                if (idx >= Slots.Length)
                    idx = Slots.Length - 1;

                Slots[idx] += 1;

                OpCount++;
                Duration += curDur;
            }
        }
    }
}
