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

namespace Apache.Ignite.Core.Impl.Client.Datastream
{
    using System;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Client data streamer buffer.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable",
        Justification = "WaitHandle is not used in ReaderWriterLockSlim, no need to dispose.")]
    internal sealed class DataStreamerClientBuffer<TK, TV>
    {
        /** Array vs concurrent data structures:
         * - Can be written in parallel, since index is from Interlocked.Increment, no additional synchronization needed
         * - Compact storage (contiguous memory block)
         * - Less allocations
         * - Easy pooling
         */
        private readonly DataStreamerClientEntry<TK, TV>[] _entries;

        /** */
        private TaskCompletionSource<object> _flushCompletionSource;

        /** */
        private readonly ReaderWriterLockSlim _rwLock = new ReaderWriterLockSlim();

        /** */
        private readonly DataStreamerClientPerNodeBuffer<TK,TV> _parent;

        /** Previous buffer in the chain. Buffer chain allows us to track previous flushes. */
        private DataStreamerClientBuffer<TK,TV> _previous;

        /** */
        private long _size;

        /** */
        private volatile bool _flushing;

        /** */
        private volatile bool _flushed;

        /// <summary>
        /// Initializes a new instance of <see cref="DataStreamerClientBuffer{TK,TV}"/>.
        /// </summary>
        public DataStreamerClientBuffer(
            DataStreamerClientEntry<TK,TV>[] entries,
            DataStreamerClientPerNodeBuffer<TK, TV> parent,
            DataStreamerClientBuffer<TK, TV> previous)
        {
            Debug.Assert(parent != null);

            _entries = entries;
            _parent = parent;
            _previous = previous;
        }

        /// <summary>
        /// Gets the entry count.
        /// </summary>
        public int Count
        {
            // Size can exceed entries length when multiple threads compete on Add.
            get { return _size > _entries.Length ? _entries.Length : (int) _size; }
        }

        /// <summary>
        /// Gets the flush task for this and all previous buffers.
        /// </summary>
        public Task GetChainFlushTask()
        {
            _rwLock.EnterWriteLock();

            try
            {
                if (CheckChainFlushed())
                {
                    return TaskRunner.CompletedTask;
                }

                var previous = _previous;

                if (_flushed)
                {
                    return previous == null
                        ? TaskRunner.CompletedTask
                        : previous.GetChainFlushTask();
                }

                if (_flushCompletionSource == null)
                {
                    _flushCompletionSource = new TaskCompletionSource<object>();
                }

                return previous == null
                    ? _flushCompletionSource.Task
                    : TaskRunner.WhenAll(new[] {previous.GetChainFlushTask(), _flushCompletionSource.Task});
            }
            finally
            {
                _rwLock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Gets the entries array.
        /// </summary>
        public DataStreamerClientEntry<TK, TV>[] Entries
        {
            get { return _entries; }
        }

        /// <summary>
        /// Gets the previous buffer in the chain.
        /// </summary>
        public DataStreamerClientBuffer<TK, TV> Previous
        {
            get { return _previous; }
        }

        /// <summary>
        /// Adds an entry to the buffer.
        /// </summary>
        /// <param name="entry">Entry.</param>
        /// <returns>True when added successfully; false otherwise (buffer is full, flushing, flushed, or closed).</returns>
        public bool Add(DataStreamerClientEntry<TK, TV> entry)
        {
            if (!_rwLock.TryEnterReadLock(0))
                return false;

            long newSize;

            try
            {
                if (_flushing || _flushed)
                {
                    return false;
                }

                newSize = Interlocked.Increment(ref _size);

                if (newSize > _entries.Length)
                {
                    return false;
                }

                _entries[newSize - 1] = entry;
            }
            finally
            {
                _rwLock.ExitReadLock();
            }

            if (newSize == _entries.Length)
            {
                TryStartFlush();
            }

            return true;
        }

        /// <summary>
        /// Initiates the flush operation.
        /// </summary>
        public void ScheduleFlush()
        {
            if (Interlocked.CompareExchange(ref _size, -1, -1) == 0)
            {
                // Empty buffer.
                return;
            }

            TryStartFlush();
        }

        /// <summary>
        /// Marks this buffer as flushed.
        /// </summary>
        public bool MarkFlushed()
        {
            _rwLock.EnterWriteLock();

            try
            {
                if (_flushing || _flushed)
                {
                    return false;
                }

                _flushed = true;
                return true;
            }
            finally
            {
                _rwLock.ExitWriteLock();
            }
        }

        /** <inheritdoc /> */
        public override string ToString()
        {
            return string.Format("{0} [Count={1}, Flushing={2}, Flushed={3}, Previous={4}]", GetType().Name, Count,
                _flushing, _flushed, _previous);
        }

        /// <summary>
        /// Attempts to start the flush operation.
        /// </summary>
        private void TryStartFlush()
        {
            _rwLock.EnterWriteLock();

            try
            {
                if (_flushing || _flushed)
                {
                    return;
                }

                _flushing = true;

                if (Count > 0)
                {
                    StartFlush();
                }
                else
                {
                    OnFlushed();
                }
            }
            finally
            {
                _rwLock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Starts the flush operation.
        /// </summary>
        private void StartFlush()
        {
            // NOTE: Continuation runs on socket thread - set result on thread pool.
            _parent.FlushBufferAsync(this).ContWith(
                t => ThreadPool.QueueUserWorkItem(buf =>
                    ((DataStreamerClientBuffer<TK, TV>)buf).OnFlushed(t.Exception), this),
                TaskContinuationOptions.ExecuteSynchronously);
        }

        /// <summary>
        /// Called when flush operation completes.
        /// </summary>
        private void OnFlushed(AggregateException exception = null)
        {
            TaskCompletionSource<object> tcs;

            _rwLock.EnterWriteLock();

            try
            {
                _flushed = true;
                _flushing = false;

                tcs = _flushCompletionSource;
            }
            finally
            {
                _rwLock.ExitWriteLock();
            }

            CheckChainFlushed();

            if (tcs != null)
            {
                var previous = _previous;

                if (previous == null)
                {
                    TrySetResultOrException(tcs, exception);
                }
                else
                {
                    previous.GetChainFlushTask().ContWith(
                        t => TrySetResultOrException(tcs, exception ?? t.Exception),
                        TaskContinuationOptions.ExecuteSynchronously);
                }
            }
        }

        /// <summary>
        /// Checks if entire buffer chain is already flushed.
        /// </summary>
        private bool CheckChainFlushed()
        {
            var previous = _previous;

            if (previous == null)
            {
                return _flushed;
            }

            if (!previous.CheckChainFlushed())
            {
                return false;
            }

            _previous = null;

            return _flushed;
        }

        /// <summary>
        /// Sets task completion source status.
        /// </summary>
        private static void TrySetResultOrException(TaskCompletionSource<object> tcs, Exception exception)
        {
            if (exception == null)
            {
                tcs.TrySetResult(null);
            }
            else
            {
                tcs.TrySetException(exception);
            }
        }
    }
}
