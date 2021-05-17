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
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Client data streamer buffer.
    /// </summary>
    internal sealed class DataStreamerClientBuffer<TK, TV>
    {
        /** Array vs concurrent data structures:
         * - Can be written in parallel, since index is from Interlocked.Increment, no additional synchronization needed
         * - Compact storage
         * - Easy pooling
         */
        private readonly DataStreamerClientEntry<TK, TV>[] _entries;

        /** */
        private TaskCompletionSource<object> _flushCompletionSource;

        /** */
        private readonly ReaderWriterLockSlim _rwLock = new ReaderWriterLockSlim();

        /** */
        private readonly DataStreamerClientPerNodeBuffer<TK,TV> _parent;

        /** */
        private DataStreamerClientBuffer<TK,TV> _previous;

        /** */
        private long _size;

        /** */
        private volatile bool _flushing;

        /** */
        private volatile bool _flushed;

        /** TODO: Handle exceptions on top level. Either retry or close streamer with an error. */
        private volatile Exception _exception;

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

        public int Count
        {
            get { return _size > _entries.Length ? _entries.Length : (int) _size; }
        }

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

        public DataStreamerClientEntry<TK, TV>[] Entries
        {
            get { return _entries; }
        }

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
                TryRunFlushAction(close: false);
            }

            return true;
        }

        /// <summary>
        /// Returns true if flushing has started as a result of this call or before that.
        /// </summary>
        /// <param name="close"></param>
        public bool ScheduleFlush(bool close)
        {
            if (Interlocked.CompareExchange(ref _size, -1, -1) == 0)
            {
                // Empty buffer.
                return false;
            }

            TryRunFlushAction(close);

            return true;
        }

        private void TryRunFlushAction(bool close)
        {
            _rwLock.EnterWriteLock();

            try
            {
                if (_flushing || _flushed)
                {
                    return;
                }

                _flushing = true;

                if (close)
                {
                    var previous = _previous;

                    if (previous != null && !previous._flushed)
                    {
                        // All previous flushes must complete before we close the streamer.
                        previous.GetChainFlushTask().ContinueWith(_ => RunFlushAction(true));
                    }
                    else
                    {
                        RunFlushAction(true);
                    }
                }
                else if (Count > 0)
                {
                    RunFlushAction(false);
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

        private void RunFlushAction(bool close)
        {
            // TODO: This is not necessary during normal operation - can we get rid of this if no one listens
            // for completions?

            // NOTE: Continuation runs on socket thread - set result on thread pool.
            _parent.FlushAsync(this, close).ContinueWith(
                t => ThreadPool.QueueUserWorkItem(buf =>
                    ((DataStreamerClientBuffer<TK, TV>)buf).OnFlushed(t.Exception), this),
                TaskContinuationOptions.ExecuteSynchronously);
        }

        private void OnFlushed(AggregateException exception = null)
        {
            TaskCompletionSource<object> tcs;

            _rwLock.EnterWriteLock();

            try
            {
                _flushed = true;
                _flushing = false;
                _exception = exception;

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
                    previous.GetChainFlushTask().ContinueWith(
                        t => TrySetResultOrException(tcs, exception ?? t.Exception),
                        TaskContinuationOptions.ExecuteSynchronously);
                }
            }
        }

        /** */
        private void TrySetResultOrException(TaskCompletionSource<object> tcs, Exception exception)
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

        /** */
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
    }
}
