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
            lock (this)
            {
                if (CheckChainFlushed())
                {
                    return TaskRunner.CompletedTask;
                }

                if (_flushCompletionSource == null)
                {
                    _flushCompletionSource = new TaskCompletionSource<object>();
                }

                return _flushCompletionSource.Task;
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
                if (_flushing)
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
                if (_flushing)
                {
                    return;
                }

                _flushing = true;

                if (Count > 0)
                {
                    RunFlushAction(close);
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

            lock (this)
            {
                _flushed = true;

                tcs = _flushCompletionSource;
            }

            CheckChainFlushed();

            if (tcs != null)
            {
                var previous = _previous;

                if (previous == null)
                {
                    TrySetResult(tcs, exception);
                }
                else
                {
                    previous.GetChainFlushTask().ContinueWith(
                        t => TrySetResult(tcs, exception ?? t.Exception),
                        TaskContinuationOptions.ExecuteSynchronously);
                }
            }
        }

        /** */
        private static void TrySetResult(TaskCompletionSource<object> tcs, Exception exception)
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
