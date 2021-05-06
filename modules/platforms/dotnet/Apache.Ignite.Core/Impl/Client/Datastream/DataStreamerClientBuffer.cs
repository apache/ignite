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
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Client data streamer buffer.
    /// </summary>
    internal sealed class DataStreamerClientBuffer<TK, TV> : IEnumerable<DataStreamerClientEntry<TK, TV>>
    {
        /** Concurrent bag already has per-thread buffers. */
        private readonly ConcurrentBag<DataStreamerClientEntry<TK, TV>> _entries =
            new ConcurrentBag<DataStreamerClientEntry<TK, TV>>();

        /** */
        private readonly int _maxSize;

        /** */
        private readonly Func<DataStreamerClientBuffer<TK, TV>, Task> _flushAction;

        /** */
        private readonly TaskCompletionSource<object> _flushCompletionSource = new TaskCompletionSource<object>();

        /** */
        private long _size;

        /** */
        private int _activeOps;

        public DataStreamerClientBuffer(int maxSize, Func<DataStreamerClientBuffer<TK, TV>, Task> flushAction)
        {
            Debug.Assert(maxSize > 0);

            _maxSize = maxSize;
            _flushAction = flushAction;
        }

        public int Count
        {
            get { return _entries.Count; }
        }

        public Task FlushTask
        {
            get { return _flushCompletionSource.Task; }
        }

        public AddResult Add(DataStreamerClientEntry<TK, TV> entry)
        {
            Interlocked.Increment(ref _activeOps);

            try
            {
                var newSize = Interlocked.Increment(ref _size);
                if (newSize > _maxSize)
                {
                    return AddResult.FailFull;
                }

                _entries.Add(entry);

                return newSize == _maxSize
                    ? AddResult.OkFull
                    : AddResult.Ok;
            }
            finally
            {
                var ops = Interlocked.Decrement(ref _activeOps);

                if (ops == 0 &&
                    Interlocked.CompareExchange(ref _size, -1, -1) >= _maxSize)
                {
                    // TODO: Thread pool?
                    // TODO: Error handling
                    RunFlushAction();
                }
            }
        }

        public bool ScheduleFlush()
        {
            var res = Interlocked.Add(ref _size, _maxSize);

            if (res - _maxSize >= _maxSize)
            {
                // Already flushed.
                return false;
            }

            if (Interlocked.CompareExchange(ref _activeOps, -1, -1) == 0)
            {
                RunFlushAction();
            }

            return true;
        }

        public IEnumerator<DataStreamerClientEntry<TK, TV>> GetEnumerator()
        {
            return _entries.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        private void RunFlushAction()
        {
            _flushAction(this).ContinueWith(
                _ => _flushCompletionSource.TrySetResult(null),
                TaskContinuationOptions.ExecuteSynchronously);
        }
    }
}
