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
        private readonly Action<DataStreamerClientBuffer<TK, TV>> _flushAction;

        /** */
        private long _size;

        /** */
        private int _activeOps;

        public DataStreamerClientBuffer(int maxSize, Action<DataStreamerClientBuffer<TK, TV>> flushAction)
        {
            Debug.Assert(maxSize > 0);

            _maxSize = maxSize;
            _flushAction = flushAction;
        }

        public int Count
        {
            get { return _entries.Count; }
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
                    _flushAction(this);
                }
            }
        }

        public void ScheduleFlush()
        {
            Interlocked.Add(ref _size, _maxSize);

            if (Interlocked.CompareExchange(ref _activeOps, -1, -1) == 0)
            {
                _flushAction(this);
            }
        }

        public IEnumerator<DataStreamerClientEntry<TK, TV>> GetEnumerator()
        {
            return _entries.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
