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
    using System.Threading.Tasks;

    /// <summary>
    /// Manages per-node buffers and flush operations.
    /// </summary>
    internal sealed class DataStreamerClientPerNodeBuffer<TK, TV>
    {
        /** */
        private readonly int _maxBufferSize;

        /** */
        private readonly Func<DataStreamerClientBuffer<TK, TV>, Task> _flushAction;

        /** */
        private volatile DataStreamerClientBuffer<TK, TV> _buffer;

        public DataStreamerClientPerNodeBuffer(
            int maxBufferSize,
            Func<DataStreamerClientBuffer<TK, TV>, Task> flushAction)
        {
            Debug.Assert(flushAction != null);

            _maxBufferSize = maxBufferSize;
            _flushAction = flushAction;

            _buffer = new DataStreamerClientBuffer<TK, TV>(_maxBufferSize, flushAction);
        }

        public void Add(DataStreamerClientEntry<TK,TV> entry)
        {
            var buffer = _buffer;

            while (true)
            {
                var addResult = buffer.Add(entry);

                if (addResult == AddResult.Ok)
                {
                    break;
                }

                if (addResult == AddResult.OkFull)
                {
                    // Buffer was completed by the current thread - replace it with a new one.
                    _buffer = new DataStreamerClientBuffer<TK, TV>(_maxBufferSize, _flushAction);
                    break;
                }

                // Buffer was completed and replaced by another thread - retry.
                buffer = _buffer;
            }
        }

        public Task ScheduleFlush()
        {
            var buffer = _buffer;

            if (buffer.ScheduleFlush())
            {
                // Buffer was completed by the current thread - replace it with a new one.
                _buffer = new DataStreamerClientBuffer<TK, TV>(_maxBufferSize, _flushAction);

                return buffer.FlushTask;
            }

            return null;
        }
    }
}
