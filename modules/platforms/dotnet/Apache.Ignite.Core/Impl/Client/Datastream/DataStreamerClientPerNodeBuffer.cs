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
        private volatile DataStreamerClientBuffer<TK, TV> _buffer;

        private readonly int _maxBufferSize;

        private readonly Func<DataStreamerClientBuffer<TK, TV>, Task> _flushAction;

        public DataStreamerClientPerNodeBuffer(
            int maxBufferSize,
            Func<DataStreamerClientBuffer<TK, TV>, Task> flushAction)
        {
            Debug.Assert(flushAction != null);

            _maxBufferSize = maxBufferSize;
            _flushAction = flushAction;

            _buffer = new DataStreamerClientBuffer<TK, TV>(_maxBufferSize, flushAction);
        }

        public Task FlushTask
        {
            get { return _buffer.FlushTask; }
        }

        public AddResult Add(DataStreamerClientEntry<TK,TV> entry)
        {
            return _buffer.Add(entry);
        }

        public bool ScheduleFlush()
        {
            return _buffer.ScheduleFlush();
        }
    }
}
