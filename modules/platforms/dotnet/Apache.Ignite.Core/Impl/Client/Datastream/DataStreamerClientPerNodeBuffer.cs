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

    /// <summary>
    /// Manages per-node buffers and flush operations.
    /// </summary>
    internal sealed class DataStreamerClientPerNodeBuffer<TK, TV>
    {
        /** */
        private readonly DataStreamerClient<TK, TV> _client;

        /** */
        private readonly ClientSocket _socket;

        /** */
        private readonly SemaphoreSlim _semaphore;

        /** */
        private readonly int _maxBufferSize;

        /** Only the thread that completes the previous buffer can set a new one to this field. */
        private volatile DataStreamerClientBuffer<TK, TV> _buffer;


        public DataStreamerClientPerNodeBuffer(DataStreamerClient<TK, TV> client, ClientSocket socket)
        {
            Debug.Assert(client != null);

            _maxBufferSize = client.Options.ClientPerNodeBufferSize;
            _client = client;
            _socket = socket;
            _semaphore = new SemaphoreSlim(client.Options.ClientPerNodeParallelOperations);

            _buffer = new DataStreamerClientBuffer<TK, TV>(_maxBufferSize, this, null);
        }

        public void Add(DataStreamerClientEntry<TK,TV> entry)
        {
            while (true)
            {
                var buffer = GetBuffer();

                if (buffer.Add(entry))
                {
                    break;
                }

                Interlocked.CompareExchange(ref _buffer,
                    new DataStreamerClientBuffer<TK, TV>(_maxBufferSize, this, buffer), buffer);
            }
        }

        public Task ScheduleFlush(bool close)
        {
            DataStreamerClientBuffer<TK, TV> buffer;

            if (close)
            {
                while (true)
                {
                    buffer = _buffer;

                    if (buffer == null)
                    {
                        return null;
                    }

                    if (Interlocked.CompareExchange(ref _buffer, null, buffer) == buffer)
                    {
                        buffer.ScheduleFlush();
                        return buffer.FlushTask;
                    }
                }
            }

            buffer = GetBuffer();
            buffer.ScheduleFlush();

            return buffer.FlushTask;
        }

        internal Task FlushAsync(DataStreamerClientBuffer<TK,TV> buffer)
        {
            // TODO: Create streamer and preserve ID.
            return _client.FlushBufferAsync(buffer, _socket, _semaphore, flush: true, close: true);
        }

        private DataStreamerClientBuffer<TK, TV> GetBuffer()
        {
            var buffer = _buffer;

            if (buffer == null)
            {
                throw new ObjectDisposedException("DataStreamerClient", "Data streamer has been disposed");
            }

            return buffer;
        }
    }
}
