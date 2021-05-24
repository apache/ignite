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
        private readonly ReaderWriterLockSlim _rwLock = new ReaderWriterLockSlim();

        /** Only the thread that completes the previous buffer can set a new one to this field. */
        private volatile DataStreamerClientBuffer<TK, TV> _buffer;

        /** */
        private volatile bool _closed = false;

        public DataStreamerClientPerNodeBuffer(DataStreamerClient<TK, TV> client, ClientSocket socket)
        {
            Debug.Assert(client != null);

            _client = client;
            _socket = socket;
            _semaphore = new SemaphoreSlim(client.Options.PerNodeParallelOperations);

            _buffer = new DataStreamerClientBuffer<TK, TV>(_client.GetArray(), this, null);
        }

        public bool Add(DataStreamerClientEntry<TK,TV> entry)
        {
            if (!_rwLock.TryEnterReadLock(0))
            {
                return false;
            }

            try
            {
                if (_closed)
                {
                    return false;
                }

                while (true)
                {
                    var buffer = GetBuffer();

                    if (buffer.Add(entry))
                    {
                        break;
                    }

                    var entryArray = _client.GetArray();

                    if (Interlocked.CompareExchange(ref _buffer,
                        new DataStreamerClientBuffer<TK, TV>(entryArray, this, buffer), buffer) != buffer)
                    {
                        _client.ReturnArray(entryArray);
                    }
                }

                return true;
            }
            finally
            {
                _rwLock.ExitReadLock();
            }
        }

        public void Close()
        {
            _rwLock.EnterWriteLock();

            try
            {
                _closed = true;
            }
            finally
            {
                _rwLock.ExitWriteLock();
            }
        }

        public Task FlushAllAsync()
        {
            var buffer = GetBuffer();
            buffer.ScheduleFlush(userRequested: true);

            return buffer.GetChainFlushTask();
        }

        internal Task FlushAsync(DataStreamerClientBuffer<TK, TV> buffer, bool userRequested)
        {
            // Stateless mode: every client client-side buffer creates a one-off streamer on the server.
            return _client.FlushBufferAsync(buffer, _socket, _semaphore, userRequested);
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
