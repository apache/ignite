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
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Manages per-node buffers and flush operations.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable",
        Justification = "WaitHandle is not used in SemaphoreSlim and ReaderWriterLockSlim, no need to dispose.")]
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

        /** */
        private volatile DataStreamerClientBuffer<TK, TV> _buffer;

        /** */
        private volatile bool _closed;

        /// <summary>
        /// Initializes a new instance of <see cref="DataStreamerClientPerNodeBuffer{TK,TV}"/>.
        /// </summary>
        /// <param name="client">Streamer.</param>
        /// <param name="socket">Socket for the node.</param>
        public DataStreamerClientPerNodeBuffer(DataStreamerClient<TK, TV> client, ClientSocket socket)
        {
            Debug.Assert(client != null);

            _client = client;
            _socket = socket;
            _semaphore = new SemaphoreSlim(client.Options.PerNodeParallelOperations);

            _buffer = new DataStreamerClientBuffer<TK, TV>(_client.GetPooledArray(), this, null);
        }

        /// <summary>
        /// Adds an entry to the buffer.
        /// </summary>
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
                    var buffer = _buffer;

                    if (buffer.Add(entry))
                    {
                        return true;
                    }

                    var entryArray = _client.GetPooledArray();

#pragma warning disable 0420 // A reference to a volatile field will not be treated as volatile (not a problem)
                    if (Interlocked.CompareExchange(ref _buffer,
                        new DataStreamerClientBuffer<TK, TV>(entryArray, this, buffer), buffer) != buffer)
                    {
                        _client.ReturnPooledArray(entryArray);
                    }
#pragma warning restore 0420 // A reference to a volatile field will not be treated as volatile
                }
            }
            finally
            {
                _rwLock.ExitReadLock();
            }
        }

        /// <summary>
        /// Closes this instance and returns the remaining buffer, if any.
        /// </summary>
        public DataStreamerClientBuffer<TK, TV> Close()
        {
            _rwLock.EnterWriteLock();

            try
            {
                _closed = true;

                return _buffer;
            }
            finally
            {
                _rwLock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Flushes all buffers asynchronously.
        /// </summary>
        public Task FlushAllAsync()
        {
            var buffer = _buffer;
            buffer.ScheduleFlush();

            return buffer.GetChainFlushTask();
        }

        /// <summary>
        /// Flushes the specified buffer asynchronously.
        /// </summary>
        public Task FlushBufferAsync(DataStreamerClientBuffer<TK, TV> buffer)
        {
            return _client.FlushBufferAsync(buffer, _socket, _semaphore);
        }

        /** <inheritdoc /> */
        public override string ToString()
        {
            return string.Format("{0} [Socket={1}, Closed={2}, Buffer={3}]", GetType().Name, _socket.RemoteEndPoint,
                _closed, _buffer);
        }
    }
}
