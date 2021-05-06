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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Datastream;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    ///
    /// </summary>
    internal sealed class DataStreamerClient<TK, TV> : IDataStreamerClient<TK, TV>
    {
        /** Streamer flags. */
        [Flags]
        private enum Flags : byte
        {
            None = 0,
            AllowOverwrite = 0x01,
            SkipStore = 0x02,
            KeepBinary = 0x04,
            Flush = 0x08,
            Close = 0x10
        }

        /** */
        private readonly ClientFailoverSocket _socket;

        /** */
        private readonly int _cacheId;

        /** */
        private readonly string _cacheName;

        /** */
        private readonly DataStreamerClientOptions<TK, TV> _options;

        /** */
        private readonly ConcurrentDictionary<ClientSocket, DataStreamerClientBuffer<TK, TV>> _buffers =
            new ConcurrentDictionary<ClientSocket, DataStreamerClientBuffer<TK, TV>>();

        /** */
        private readonly TaskCompletionSource<object> _closeTaskSource = new TaskCompletionSource<object>();

        /** */
        private int _activeFlushes;

        /** */
        private volatile bool _isClosed;

        public DataStreamerClient(
            ClientFailoverSocket socket,
            string cacheName,
            DataStreamerClientOptions<TK, TV> options)
        {
            Debug.Assert(socket != null);
            Debug.Assert(!string.IsNullOrEmpty(cacheName));

            // TODO: Validate options (non-zero buffer sizes).
            _socket = socket;
            _cacheName = cacheName;
            _cacheId = BinaryUtils.GetCacheId(cacheName);

            // Copy to prevent modification.
            _options = new DataStreamerClientOptions<TK, TV>(options);
        }

        public void Dispose()
        {
            // TODO: Dispose should not throw - how can we achieve that?
            // Require Flush, like Transaction requires Commit?
            // Log errors, but don't throw?

            // TODO: Lock?
            // TODO: ThrowIfDisposed everywhere.
            Close(cancel: false);
        }

        /** <inheritdoc /> */
        public string CacheName
        {
            get { return _cacheName; }
        }

        public DataStreamerClientOptions<TK, TV> Options
        {
            get
            {
                // Copy to prevent modification.
                return new DataStreamerClientOptions<TK, TV>(_options);
            }
        }

        public void Add(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            Add(new DataStreamerClientEntry<TK, TV>(key, val));
        }

        public void Add(IEnumerable<KeyValuePair<TK, TV>> entries)
        {
            IgniteArgumentCheck.NotNull(entries, "entries");

            foreach (var entry in entries)
            {
                Add(entry.Key, entry.Value);
            }
        }

        public void Remove(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            if (!_options.AllowOverwrite)
            {
                throw new IgniteClientException("DataStreamer can't remove data when AllowOverwrite is false.");
            }

            Add(new DataStreamerClientEntry<TK, TV>(key));
        }

        public void Remove(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            foreach (var key in keys)
            {
                Remove(key);
            }
        }

        public void Flush()
        {
            FlushAsync().Wait();
        }

        public Task FlushAsync()
        {
            if (_buffers.IsEmpty)
            {
                return TaskRunner.CompletedTask;
            }

            // TODO: Wait for ongoing flushes.
            var tasks = new List<Task>(_buffers.Count);

            foreach (var pair in _buffers)
            {
                var buffer = pair.Value;

                if (buffer.ScheduleFlush())
                {
                    var socket = pair.Key;
                    _buffers[socket] = CreateBuffer(socket);
                    tasks.Add(buffer.FlushTask);
                }
            }

            return TaskRunner.WhenAll(tasks.ToArray());
        }

        public void Close(bool cancel)
        {
            CloseAsync(cancel).Wait();
        }

        public Task CloseAsync(bool cancel)
        {
            if (_isClosed)
            {
                return _closeTaskSource.Task;
            }

            _isClosed = true;

            if (cancel)
            {
                SetCloseResultIfNoActiveFlushes();
            }
            else
            {
                FlushAsync().ContinueWith(_ => SetCloseResultIfNoActiveFlushes());
            }

            return _closeTaskSource.Task;
        }

        private void Add(DataStreamerClientEntry<TK, TV> entry)
        {
            // TODO: There is no backpressure - it is possible to have a lot of pending buffers
            // See perNodeParallelOperations and timeout in thick streamer - this blocks Add methods!
            // ClientFailoverSocket is responsible for maintaining connections and affinity logic.
            // We simply get the socket for the key.
            // TODO: Some buffers may become abandoned when a socket for them is disconnected
            // or server node leaves the cluster.
            // We should track such topology changes by subscribing to topology update events.
            var socket = _socket.GetAffinitySocket(_cacheId, entry.Key) ?? _socket.GetSocket();
            var buffer = GetOrAddBuffer(socket);

            while (true)
            {
                var addResult = buffer.Add(entry);

                if (addResult == AddResult.Ok)
                {
                    break;
                }

                if (addResult == AddResult.OkFull)
                {
                    _buffers[socket] = CreateBuffer(socket);
                    break;
                }

                buffer = _buffers[socket];
            }

            // Check in the end: we guarantee that if Add completes without errors, then the data won't be lost.
            // TODO: This provides unclear message - current entry may be loaded successfully.
            // The only way to fix this properly is an RW lock - use benchmarks to check how this performs.
            ThrowIfClosed();
        }

        private Task FlushBufferAsync(DataStreamerClientBuffer<TK, TV> buffer, ClientSocket socket)
        {
            if (buffer.Count == 0)
            {
                return Task.CompletedTask;
            }

            Interlocked.Increment(ref _activeFlushes);

            // TODO: Flush in a loop until succeeded.
            // TODO: Make sure to call OnFlushCompleted in any case.
            return socket.DoOutInOpAsync(
                    ClientOp.DataStreamerStart,
                    ctx => WriteBuffer(buffer, ctx.Writer),
                    ctx => ctx.Stream.ReadLong())
                .ContWith(_ => OnFlushCompleted());
        }

        private void WriteBuffer(DataStreamerClientBuffer<TK, TV> buffer, BinaryWriter w)
        {
            w.WriteInt(_cacheId);
            w.WriteByte((byte) GetFlags(flush: true, close: true));
            w.WriteInt(_options.ServerPerNodeBufferSize);
            w.WriteInt(_options.ServerPerThreadBufferSize);
            w.WriteObject(_options.Receiver);

            w.WriteInt(buffer.Count);

            foreach (var entry in buffer)
            {
                w.WriteObjectDetached(entry.Key);

                if (entry.Remove)
                {
                    w.WriteObject<object>(null);
                }
                else
                {
                    w.WriteObjectDetached(entry.Val);
                }
            }
        }

        private Flags GetFlags(bool flush, bool close)
        {
            var flags = Flags.None;

            if (flush)
            {
                flags |= Flags.Flush;
            }

            if (close)
            {
                flags |= Flags.Close;
            }

            if (_options.AllowOverwrite)
            {
                flags |= Flags.AllowOverwrite;
            }

            if (_options.SkipStore)
            {
                flags |= Flags.SkipStore;
            }

            return flags;
        }

        /// <summary>
        /// Called when a flush operation completes.
        /// </summary>
        private void OnFlushCompleted()
        {
            var res = Interlocked.Decrement(ref _activeFlushes);

            if (_isClosed && res == 0)
            {
                _closeTaskSource.TrySetResult(null);
            }
        }

        private DataStreamerClientBuffer<TK, TV> GetOrAddBuffer(ClientSocket socket)
        {
            return _buffers.GetOrAdd(socket, (sock, streamer) => streamer.CreateBuffer(sock), this);
        }

        private void ThrowIfClosed()
        {
            if (_isClosed)
            {
                throw new ObjectDisposedException("DataStreamerClient", "Data streamer has been disposed");
            }
        }

        private void SetCloseResultIfNoActiveFlushes()
        {
            if (Interlocked.CompareExchange(ref _activeFlushes, 0, 0) == 0)
            {
                _closeTaskSource.TrySetResult(null);
            }
        }

        private DataStreamerClientBuffer<TK, TV> CreateBuffer(ClientSocket socket)
        {
            return new DataStreamerClientBuffer<TK, TV>(
                _options.ClientPerNodeBufferSize,
                buf => FlushBufferAsync(buf, socket));
        }
    }
}
