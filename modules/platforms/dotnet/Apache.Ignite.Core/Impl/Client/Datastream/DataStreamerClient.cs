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
        private readonly ConcurrentDictionary<ClientSocket, DataStreamerClientPerNodeBuffer<TK, TV>> _buffers =
            new ConcurrentDictionary<ClientSocket, DataStreamerClientPerNodeBuffer<TK, TV>>();

        /** */
        private readonly ConcurrentStack<DataStreamerClientEntry<TK, TV>[]> _arrayPool
            = new ConcurrentStack<DataStreamerClientEntry<TK, TV>[]>();

        /** Exception. When set, the streamer is closed. */
        private volatile Exception _exception;

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

        /** <inheritdoc /> */
        public bool IsClosed
        {
            get { return _exception != null; }
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
            return FlushAsync(false);
        }

        public void Close(bool cancel)
        {
            CloseAsync(cancel).Wait();
        }

        public Task CloseAsync(bool cancel)
        {
            if (_exception != null)
            {
                // TODO: ??
                return Task.CompletedTask;
            }

            _exception = new ObjectDisposedException("DataStreamerClient", "Data streamer has been disposed");

            if (cancel)
            {
                // Disregard current buffers, but wait for active flushes.
                // TODO: Implement!
                return Task.CompletedTask;
            }

            return FlushAsync(close: true);
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
            buffer.Add(entry);

            // Check in the end: we guarantee that if Add completes without errors, then the data won't be lost.
            // TODO: This provides unclear message - current entry may be loaded successfully.
            // The only way to fix this properly is an RW lock - use benchmarks to check how this performs.
            ThrowIfClosed();
        }

        private Task FlushAsync(bool close)
        {
            if (_buffers.IsEmpty)
            {
                return TaskRunner.CompletedTask;
            }

            var tasks = new List<Task>(_buffers.Count);

            foreach (var pair in _buffers)
            {
                var buffer = pair.Value;
                var task = buffer.FlushAllAsync(close);

                if (task != null && !task.IsCompleted)
                {
                    tasks.Add(task);
                }
            }

            return TaskRunner.WhenAll(tasks.ToArray());
        }

        internal Task FlushBufferAsync(
            DataStreamerClientBuffer<TK, TV> buffer,
            ClientSocket socket,
            SemaphoreSlim semaphore)
        {
            // TODO: WaitAsync is not available on .NET 4, but is available on .NET 4.5 and later
            // Use reflection to get it.
            // semaphore.WaitAsync();
            semaphore.Wait();

            // TODO: Flush in a loop until succeeded.
            var tcs = new TaskCompletionSource<long>();

            return socket.DoOutInOpAsync(
                    ClientOp.DataStreamerStart,
                    ctx => WriteBuffer(buffer, ctx.Writer),
                    ctx => (object)null,
                    syncCallback: true)
                .ContinueWith(t =>
                {
                    semaphore.Release();

                    // TODO: RwLock here and in other places?
                    _exception = _exception ?? t.Exception;
                }, TaskContinuationOptions.ExecuteSynchronously);
        }

        // private void FlushBuffer(
        //     DataStreamerClientBuffer<TK, TV> buffer,
        //     long? streamerId,
        //     ClientSocket socket,
        //     TaskCompletionSource<long> tcs,
        //     bool flush,
        //     bool close)
        // {
        //     socket.DoOutInOpAsync(
        //             streamerId == null ? ClientOp.DataStreamerStart : ClientOp.DataStreamerAddData,
        //             ctx => WriteBuffer(buffer, streamerId, ctx.Writer, flush, close),
        //             ctx => streamerId == null ? ctx.Stream.ReadLong() : 0,
        //             syncCallback: true)
        //         .ContinueWith(t =>
        //         {
        //             // TODO: Retry when socket disconnected.
        //             // TODO: What if server fails with some buffered data there??? This is a huge problem for perf!
        //             if (t.Exception != null)
        //             {
        //                 tcs.TrySetException(t.Exception);
        //             }
        //             else
        //             {
        //                 tcs.TrySetResult(t.Result);
        //             }
        //         }, TaskContinuationOptions.ExecuteSynchronously);
        // }

        internal DataStreamerClientEntry<TK, TV>[] GetArray()
        {
            DataStreamerClientEntry<TK,TV>[] res;

            if (_arrayPool.TryPop(out res))
            {
                // Reset buffer and return.
                for (int i = 0; i < res.Length; i++)
                {
                    res[i] = new DataStreamerClientEntry<TK, TV>();
                }

                return res;
            }

            res = new DataStreamerClientEntry<TK, TV>[_options.ClientPerNodeBufferSize];
            return res;
        }

        internal void ReturnArray(DataStreamerClientEntry<TK, TV>[] buffer)
        {
            _arrayPool.Push(buffer);
        }

        private void WriteBuffer(DataStreamerClientBuffer<TK, TV> buffer, BinaryWriter w)
        {
                w.WriteInt(_cacheId);
                w.WriteByte((byte) GetFlags());
                w.WriteInt(_options.ServerPerNodeBufferSize);
                w.WriteInt(_options.ServerPerThreadBufferSize);
                w.WriteObject(_options.Receiver);

            w.WriteInt(buffer.Count);

            var entries = buffer.Entries;

            for (var i = 0; i < entries.Length; i++)
            {
                var entry = entries[i];

                if (entry.IsEmpty)
                {
                    continue;
                }

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

            ReturnArray(entries);
        }

        private Flags GetFlags()
        {
            // TODO: Cache the flags once in ctor.
            var flags = Flags.Flush | Flags.Close;

            if (_options.AllowOverwrite)
            {
                flags |= Flags.AllowOverwrite;
            }

            if (_options.SkipStore)
            {
                flags |= Flags.SkipStore;
            }

            if (_options.KeepBinary)
            {
                flags |= Flags.KeepBinary;
            }

            return flags;
        }

        private DataStreamerClientPerNodeBuffer<TK, TV> GetOrAddBuffer(ClientSocket socket)
        {
#if NETCOREAPP
            return _buffers.GetOrAdd(socket, (sock, streamer) => streamer.CreatePerNodeBuffer(sock), this);
#else
            // Do not allocate closure on every call, only when the buffer does not exist (rare).
            DataStreamerClientPerNodeBuffer<TK,TV> res;
            return _buffers.TryGetValue(socket, out res)
                ? res
                : _buffers.GetOrAdd(socket, sock => CreateBuffer(sock));
#endif
        }

        private void ThrowIfClosed()
        {
            var ex = _exception;

            if (ex != null)
            {
                throw ex;
            }
        }

        private DataStreamerClientPerNodeBuffer<TK, TV> CreatePerNodeBuffer(ClientSocket socket)
        {
            return new DataStreamerClientPerNodeBuffer<TK, TV>(this, socket);
        }
    }
}
