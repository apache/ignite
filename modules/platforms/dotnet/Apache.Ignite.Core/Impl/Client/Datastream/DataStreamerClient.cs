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
    /// TODO:
    /// * Simplify server-side part since we don't do stateful? Or keep as is?
    /// * Tune server-side buffer sizes for stateless mode - always match the count
    /// * Receiver tests
    /// * keepBinary tests
    /// * allowOverwrite tests
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
        private const int ServerBufferSizeAuto = -1;

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

        /** */
        private readonly ReaderWriterLockSlim _rwLock = new ReaderWriterLockSlim();

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
            ThrowIfClosed();

            return FlushInternalAsync();
        }

        public void Close(bool cancel)
        {
            CloseAsync(cancel).Wait();
        }

        public Task CloseAsync(bool cancel)
        {
            _rwLock.EnterWriteLock();

            try
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

                return FlushInternalAsync();
            }
            finally
            {
                _rwLock.ExitWriteLock();
            }
        }

        private void Add(DataStreamerClientEntry<TK, TV> entry)
        {
            if (!_rwLock.TryEnterReadLock(0))
            {
                throw new ObjectDisposedException("DataStreamerClient", "Data streamer has been disposed");
            }

            try
            {
                ThrowIfClosed();

                // TODO: Some buffers may become abandoned when a socket for them is disconnected
                // or server node leaves the cluster.
                // We should track such topology changes by subscribing to topology update events.
                AddNoLock(entry);
            }
            finally
            {
                _rwLock.ExitReadLock();
            }
        }

        private void AddNoLock(DataStreamerClientEntry<TK, TV> entry)
        {
            var socket = _socket.GetAffinitySocket(_cacheId, entry.Key) ?? _socket.GetSocket();
            var buffer = GetOrAddBuffer(socket);
            buffer.Add(entry);
        }

        private Task FlushInternalAsync()
        {
            if (_buffers.IsEmpty)
            {
                return TaskRunner.CompletedTask;
            }

            var tasks = new List<Task>(_buffers.Count);

            foreach (var pair in _buffers)
            {
                var buffer = pair.Value;
                var task = buffer.FlushAllAsync();

                if (task != null && !task.IsCompleted)
                {
                    tasks.Add(task);
                }
            }

            return TaskRunner.WhenAll(tasks.ToArray());
        }

        internal Task FlushBufferAsync(DataStreamerClientBuffer<TK, TV> buffer,
            ClientSocket socket,
            SemaphoreSlim semaphore, bool userRequested)
        {
            // TODO: WaitAsync is not available on .NET 4, but is available on .NET 4.5 and later
            // Use reflection to get it.
            // semaphore.WaitAsync();
            semaphore.Wait();

            // TODO: Flush in a loop until succeeded.
            var tcs = new TaskCompletionSource<object>();

            FlushBuffer(buffer, socket, tcs, userRequested);

            return tcs.Task.ContinueWith(t =>
            {
                semaphore.Release();

                _exception = _exception ?? t.Exception;

                return t.Result;
            }, TaskContinuationOptions.ExecuteSynchronously);
        }

        private void FlushBuffer(DataStreamerClientBuffer<TK, TV> buffer,
            ClientSocket socket,
            TaskCompletionSource<object> tcs,
            bool userRequested)
        {
            socket.DoOutInOpAsync(
                    ClientOp.DataStreamerStart,
                    ctx => WriteBuffer(buffer, ctx.Writer),
                    ctx => (object)null,
                    syncCallback: true)
                .ContinueWith(t =>
                {
                    // NOTE: We are on socket receiver thread here - don't perform any heavy operations.
                    var entries = buffer.Entries;

                    if (t.Exception == null)
                    {
                        ReturnArray(entries);
                        tcs.SetResult(null);

                        return;
                    }

                    if (!socket.IsDisposed)
                    {
                        // Socket is still connected: this error does not need to be retried.
                        ReturnArray(entries);
                        tcs.SetException(t.Exception);

                        return;
                    }

                    // Release receiver thread, perform retry on a separate thread.
                    // TODO: Retry count limit.
                    ThreadPool.QueueUserWorkItem(_ =>
                    {
                        try
                        {
                            // Connection failed. Remove disconnected socket from the map.
                            DataStreamerClientPerNodeBuffer<TK, TV> unused;
                            _buffers.TryRemove(socket, out unused);

                            // Re-add entries to other buffers.
                            var count = buffer.Count;

                            for (var i = 0; i < count; i++)
                            {
                                var entry = entries[i];

                                if (!entry.IsEmpty)
                                {
                                    AddNoLock(entry);
                                }
                            }

                            // TODO: Flush only when requested by the user!
                            // TODO: What if we are in Close mode?
                            if (userRequested)
                            {
                                // When flush is initiated by the user, we should retry flushing immediately.
                                // Otherwise re-adding entries to other buffers is enough.
                                FlushInternalAsync().ContinueWith(flushTask => flushTask.SetAsResult(tcs));
                            }
                        }
                        catch (Exception e)
                        {
                            tcs.SetException(e);
                        }
                        finally
                        {
                            ReturnArray(entries);
                        }
                    });
                }, TaskContinuationOptions.ExecuteSynchronously);
        }

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

            res = new DataStreamerClientEntry<TK, TV>[_options.PerNodeBufferSize];
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
            w.WriteInt(ServerBufferSizeAuto); // Server per-node buffer size.
            w.WriteInt(ServerBufferSizeAuto); // Server per-thread buffer size.
            w.WriteObject(_options.Receiver);

            var count = buffer.Count;
            w.WriteInt(count);

            var entries = buffer.Entries;

            for (var i = 0; i < count; i++)
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
