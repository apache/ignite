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
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Datastream;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// TODO:
    /// * Receiver tests
    /// * keepBinary tests
    /// * Unwrap ugly AggregateExceptions. Too much nesting.
    /// * Remove diagnostic output
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

        /** Cached flags. */
        private readonly Flags _flags;

        public DataStreamerClient(
            ClientFailoverSocket socket,
            string cacheName,
            DataStreamerClientOptions<TK, TV> options)
        {
            Debug.Assert(socket != null);
            Debug.Assert(!string.IsNullOrEmpty(cacheName));

            _socket = socket;
            _cacheName = cacheName;
            _cacheId = BinaryUtils.GetCacheId(cacheName);

            // Copy to prevent modification.
            _options = new DataStreamerClientOptions<TK, TV>(options);
            _flags = GetFlags(_options);
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

        private void Add(DataStreamerClientEntry<TK, TV> entry)
        {
            if (!_rwLock.TryEnterReadLock(0))
            {
                throw new ObjectDisposedException("DataStreamerClient", "Data streamer has been disposed");
            }

            try
            {
                ThrowIfClosed();

                AddNoLock(entry);
            }
            finally
            {
                _rwLock.ExitReadLock();
            }
        }

        private void AddNoLock(DataStreamerClientEntry<TK, TV> entry)
        {
            // TODO: Limit retry count?
            while (true)
            {
                // TODO: Retry connection failures if GetSocket fails (needed for affinity awareness mode)
                var socket = _socket.GetAffinitySocket(_cacheId, entry.Key) ?? _socket.GetSocket();
                var buffer = GetOrAddBuffer(socket);

                if (buffer.Add(entry))
                {
                    return;
                }
            }
        }

        private Task FlushInternalAsync(DataStreamerClientPerNodeBuffer<TK, TV> additionalBuffer = null)
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

            if (additionalBuffer != null)
            {
                var additionalTask = additionalBuffer.FlushAllAsync();

                if (additionalTask != null && !additionalTask.IsCompleted)
                {
                    tasks.Add(additionalTask);
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

        private void FlushBuffer(
            DataStreamerClientBuffer<TK, TV> buffer,
            ClientSocket socket,
            TaskCompletionSource<object> tcs,
            bool userRequested)
        {
            try
            {
                socket.DoOutInOpAsync(
                        ClientOp.DataStreamerStart,
                        ctx => WriteBuffer(buffer, ctx.Writer),
                        ctx => (object)null,
                        syncCallback: true)
                    .ContinueWith(
                        t => FlushBufferCompleteOrRetry(buffer, socket, tcs, userRequested, t.Exception, onSocketThread: true, t),
                        TaskContinuationOptions.ExecuteSynchronously);
            }
            catch (Exception exception)
            {
                FlushBufferCompleteOrRetry(buffer, socket, tcs, userRequested, exception);
            }
        }

        private void FlushBufferCompleteOrRetry(DataStreamerClientBuffer<TK, TV> buffer,
            ClientSocket socket,
            TaskCompletionSource<object> tcs,
            bool userRequested,
            Exception exception,
            bool onSocketThread = false,
            Task prevTask = null)
        {
            // NOTE: when onSocketThread is true, we are on socket receiver thread - don't perform any heavy operations.

            if (exception == null)
            {
                PrintEntries(buffer.Entries, $"SENT (disposed={socket.IsDisposed}, task={prevTask?.Status}, socket={socket.RemoteEndPoint})");

                ReturnArray(buffer.Entries);
                tcs.SetResult(null);

                return;
            }

            if (!socket.IsDisposed && !ShouldRetry(exception))
            {
                // Socket is still connected: this error does not need to be retried.
                Console.WriteLine(">>>> NON_RETRY_ERROR: " + exception); // TODO
                ReturnArray(buffer.Entries);
                tcs.SetException(exception);

                return;
            }

            // TODO: Retry count limit (see DataStreamerImpl#DFLT_MAX_REMAP_CNT).
            if (onSocketThread)
            {
                // Release receiver thread, perform retry on a separate thread.
                ThreadPool.QueueUserWorkItem(_ => FlushBufferRetry(buffer, socket, tcs, userRequested));
            }
            else
            {
                FlushBufferRetry(buffer, socket, tcs, userRequested);
            }
        }

        private static bool ShouldRetry(Exception exception)
        {
            var aggregate = exception as AggregateException;

            if (aggregate != null)
            {
                exception = aggregate.GetBaseException();
            }

            var clientEx = exception as IgniteClientException;

            if (clientEx != null && clientEx.StatusCode == ClientStatusCode.InvalidNodeState)
            {
                return true;
            }

            return false;
        }

        private static void PrintEntries(DataStreamerClientEntry<TK, TV>[] entries, string prefix)
        {
            var sb = new StringBuilder();

            for (var i = 0; i < entries.Length; i++)
            {
                var entry = entries[i];

                if (!entry.IsEmpty)
                {
                    sb.Append(entry.Key).Append(", ");
                }
            }

            Console.WriteLine($">>>> {prefix}: {sb}");
        }

        private void FlushBufferRetry(
            DataStreamerClientBuffer<TK, TV> buffer,
            ClientSocket failedSocket,
            TaskCompletionSource<object> tcs,
            bool userRequested)
        {
            Console.WriteLine(">>>> RETRY");

            try
            {
                // Connection failed. Remove disconnected socket from the map.
                // TODO: This possibly removes a buffer that was never flushed.
                DataStreamerClientPerNodeBuffer<TK, TV> removed;
                _buffers.TryRemove(failedSocket, out removed);

                // Re-add entries to other buffers.
                var count = buffer.Count;
                var entries = buffer.Entries;

                for (var i = 0; i < count; i++)
                {
                    var entry = entries[i];

                    if (!entry.IsEmpty)
                    {
                        Console.WriteLine(">>>> RETRY " + entry.Key);
                        AddNoLock(entry);
                    }
                }

                ReturnArray(entries);

                //if (userRequested)
                {
                    // When flush is initiated by the user, we should retry flushing immediately.
                    // Otherwise re-adding entries to other buffers is enough.

                    ThreadPool.QueueUserWorkItem(_ =>
                    {
                        // TODO: Flush removed!
                        removed?.Close();

                        FlushInternalAsync()
                            .ContinueWith(flushTask => flushTask.SetAsResult(tcs));
                    });
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(">>>> Failed to retry flush: " + e);  // TODO: Retry again!

                tcs.SetException(e);
            }
        }

        private void WriteBuffer(DataStreamerClientBuffer<TK, TV> buffer, BinaryWriter w)
        {
            w.WriteInt(_cacheId);
            w.WriteByte((byte) _flags);
            w.WriteInt(ServerBufferSizeAuto); // Server per-node buffer size.
            w.WriteInt(ServerBufferSizeAuto); // Server per-thread buffer size.
            w.WriteObject(_options.Receiver);

            var count = buffer.Count;
            w.WriteInt(count);

            var entries = buffer.Entries;

            PrintEntries(entries, "SENDING ");

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

        private static Flags GetFlags(DataStreamerClientOptions options)
        {
            var flags = Flags.Flush | Flags.Close;

            if (options.AllowOverwrite)
            {
                flags |= Flags.AllowOverwrite;
            }

            if (options.SkipStore)
            {
                flags |= Flags.SkipStore;
            }

            if (options.KeepBinary)
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
