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
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Datastream;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Thin client data streamer.
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
        private readonly ReaderWriterLockSlim _rwLock = new ReaderWriterLockSlim();

        /** Cached flags. */
        private readonly Flags _flags;
        
        /** */
        private readonly ConcurrentStack<DataStreamerClientEntry<TK, TV>[]> _arrayPool
            = new ConcurrentStack<DataStreamerClientEntry<TK, TV>[]>();

        /** */
        private int _arraysAllocated;

        /** Exception. When set, the streamer is closed. */
        private volatile Exception _exception;

        /** Cancelled flag. */
        private volatile bool _cancelled;

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

        /** <inheritdoc /> */
        public void Dispose()
        {
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
                    // Already closed.
                    return Task.CompletedTask;
                }

                _exception = new ObjectDisposedException("DataStreamerClient", "Data streamer has been disposed");

                if (cancel)
                {
                    // Disregard current buffers, stop all retry loops.
                    _cancelled = true;
                    
                    return Task.CompletedTask;
                }

                return FlushInternalAsync();
            }
            finally
            {
                _rwLock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Gets the count of allocated arrays.
        /// </summary>
        internal int ArraysAllocated
        {
            get { return Interlocked.CompareExchange(ref _arraysAllocated, -1, -1); }
        }

        /// <summary>
        /// Gets the count of pooled arrays. 
        /// </summary>
        internal int ArraysPooled
        {
            get { return _arrayPool.Count; }
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

            Interlocked.Increment(ref _arraysAllocated);
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
            while (!_cancelled)
            {
                try
                {
                    var socket = _socket.GetAffinitySocket(_cacheId, entry.Key) ?? _socket.GetSocket();
                    var buffer = GetOrAddBuffer(socket);

                    if (buffer.Add(entry))
                    {
                        return;
                    }
                }
                catch (Exception e)
                {
                    if (ShouldRetry(e))
                    {
                        continue;
                    }

                    throw;
                }
            }
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

        internal Task FlushBufferAsync(DataStreamerClientBuffer<TK, TV> buffer, ClientSocket socket,
            SemaphoreSlim semaphore)
        {
            semaphore.Wait();

            var tcs = new TaskCompletionSource<object>();

            FlushBuffer(buffer, socket, tcs);

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
            TaskCompletionSource<object> tcs)
        {
            try
            {
                socket.DoOutInOpAsync(
                        ClientOp.DataStreamerStart,
                        ctx => WriteBuffer(buffer, ctx.Writer),
                        ctx => (object)null,
                        syncCallback: true)
                    .ContinueWith(
                        t => FlushBufferCompleteOrRetry(buffer, socket, tcs, t.Exception),
                        TaskContinuationOptions.ExecuteSynchronously);
            }
            catch (Exception exception)
            {
                FlushBufferCompleteOrRetry(buffer, socket, tcs, exception);
            }
        }

        private void FlushBufferCompleteOrRetry(DataStreamerClientBuffer<TK, TV> buffer,
            ClientSocket socket,
            TaskCompletionSource<object> tcs,
            Exception exception)
        {
            if (exception == null)
            {
                ReturnArray(buffer.Entries);
                tcs.SetResult(null);

                return;
            }

            if (_cancelled || (!socket.IsDisposed && !ShouldRetry(exception)))
            {
                // Socket is still connected: this error does not need to be retried.
                ReturnArray(buffer.Entries);
                tcs.SetException(exception);

                return;
            }

            // Release receiver thread, perform retry on a separate thread.
            ThreadPool.QueueUserWorkItem(_ => FlushBufferRetry(buffer, socket, tcs));
        }

        private static bool ShouldRetry(Exception exception)
        {
            var aggregate = exception as AggregateException;

            if (aggregate != null)
            {
                exception = aggregate.GetBaseException();
            }

            if (exception is SocketException)
            {
                return true;
            }
            
            var clientEx = exception as IgniteClientException;

            if (clientEx != null && 
                (clientEx.InnerException is SocketException || 
                 clientEx.StatusCode == ClientStatusCode.InvalidNodeState))
            {
                return true;
            }

            return false;
        }

        private void FlushBufferRetry(
            DataStreamerClientBuffer<TK, TV> buffer,
            ClientSocket failedSocket,
            TaskCompletionSource<object> tcs)
        {
            try
            {
                // Connection failed. Remove disconnected socket from the map.
                DataStreamerClientPerNodeBuffer<TK, TV> removed;
                _buffers.TryRemove(failedSocket, out removed);

                // Re-add entries to other buffers.
                ReAddEntriesAndReturnBuffer(buffer);

                if (removed != null)
                {
                    var remaining = removed.Close();

                    if (remaining != null)
                    {
                        ReAddEntriesAndReturnBuffer(remaining);
                    }
                }

                FlushInternalAsync().ContinueWith(flushTask => flushTask.SetAsResult(tcs));
            }
            catch (Exception e)
            {
                tcs.SetException(e);
            }
        }

        private void ReAddEntriesAndReturnBuffer(DataStreamerClientBuffer<TK, TV> buffer)
        {
            var count = buffer.Count;
            var entries = buffer.Entries;

            for (var i = 0; i < count; i++)
            {
                var entry = entries[i];

                if (!entry.IsEmpty)
                {
                    AddNoLock(entry);
                }
            }

            ReturnArray(entries);
        }

        private void WriteBuffer(DataStreamerClientBuffer<TK, TV> buffer, BinaryWriter w)
        {
            w.WriteInt(_cacheId);
            w.WriteByte((byte) _flags);
            w.WriteInt(ServerBufferSizeAuto); // Server per-node buffer size.
            w.WriteInt(ServerBufferSizeAuto); // Server per-thread buffer size.
            w.WriteObject(_options.Receiver);

            if (_options.Receiver != null)
            {
                w.WriteByte(ClientPlatformId.Dotnet);
            }

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
            DataStreamerClientPerNodeBuffer<TK,TV> res;
            if (_buffers.TryGetValue(socket, out res))
            {
                return res;
            }

            var candidate = CreatePerNodeBuffer(socket);

            res = _buffers.GetOrAdd(socket, candidate);

            if (res != candidate)
            {
                // Another thread won - return array to the pool.
                ReturnArray(candidate.Close().Entries);
            }

            return res;
        }

        private void ThrowIfClosed()
        {
            var ex = _exception;

            if (ex != null)
            {
                throw new ObjectDisposedException("Streamer is closed.", ex);
            }
        }

        private DataStreamerClientPerNodeBuffer<TK, TV> CreatePerNodeBuffer(ClientSocket socket)
        {
            return new DataStreamerClientPerNodeBuffer<TK, TV>(this, socket);
        }
    }
}
