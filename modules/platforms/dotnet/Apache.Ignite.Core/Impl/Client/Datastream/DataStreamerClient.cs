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
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Datastream;
    using Apache.Ignite.Core.Datastream;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Datastream;
    using BinaryWriter = Apache.Ignite.Core.Impl.Binary.BinaryWriter;

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
        private const int MaxRetries = 16;

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
        [SuppressMessage("Microsoft.Design", "CA2213:DisposableFieldsShouldBeDisposed",
            Justification = "WaitHandle is not used in ReaderWriterLockSlim, no need to dispose.")]
        private readonly ReaderWriterLockSlim _rwLock = new ReaderWriterLockSlim();

        /** Cached flags. */
        private readonly Flags _flags;

        /** */
        private readonly ConcurrentStack<DataStreamerClientEntry<TK, TV>[]> _arrayPool
            = new ConcurrentStack<DataStreamerClientEntry<TK, TV>[]>();

        private readonly Timer _autoFlushTimer;

        /** */
        private int _arraysAllocated;

        /** */
        private long _entriesSent;

        /** Exception. When set, the streamer is closed. */
        private volatile Exception _exception;

        /** Cancelled flag. */
        private volatile bool _cancelled;

        /// <summary>
        /// Initializes a new instance of <see cref="DataStreamerClient{TK,TV}"/>.
        /// </summary>
        /// <param name="socket">Socket.</param>
        /// <param name="cacheName">Cache name.</param>
        /// <param name="options">Options.</param>
        public DataStreamerClient(
            ClientFailoverSocket socket,
            string cacheName,
            DataStreamerClientOptions<TK, TV> options)
        {
            Debug.Assert(socket != null);
            Debug.Assert(!string.IsNullOrEmpty(cacheName));

            // Copy to prevent modification.
            _options = new DataStreamerClientOptions<TK, TV>(options);
            _socket = socket;
            _cacheName = cacheName;
            _cacheId = BinaryUtils.GetCacheId(cacheName);
            _flags = GetFlags(_options);

            var interval = _options.AutoFlushInterval;
            if (interval != TimeSpan.Zero)
            {
                _autoFlushTimer = new Timer(_ => AutoFlush(), null, interval, interval);
            }
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

        /** <inheritdoc /> */
        public DataStreamerClientOptions<TK, TV> Options
        {
            get
            {
                // Copy to prevent modification.
                return new DataStreamerClientOptions<TK, TV>(_options);
            }
        }

        /** <inheritdoc /> */
        public void Add(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            Add(new DataStreamerClientEntry<TK, TV>(key, val));
        }

        /** <inheritdoc /> */
        public void Remove(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            if (!_options.AllowOverwrite)
            {
                throw new IgniteClientException("DataStreamer can't remove data when AllowOverwrite is false.");
            }

            Add(new DataStreamerClientEntry<TK, TV>(key));
        }

        /** <inheritdoc /> */
        public void Flush()
        {
            FlushAsync().Wait();
        }

        /** <inheritdoc /> */
        public Task FlushAsync()
        {
            ThrowIfClosed();

            return FlushInternalAsync();
        }

        /** <inheritdoc /> */
        public void Close(bool cancel)
        {
            CloseAsync(cancel).Wait();
        }

        /** <inheritdoc /> */
        public Task CloseAsync(bool cancel)
        {
            _rwLock.EnterWriteLock();

            try
            {
                if (_exception != null)
                {
                    // Already closed.
                    return TaskRunner.CompletedTask;
                }

                _exception = new ObjectDisposedException("DataStreamerClient", "Data streamer has been disposed");

                if (_autoFlushTimer != null)
                {
                    _autoFlushTimer.Dispose();
                }

                if (cancel)
                {
                    // Disregard current buffers, stop all retry loops.
                    _cancelled = true;

                    return TaskRunner.CompletedTask;
                }

                return FlushInternalAsync();
            }
            finally
            {
                _rwLock.ExitWriteLock();
            }
        }

        /** <inheritdoc /> */
        public override string ToString()
        {
            return string.Format("{0} [CacheName={1}, IsClosed={2}]", GetType().Name, CacheName, IsClosed);
        }

        /// <summary>
        /// Gets the count of sent entries.
        /// </summary>
        internal long EntriesSent
        {
            get { return Interlocked.CompareExchange(ref _entriesSent, -1, -1); }
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

        /// <summary>
        /// Gets the pooled entry array.
        /// </summary>
        internal DataStreamerClientEntry<TK, TV>[] GetPooledArray()
        {
            DataStreamerClientEntry<TK,TV>[] res;

            if (_arrayPool.TryPop(out res))
            {
                // Reset buffer and return.
                Array.Clear(res, 0, res.Length);

                return res;
            }

            Interlocked.Increment(ref _arraysAllocated);
            res = new DataStreamerClientEntry<TK, TV>[_options.PerNodeBufferSize];
            return res;
        }

        /// <summary>
        /// Returns entry array to the pool.
        /// </summary>
        internal void ReturnPooledArray(DataStreamerClientEntry<TK, TV>[] buffer)
        {
            _arrayPool.Push(buffer);
        }

        /// <summary>
        /// Adds an entry to the streamer.
        /// </summary>
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

        /// <summary>
        /// Adds an entry without RW lock.
        /// </summary>
        private void AddNoLock(DataStreamerClientEntry<TK, TV> entry)
        {
            var retries = MaxRetries;

            while (!_cancelled)
            {
                try
                {
                    var socket = _socket.GetAffinitySocket(_cacheId, entry.Key) ?? _socket.GetSocket();
                    var buffer = GetOrAddPerNodeBuffer(socket);

                    if (buffer.Add(entry))
                    {
                        return;
                    }
                }
                catch (Exception e)
                {
                    if (ShouldRetry(e) && retries --> 0)
                    {
                        continue;
                    }

                    throw;
                }
            }
        }

        /// <summary>
        /// Flushes the streamer asynchronously.
        /// </summary>
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

        /// <summary>
        /// Flushes the specified buffer asynchronously.
        /// </summary>
        internal Task FlushBufferAsync(
            DataStreamerClientBuffer<TK, TV> buffer,
            ClientSocket socket,
            SemaphoreSlim semaphore)
        {
            semaphore.Wait();

            var tcs = new TaskCompletionSource<object>();

            FlushBufferInternalAsync(buffer, socket, tcs);

            return tcs.Task.ContWith(t =>
            {
                semaphore.Release();

                _exception = _exception ?? t.Exception;

                return t.Result;
            }, TaskContinuationOptions.ExecuteSynchronously);
        }

        /// <summary>
        /// Flushes the specified buffer asynchronously.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Any exception should be propagated to TaskCompletionSource.")]
        private void FlushBufferInternalAsync(
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
                    .ContWith(
                        t => FlushBufferCompleteOrRetry(buffer, socket, tcs, t.Exception),
                        TaskContinuationOptions.ExecuteSynchronously);
            }
            catch (Exception exception)
            {
                FlushBufferCompleteOrRetry(buffer, socket, tcs, exception);
            }
        }

        /// <summary>
        /// Completes or retries the buffer flush operation.
        /// </summary>
        private void FlushBufferCompleteOrRetry(
            DataStreamerClientBuffer<TK, TV> buffer,
            ClientSocket socket,
            TaskCompletionSource<object> tcs,
            Exception exception)
        {
            if (exception == null)
            {
                // Successful flush.
                Interlocked.Add(ref _entriesSent, buffer.Count);
                ReturnPooledArray(buffer.Entries);
                tcs.SetResult(null);

                return;
            }

            if (_cancelled || (!socket.IsDisposed && !ShouldRetry(exception)))
            {
                // Socket is still connected: this error does not need to be retried.
                ReturnPooledArray(buffer.Entries);
                tcs.SetException(exception);

                return;
            }

            // Release receiver thread, perform retry on a separate thread.
            ThreadPool.QueueUserWorkItem(_ => FlushBufferRetry(buffer, socket, tcs));
        }

        /// <summary>
        /// Retries the buffer flush operation.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Any exception should be propagated to TaskCompletionSource.")]
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

                    while (remaining != null)
                    {
                        if (remaining.MarkFlushed())
                        {
                            ReAddEntriesAndReturnBuffer(remaining);
                        }

                        remaining = remaining.Previous;
                    }
                }

                // Note: if initial flush was caused by full buffer, not requested by the user,
                // we don't need to force flush everything here - just re-add entries to other buffers.
                FlushInternalAsync().ContWith(flushTask => flushTask.SetAsResult(tcs));
            }
            catch (Exception e)
            {
                tcs.SetException(e);
            }
        }

        /// <summary>
        /// Re-adds obsolete buffer entries to new buffers and returns the array to the pool.
        /// </summary>
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

            ReturnPooledArray(entries);
        }

        /// <summary>
        /// Writes buffer data to the specified writer.
        /// </summary>
        private void WriteBuffer(DataStreamerClientBuffer<TK, TV> buffer, BinaryWriter w)
        {
            w.WriteInt(_cacheId);
            w.WriteByte((byte) _flags);
            w.WriteInt(ServerBufferSizeAuto); // Server per-node buffer size.
            w.WriteInt(ServerBufferSizeAuto); // Server per-thread buffer size.

            if (_options.Receiver != null)
            {
                var rcvHolder = new StreamReceiverHolder(_options.Receiver,
                    (rec, grid, cache, stream, keepBinary) =>
                        StreamReceiverHolder.InvokeReceiver((IStreamReceiver<TK, TV>) rec, grid, cache, stream,
                            keepBinary));

                w.WriteObjectDetached(rcvHolder);
                w.WriteByte(ClientPlatformId.Dotnet);
            }
            else
            {
                w.WriteObject<object>(null);
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

        /// <summary>
        /// Gets or adds per-node buffer for the specified socket.
        /// </summary>
        private DataStreamerClientPerNodeBuffer<TK, TV> GetOrAddPerNodeBuffer(ClientSocket socket)
        {
            DataStreamerClientPerNodeBuffer<TK,TV> res;
            if (_buffers.TryGetValue(socket, out res))
            {
                return res;
            }

            var candidate = new DataStreamerClientPerNodeBuffer<TK, TV>(this, socket);

            res = _buffers.GetOrAdd(socket, candidate);

            if (res != candidate)
            {
                // Another thread won - return array to the pool.
                ReturnPooledArray(candidate.Close().Entries);
            }

            return res;
        }

        /// <summary>
        /// Throws an exception if current streamer instance is closed.
        /// </summary>
        private void ThrowIfClosed()
        {
            var ex = _exception;

            if (ex == null)
            {
                return;
            }

            if (ex is ObjectDisposedException)
            {
                throw new ObjectDisposedException("Streamer is closed.");
            }

            throw new IgniteClientException("Streamer is closed with error, check inner exception for details.", ex);
        }

        /// <summary>
        /// Performs timer-based automatic flush.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Streamer will be closed if an exception occurs during automated flush. " +
                            "Timer thread should not throw.")]
        private void AutoFlush()
        {
            if (_exception != null)
            {
                return;
            }

            // Prevent multiple parallel timer calls.
            if (!Monitor.TryEnter(_autoFlushTimer))
            {
                return;
            }

            try
            {
                // Initiate flush, don't wait for completion.
                FlushInternalAsync();
            }
            catch (Exception)
            {
                // Ignore.
            }
            finally
            {
                Monitor.Exit(_autoFlushTimer);
            }
        }

        /// <summary>
        /// Gets a value indicating whether flush should be retried after the specified exception.
        /// </summary>
        private static bool ShouldRetry(Exception exception)
        {
            while (exception.InnerException != null)
            {
                exception = exception.InnerException;
            }

            if (exception is SocketException || exception is IOException)
            {
                return true;
            }

            var clientEx = exception as IgniteClientException;

            if (clientEx != null && clientEx.StatusCode == ClientStatusCode.InvalidNodeState)
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// Gets the flags.
        /// </summary>
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

            if (options.ReceiverKeepBinary)
            {
                flags |= Flags.KeepBinary;
            }

            return flags;
        }
    }
}
