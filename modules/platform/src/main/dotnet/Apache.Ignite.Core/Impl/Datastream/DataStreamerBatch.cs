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

namespace Apache.Ignite.Core.Impl.Datastream
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Portable;

    /// <summary>
    /// Data streamer batch.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable")]
    internal class DataStreamerBatch<TK, TV>
    {
        /** Queue. */
        private readonly ConcurrentQueue<object> _queue = new ConcurrentQueue<object>();

        /** Lock for concurrency. */
        private readonly ReaderWriterLockSlim _rwLock = new ReaderWriterLockSlim();

        /** Previous batch. */
        private volatile DataStreamerBatch<TK, TV> _prev;

        /** Current queue size.*/
        private volatile int _size;
        
        /** Send guard. */
        private bool _sndGuard;

        /** */
        private readonly Future<object> _fut = new Future<object>();

        /// <summary>
        /// Constructor.
        /// </summary>
        public DataStreamerBatch() : this(null)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="prev">Previous batch.</param>
        public DataStreamerBatch(DataStreamerBatch<TK, TV> prev)
        {
            _prev = prev;

            if (prev != null)
                Thread.MemoryBarrier(); // Prevent "prev" field escape.

            _fut.Listen(() => ParentsCompleted());
        }

        /// <summary>
        /// Gets the future.
        /// </summary>
        public IFuture Future
        {
            get { return _fut; }
        }

        /// <summary>
        /// Add object to the batch.
        /// </summary>
        /// <param name="val">Value.</param>
        /// <param name="cnt">Items count.</param>
        /// <returns>Positive value in case batch is active, -1 in case no more additions are allowed.</returns>
        public int Add(object val, int cnt)
        {
            // If we cannot enter read-lock immediately, then send is scheduled and batch is definetely blocked.
            if (!_rwLock.TryEnterReadLock(0))
                return -1;

            try 
            {
                // 1. Ensure additions are possible
                if (_sndGuard)
                    return -1;

                // 2. Add data and increase size.
                _queue.Enqueue(val);

#pragma warning disable 0420
                int newSize = Interlocked.Add(ref _size, cnt);
#pragma warning restore 0420

                return newSize;
            }
            finally
            {
                _rwLock.ExitReadLock();
            }
        }

        /// <summary>
        /// Internal send routine.
        /// </summary>
        /// <param name="ldr">streamer.</param>
        /// <param name="plc">Policy.</param>
        public void Send(DataStreamerImpl<TK, TV> ldr, int plc)
        {
            // 1. Delegate to the previous batch first.
            DataStreamerBatch<TK, TV> prev0 = _prev;

            if (prev0 != null)
                prev0.Send(ldr, DataStreamerImpl<TK, TV>.PlcContinue);

            // 2. Set guard.
            _rwLock.EnterWriteLock();

            try
            {
                if (_sndGuard)
                    return;
                else
                    _sndGuard = true;
            }
            finally
            {
                _rwLock.ExitWriteLock();
            }

            var handleRegistry = ldr.Marshaller.Ignite.HandleRegistry;

            long futHnd = 0;

            // 3. Actual send.
            ldr.Update(writer =>
            {
                writer.WriteInt(plc);

                if (plc != DataStreamerImpl<TK, TV>.PlcCancelClose)
                {
                    futHnd = handleRegistry.Allocate(_fut);

                    try
                    {
                        writer.WriteLong(futHnd);

                        WriteTo(writer);
                    }
                    catch (Exception)
                    {
                        handleRegistry.Release(futHnd);

                        throw;
                    }
                }
            });

            if (plc == DataStreamerImpl<TK, TV>.PlcCancelClose || _size == 0)
            {
                _fut.OnNullResult();
                
                handleRegistry.Release(futHnd);
            }
        }


        /// <summary>
        /// Await completion of current and all previous loads.
        /// </summary>
        public void AwaitCompletion()
        {
            DataStreamerBatch<TK, TV> curBatch = this;

            while (curBatch != null)
            {
                try
                {
                    curBatch._fut.Get();
                }
                catch (Exception)
                {
                    // Ignore.
                }

                curBatch = curBatch._prev;
            }
        }

        /// <summary>
        /// Write batch content.
        /// </summary>
        /// <param name="writer">Portable writer.</param>
        private void WriteTo(PortableWriterImpl writer)
        {
            writer.WriteInt(_size);

            object val;

            while (_queue.TryDequeue(out val))
            {
                // 1. Is it a collection?
                ICollection<KeyValuePair<TK, TV>> entries = val as ICollection<KeyValuePair<TK, TV>>;

                if (entries != null)
                {
                    foreach (KeyValuePair<TK, TV> item in entries)
                    {
                        writer.Write(item.Key);
                        writer.Write(item.Value);
                    }

                    continue;
                }

                // 2. Is it a single entry?
                DataStreamerEntry<TK, TV> entry = val as DataStreamerEntry<TK, TV>;

                if (entry != null) {
                    writer.Write(entry.Key);
                    writer.Write(entry.Value);

                    continue;
                }

                // 3. Is it remove merker?
                DataStreamerRemoveEntry<TK> rmvEntry = val as DataStreamerRemoveEntry<TK>;

                if (rmvEntry != null)
                {
                    writer.Write(rmvEntry.Key);
                    writer.Write<object>(null);
                }
            }
        }

        /// <summary>
        /// Checck whether all previous batches are completed.
        /// </summary>
        /// <returns></returns>
        private bool ParentsCompleted()
        {
            DataStreamerBatch<TK, TV> prev0 = _prev;

            if (prev0 != null)
            {
                if (prev0.ParentsCompleted())
                    _prev = null;
                else
                    return false;
            }

            return _fut.IsDone;
        }
    }
}
