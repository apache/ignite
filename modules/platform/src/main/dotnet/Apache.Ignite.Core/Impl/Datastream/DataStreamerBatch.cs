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
    internal class DataStreamerBatch<K, V>
    {
        /** Queue. */
        private readonly ConcurrentQueue<object> queue = new ConcurrentQueue<object>();

        /** Lock for concurrency. */
        private readonly ReaderWriterLockSlim rwLock = new ReaderWriterLockSlim();

        /** Previous batch. */
        private volatile DataStreamerBatch<K, V> prev;

        /** Current queue size.*/
        private volatile int size;
        
        /** Send guard. */
        private bool sndGuard;

        /** */
        private readonly Future<object> fut = new Future<object>();

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
        public DataStreamerBatch(DataStreamerBatch<K, V> prev)
        {
            this.prev = prev;

            if (prev != null)
                Thread.MemoryBarrier(); // Prevent "prev" field escape.

            fut.Listen(() => ParentsCompleted());
        }

        /// <summary>
        /// Gets the future.
        /// </summary>
        public IFuture Future
        {
            get { return fut; }
        }

        /// <summary>
        /// Add object to the batch.
        /// </summary>
        /// <param name="val">Value.</param>
        /// <param name="cnt">Items count.</param>
        /// <returns>Positive value in case batch is active, -1 in case no more additions are allowed.</returns>
        public int Add(Object val, int cnt)
        {
            // If we cannot enter read-lock immediately, then send is scheduled and batch is definetely blocked.
            if (!rwLock.TryEnterReadLock(0))
                return -1;

            try 
            {
                // 1. Ensure additions are possible
                if (sndGuard)
                    return -1;

                // 2. Add data and increase size.
                queue.Enqueue(val);

#pragma warning disable 0420
                int newSize = Interlocked.Add(ref size, cnt);
#pragma warning restore 0420

                return newSize;
            }
            finally
            {
                rwLock.ExitReadLock();
            }
        }

        /// <summary>
        /// Internal send routine.
        /// </summary>
        /// <param name="ldr">streamer.</param>
        /// <param name="plc">Policy.</param>
        public void Send(DataStreamerImpl<K, V> ldr, int plc)
        {
            // 1. Delegate to the previous batch first.
            DataStreamerBatch<K, V> prev0 = prev;

            if (prev0 != null)
                prev0.Send(ldr, DataStreamerImpl<K, V>.PLC_CONTINUE);

            // 2. Set guard.
            rwLock.EnterWriteLock();

            try
            {
                if (sndGuard)
                    return;
                else
                    sndGuard = true;
            }
            finally
            {
                rwLock.ExitWriteLock();
            }

            var handleRegistry = ldr.Marshaller.Grid.HandleRegistry;

            long futHnd = 0;

            // 3. Actual send.
            ldr.Update(writer =>
            {
                writer.WriteInt(plc);

                if (plc != DataStreamerImpl<K, V>.PLC_CANCEL_CLOSE)
                {
                    futHnd = handleRegistry.Allocate(fut);

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

            if (plc == DataStreamerImpl<K, V>.PLC_CANCEL_CLOSE || size == 0)
            {
                fut.OnNullResult();
                
                handleRegistry.Release(futHnd);
            }
        }


        /// <summary>
        /// Await completion of current and all previous loads.
        /// </summary>
        public void AwaitCompletion()
        {
            DataStreamerBatch<K, V> curBatch = this;

            while (curBatch != null)
            {
                try
                {
                    curBatch.fut.Get();
                }
                catch (Exception)
                {
                    // Ignore.
                }

                curBatch = curBatch.prev;
            }
        }

        /// <summary>
        /// Write batch content.
        /// </summary>
        /// <param name="writer">Portable writer.</param>
        private void WriteTo(PortableWriterImpl writer)
        {
            writer.WriteInt(size);

            object val;

            while (queue.TryDequeue(out val))
            {
                // 1. Is it a collection?
                ICollection<KeyValuePair<K, V>> entries = val as ICollection<KeyValuePair<K, V>>;

                if (entries != null)
                {
                    foreach (KeyValuePair<K, V> item in entries)
                    {
                        writer.Write(item.Key);
                        writer.Write(item.Value);
                    }

                    continue;
                }

                // 2. Is it a single entry?
                DataStreamerEntry<K, V> entry = val as DataStreamerEntry<K, V>;

                if (entry != null) {
                    writer.Write(entry.Key);
                    writer.Write(entry.Value);

                    continue;
                }

                // 3. Is it remove merker?
                DataStreamerRemoveEntry<K> rmvEntry = val as DataStreamerRemoveEntry<K>;

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
            DataStreamerBatch<K, V> prev0 = prev;

            if (prev0 != null)
            {
                if (prev0.ParentsCompleted())
                    prev = null;
                else
                    return false;
            }

            return fut.IsDone;
        }
    }
}
