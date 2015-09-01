/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Datastream
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Common;
    using GridGain.Common;
    using GridGain.Datastream;
    using GridGain.Impl.Common;
    using GridGain.Impl.Portable;
    using GridGain.Impl.Unmanaged;

    using U = GridUtils;
    using UU = GridGain.Impl.Unmanaged.UnmanagedUtils;
    using A = Apache.Ignite.Core.Impl.Common.GridArgumentCheck;

    /// <summary>
    /// Data streamer internal interface to get rid of generics.
    /// </summary>
    internal interface IDataStreamer
    {
        /// <summary>
        /// Callback invoked on topology size change.
        /// </summary>
        /// <param name="topVer">New topology version.</param>
        /// <param name="topSize">New topology size.</param>
        void TopologyChange(long topVer, int topSize);
    }

    /// <summary>
    /// Data streamer implementation.
    /// </summary>
    internal class DataStreamerImpl<K, V> : GridDisposableTarget, IDataStreamer, IDataStreamer<K, V>
    {

#pragma warning disable 0420

        /** Policy: continue. */
        internal const int PLC_CONTINUE = 0;

        /** Policy: close. */
        internal const int PLC_CLOSE = 1;

        /** Policy: cancel and close. */
        internal const int PLC_CANCEL_CLOSE = 2;

        /** Policy: flush. */
        internal const int PLC_FLUSH = 3;
        
        /** Operation: update. */
        private const int OP_UPDATE = 1;
        
        /** Operation: set receiver. */
        private const int OP_RECEIVER = 2;
        
        /** Cache name. */
        private readonly string cacheName;

        /** Lock. */
        private readonly ReaderWriterLockSlim rwLock = new ReaderWriterLockSlim();

        /** Closed event. */
        private readonly ManualResetEventSlim closedEvt = new ManualResetEventSlim(false);

        /** Close future. */
        private readonly Future<object> closeFut = new Future<object>();

        /** GC handle to this streamer. */
        private readonly long hnd;
                
        /** Topology version. */
        private long topVer;

        /** Topology size. */
        private int topSize;
        
        /** Buffer send size. */
        private volatile int bufSndSize;

        /** Current data streamer batch. */
        private volatile DataStreamerBatch<K, V> batch;

        /** Flusher. */
        private readonly Flusher<K, V> flusher;

        /** Receiver. */
        private volatile IStreamReceiver<K, V> rcv;

        /** Receiver handle. */
        private long rcvHnd;

        /** Receiver portable mode. */
        private readonly bool keepPortable;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="cacheName">Cache name.</param>
        /// <param name="keepPortable">Portable flag.</param>
        public DataStreamerImpl(IUnmanagedTarget target, PortableMarshaller marsh, string cacheName, bool keepPortable)
            : base(target, marsh)
        {
            this.cacheName = cacheName;
            this.keepPortable = keepPortable;

            // Create empty batch.
            batch = new DataStreamerBatch<K, V>();

            // Allocate GC handle so that this data streamer could be easily dereferenced from native code.
            WeakReference thisRef = new WeakReference(this);

            hnd = marsh.Grid.HandleRegistry.Allocate(thisRef);

            // Start topology listening. This call will ensure that buffer size member is updated.
            UU.DataStreamerListenTopology(target, hnd);

            // Membar to ensure fields initialization before leaving constructor.
            Thread.MemoryBarrier();

            // Start flusher after everything else is initialized.
            flusher = new Flusher<K, V>(thisRef);

            flusher.RunThread();
        }

        /** <inheritDoc /> */
        public string CacheName
        {
            get { return cacheName; }
        }

        /** <inheritDoc /> */
        public bool AllowOverwrite
        {
            get
            {
                rwLock.EnterReadLock();

                try
                {
                    ThrowIfDisposed();

                    return UU.DataStreamerAllowOverwriteGet(target);
                }
                finally
                {
                    rwLock.ExitReadLock();
                }
            }
            set
            {
                rwLock.EnterWriteLock();

                try
                {
                    ThrowIfDisposed();

                    UU.DataStreamerAllowOverwriteSet(target, value);
                }
                finally
                {
                    rwLock.ExitWriteLock();
                }
            }
        }

        /** <inheritDoc /> */
        public bool SkipStore
        {
            get
            {
                rwLock.EnterReadLock(); 
                
                try
                {
                    ThrowIfDisposed();

                    return UU.DataStreamerSkipStoreGet(target);
                }
                finally
                {
                    rwLock.ExitReadLock();
                }
            }
            set
            {
                rwLock.EnterWriteLock(); 
                
                try
                {
                    ThrowIfDisposed();

                    UU.DataStreamerSkipStoreSet(target, value);
                }
                finally
                {
                    rwLock.ExitWriteLock();
                }
            }
        }

        /** <inheritDoc /> */
        public int PerNodeBufferSize
        {
            get
            {
                rwLock.EnterReadLock(); 
                
                try
                {
                    ThrowIfDisposed();

                    return UU.DataStreamerPerNodeBufferSizeGet(target);
                }
                finally
                {
                    rwLock.ExitReadLock();
                }
            }
            set
            {
                rwLock.EnterWriteLock(); 
                
                try
                {
                    ThrowIfDisposed();

                    UU.DataStreamerPerNodeBufferSizeSet(target, value);

                    bufSndSize = topSize * value;
                }
                finally
                {
                    rwLock.ExitWriteLock();
                }
            }
        }

        /** <inheritDoc /> */
        public int PerNodeParallelOperations
        {
            get
            {
                rwLock.EnterReadLock(); 
                
                try
                {
                    ThrowIfDisposed();

                    return UU.DataStreamerPerNodeParallelOperationsGet(target);
                }
                finally
                {
                    rwLock.ExitReadLock();
                }

            }
            set
            {
                rwLock.EnterWriteLock(); 
                
                try
                {
                    ThrowIfDisposed();

                    UU.DataStreamerPerNodeParallelOperationsSet(target, value);
                }
                finally
                {
                    rwLock.ExitWriteLock();
                }

            }
        }

        /** <inheritDoc /> */
        public long AutoFlushFrequency
        {
            get
            {
                rwLock.EnterReadLock(); 
                
                try
                {
                    ThrowIfDisposed();

                    return flusher.Frequency;
                }
                finally
                {
                    rwLock.ExitReadLock();
                }

            }
            set
            {
                rwLock.EnterWriteLock(); 
                
                try
                {
                    ThrowIfDisposed();

                    flusher.Frequency = value;
                }
                finally
                {
                    rwLock.ExitWriteLock();
                }
            }
        }

        /** <inheritDoc /> */
        public IFuture Future
        {
            get
            {
                ThrowIfDisposed();

                return closeFut;
            }
        }

        /** <inheritDoc /> */
        public IStreamReceiver<K, V> Receiver
        {
            get
            {
                ThrowIfDisposed();

                return rcv;
            }
            set
            {
                A.NotNull(value, "value");

                var handleRegistry = marsh.Grid.HandleRegistry;

                rwLock.EnterWriteLock();

                try
                {
                    ThrowIfDisposed();

                    if (rcv == value)
                        return;

                    var rcvHolder = new StreamReceiverHolder(value,
                        (rec, grid, cache, stream, keepPortable) =>
                            StreamReceiverHolder.InvokeReceiver((IStreamReceiver<K, V>) rec, grid, cache, stream,
                                keepPortable));

                    var rcvHnd0 = handleRegistry.Allocate(rcvHolder);

                    try
                    {
                        DoOutOp(OP_RECEIVER, w =>
                        {
                            w.WriteLong(rcvHnd0);

                            w.WriteObject(rcvHolder);
                        });
                    }
                    catch (Exception)
                    {
                        handleRegistry.Release(rcvHnd0);
                        throw;
                    }

                    if (rcv != null)
                        handleRegistry.Release(rcvHnd);

                    rcv = value;
                    rcvHnd = rcvHnd0;
                }
                finally
                {
                    rwLock.ExitWriteLock();
                }
            }
        }

        /** <inheritDoc /> */
        public IFuture AddData(K key, V val)
        {
            ThrowIfDisposed(); 
            
            A.NotNull(key, "key");

            return Add0(new DataStreamerEntry<K, V>(key, val), 1);
        }

        /** <inheritDoc /> */
        public IFuture AddData(KeyValuePair<K, V> pair)
        {
            ThrowIfDisposed();

            return Add0(new DataStreamerEntry<K, V>(pair.Key, pair.Value), 1);
        }
        
        /** <inheritDoc /> */
        public IFuture AddData(ICollection<KeyValuePair<K, V>> entries)
        {
            ThrowIfDisposed();

            A.NotNull(entries, "entries");

            return Add0(entries, entries.Count);
        }

        /** <inheritDoc /> */
        public IFuture RemoveData(K key)
        {
            ThrowIfDisposed();

            A.NotNull(key, "key");

            return Add0(new DataStreamerRemoveEntry<K>(key), 1);
        }

        /** <inheritDoc /> */
        public void TryFlush()
        {
            ThrowIfDisposed();

            DataStreamerBatch<K, V> batch0 = batch;

            if (batch0 != null)
                Flush0(batch0, false, PLC_FLUSH);
        }

        /** <inheritDoc /> */
        public void Flush()
        {
            ThrowIfDisposed();

            DataStreamerBatch<K, V> batch0 = batch;

            if (batch0 != null)
                Flush0(batch0, true, PLC_FLUSH);
            else 
            {
                // Batch is null, i.e. data streamer is closing. Wait for close to complete.
                closedEvt.Wait();
            }
        }

        /** <inheritDoc /> */
        public void Close(bool cancel)
        {
            flusher.Stop();

            while (true)
            {
                DataStreamerBatch<K, V> batch0 = batch;

                if (batch0 == null)
                {
                    // Wait for concurrent close to finish.
                    closedEvt.Wait();

                    return;
                }

                if (Flush0(batch0, true, cancel ? PLC_CANCEL_CLOSE : PLC_CLOSE))
                {
                    closeFut.OnDone(null, null);

                    rwLock.EnterWriteLock(); 
                    
                    try
                    {
                        base.Dispose(true);

                        if (rcv != null)
                            marsh.Grid.HandleRegistry.Release(rcvHnd);

                        closedEvt.Set();
                    }
                    finally
                    {
                        rwLock.ExitWriteLock();
                    }

                    marsh.Grid.HandleRegistry.Release(hnd);

                    break;
                }
            }
        }

        /** <inheritDoc /> */
        public IDataStreamer<K1, V1> WithKeepPortable<K1, V1>()
        {
            if (keepPortable)
            {
                var result = this as IDataStreamer<K1, V1>;

                if (result == null)
                    throw new InvalidOperationException(
                        "Can't change type of portable streamer. WithKeepPortable has been called on an instance of " +
                        "portable streamer with incompatible generic arguments.");

                return result;
            }

            return new DataStreamerImpl<K1, V1>(UU.ProcessorDataStreamer(marsh.Grid.InteropProcessor, cacheName, true), 
                marsh, cacheName, true);
        }

        /** <inheritDoc /> */
        protected override void Dispose(bool disposing)
        {
            if (disposing)
                Close(false);  // Normal dispose: do not cancel
            else
            {
                // Finalizer: just close Java streamer
                try
                {
                    if (batch != null)
                        batch.Send(this, PLC_CANCEL_CLOSE);
                }
                catch (Exception)
                {
                    // Finalizers should never throw
                }

                marsh.Grid.HandleRegistry.Release(hnd, true);
                marsh.Grid.HandleRegistry.Release(rcvHnd, true);

                base.Dispose(false);
            }
        }

        /** <inheritDoc /> */
        ~DataStreamerImpl()
        {
            Dispose(false);
        }

        /** <inheritDoc /> */
        public void TopologyChange(long topVer, int topSize)
        {
            rwLock.EnterWriteLock(); 
            
            try
            {
                ThrowIfDisposed();

                if (this.topVer < topVer)
                {
                    this.topVer = topVer;
                    this.topSize = topSize;

                    bufSndSize = topSize * UU.DataStreamerPerNodeBufferSizeGet(target);
                }
            }
            finally
            {
                rwLock.ExitWriteLock();
            }

        }

        /// <summary>
        /// Internal add/remove routine.
        /// </summary>
        /// <param name="val">Value.</param>
        /// <param name="cnt">Items count.</param>
        /// <returns>Future.</returns>
        private IFuture Add0(object val, int cnt)
        {
            int bufSndSize0 = bufSndSize;

            while (true)
            {
                var batch0 = batch;

                if (batch0 == null)
                    throw new InvalidOperationException("Data streamer is stopped.");

                int size = batch0.Add(val, cnt);

                if (size == -1)
                {
                    // Batch is blocked, perform CAS.
                    Interlocked.CompareExchange(ref batch,
                        new DataStreamerBatch<K, V>(batch0), batch0);

                    continue;
                }
                if (size >= bufSndSize0)
                    // Batch is too big, schedule flush.
                    Flush0(batch0, false, PLC_CONTINUE);

                return batch0.Future;
            }
        }

        /// <summary>
        /// Internal flush routine.
        /// </summary>
        /// <param name="curBatch"></param>
        /// <param name="wait">Whether to wait for flush to complete.</param>
        /// <param name="plc">Whether this is the last batch.</param>
        /// <returns>Whether this call was able to CAS previous batch</returns>
        private bool Flush0(DataStreamerBatch<K, V> curBatch, bool wait, int plc)
        {
            // 1. Try setting new current batch to help further adders. 
            bool res = Interlocked.CompareExchange(ref batch, 
                (plc == PLC_CONTINUE || plc == PLC_FLUSH) ? 
                new DataStreamerBatch<K, V>(curBatch) : null, curBatch) == curBatch;

            // 2. Perform actual send.
            curBatch.Send(this, plc);

            if (wait)
                // 3. Wait for all futures to finish.
                curBatch.AwaitCompletion();

            return res;
        }

        /// <summary>
        /// Start write.
        /// </summary>
        /// <returns>Writer.</returns>
        internal void Update(Action<PortableWriterImpl> action)
        {
            rwLock.EnterReadLock();

            try
            {
                ThrowIfDisposed();

                DoOutOp(OP_UPDATE, action);
            }
            finally
            {
                rwLock.ExitReadLock();
            }
        }

        /// <summary>
        /// Flusher.
        /// </summary>
        private class Flusher<K1, V1>
        {
            /** State: running. */
            private const int STATE_RUNNING = 0;

            /** State: stopping. */
            private const int STATE_STOPPING = 1;

            /** State: stopped. */
            private const int STATE_STOPPED = 2;

            /** Data streamer. */
            private readonly WeakReference ldrRef;

            /** Finish flag. */
            private int state;

            /** Flush frequency. */
            private long freq;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="ldrRef">Data streamer weak reference..</param>
            public Flusher(WeakReference ldrRef)
            {
                this.ldrRef = ldrRef;

                lock (this)
                {
                    state = STATE_RUNNING;
                }
            }

            /// <summary>
            /// Main flusher routine.
            /// </summary>
            public void Run()
            {
                bool force = false;
                long curFreq = 0;
                
                try
                {
                    while (true)
                    {
                        if (curFreq > 0 || force)
                        {
                            var ldr = ldrRef.Target as DataStreamerImpl<K1, V1>;

                            if (ldr == null)
                                return;

                            ldr.TryFlush();

                            force = false;
                        }

                        lock (this)
                        {
                            // Stop immediately.
                            if (state == STATE_STOPPING)
                                return;

                            if (curFreq == freq)
                            {
                                // Frequency is unchanged
                                if (curFreq == 0)
                                    // Just wait for a second and re-try.
                                    Monitor.Wait(this, 1000);
                                else
                                {
                                    // Calculate remaining time.
                                    DateTime now = DateTime.Now;

                                    long ticks;

                                    try
                                    {
                                        ticks = now.AddMilliseconds(curFreq).Ticks - now.Ticks;

                                        if (ticks > Int32.MaxValue)
                                            ticks = Int32.MaxValue;
                                    }
                                    catch (ArgumentOutOfRangeException)
                                    {
                                        // Handle possible overflow.
                                        ticks = Int32.MaxValue;
                                    }

                                    Monitor.Wait(this, TimeSpan.FromTicks(ticks));
                                }
                            }
                            else
                            {
                                if (curFreq != 0)
                                    force = true;

                                curFreq = freq;
                            } 
                        }
                    }
                }
                finally
                {
                    // Let streamer know about stop.
                    lock (this)
                    {
                        state = STATE_STOPPED;

                        Monitor.PulseAll(this);
                    }
                }
            }
            
            /// <summary>
            /// Frequency.
            /// </summary>
            public long Frequency
            {
                get
                {
                    return Interlocked.Read(ref freq);
                }

                set
                {
                    lock (this)
                    {
                        if (freq != value)
                        {
                            freq = value;

                            Monitor.PulseAll(this);
                        }
                    }
                }
            }

            /// <summary>
            /// Stop flusher.
            /// </summary>
            public void Stop()
            {
                lock (this)
                {
                    if (state == STATE_RUNNING)
                    {
                        state = STATE_STOPPING;

                        Monitor.PulseAll(this);
                    }

                    while (state != STATE_STOPPED)
                        Monitor.Wait(this);
                }
            }

            /// <summary>
            /// Runs the flusher thread.
            /// </summary>
            public void RunThread()
            {
                new Thread(Run).Start();
            }
        }

#pragma warning restore 0420

    }
}
