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
    using System.Collections.Generic;
    using System.Threading;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Datastream;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

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
    internal class DataStreamerImpl<TK, TV> : PlatformDisposableTarget, IDataStreamer, IDataStreamer<TK, TV>
    {

#pragma warning disable 0420

        /** Policy: continue. */
        internal const int PlcContinue = 0;

        /** Policy: close. */
        internal const int PlcClose = 1;

        /** Policy: cancel and close. */
        internal const int PlcCancelClose = 2;

        /** Policy: flush. */
        internal const int PlcFlush = 3;
        
        /** Operation: update. */
        private const int OpUpdate = 1;
        
        /** Operation: set receiver. */
        private const int OpReceiver = 2;
        
        /** Cache name. */
        private readonly string _cacheName;

        /** Lock. */
        private readonly ReaderWriterLockSlim _rwLock = new ReaderWriterLockSlim();

        /** Closed event. */
        private readonly ManualResetEventSlim _closedEvt = new ManualResetEventSlim(false);

        /** Close future. */
        private readonly Future<object> _closeFut = new Future<object>();

        /** GC handle to this streamer. */
        private readonly long _hnd;
                
        /** Topology version. */
        private long _topVer;

        /** Topology size. */
        private int _topSize;
        
        /** Buffer send size. */
        private volatile int _bufSndSize;

        /** Current data streamer batch. */
        private volatile DataStreamerBatch<TK, TV> _batch;

        /** Flusher. */
        private readonly Flusher<TK, TV> _flusher;

        /** Receiver. */
        private volatile IStreamReceiver<TK, TV> _rcv;

        /** Receiver handle. */
        private long _rcvHnd;

        /** Receiver portable mode. */
        private readonly bool _keepPortable;

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
            _cacheName = cacheName;
            _keepPortable = keepPortable;

            // Create empty batch.
            _batch = new DataStreamerBatch<TK, TV>();

            // Allocate GC handle so that this data streamer could be easily dereferenced from native code.
            WeakReference thisRef = new WeakReference(this);

            _hnd = marsh.Ignite.HandleRegistry.Allocate(thisRef);

            // Start topology listening. This call will ensure that buffer size member is updated.
            UU.DataStreamerListenTopology(target, _hnd);

            // Membar to ensure fields initialization before leaving constructor.
            Thread.MemoryBarrier();

            // Start flusher after everything else is initialized.
            _flusher = new Flusher<TK, TV>(thisRef);

            _flusher.RunThread();
        }

        /** <inheritDoc /> */
        public string CacheName
        {
            get { return _cacheName; }
        }

        /** <inheritDoc /> */
        public bool AllowOverwrite
        {
            get
            {
                _rwLock.EnterReadLock();

                try
                {
                    ThrowIfDisposed();

                    return UU.DataStreamerAllowOverwriteGet(Target);
                }
                finally
                {
                    _rwLock.ExitReadLock();
                }
            }
            set
            {
                _rwLock.EnterWriteLock();

                try
                {
                    ThrowIfDisposed();

                    UU.DataStreamerAllowOverwriteSet(Target, value);
                }
                finally
                {
                    _rwLock.ExitWriteLock();
                }
            }
        }

        /** <inheritDoc /> */
        public bool SkipStore
        {
            get
            {
                _rwLock.EnterReadLock(); 
                
                try
                {
                    ThrowIfDisposed();

                    return UU.DataStreamerSkipStoreGet(Target);
                }
                finally
                {
                    _rwLock.ExitReadLock();
                }
            }
            set
            {
                _rwLock.EnterWriteLock(); 
                
                try
                {
                    ThrowIfDisposed();

                    UU.DataStreamerSkipStoreSet(Target, value);
                }
                finally
                {
                    _rwLock.ExitWriteLock();
                }
            }
        }

        /** <inheritDoc /> */
        public int PerNodeBufferSize
        {
            get
            {
                _rwLock.EnterReadLock(); 
                
                try
                {
                    ThrowIfDisposed();

                    return UU.DataStreamerPerNodeBufferSizeGet(Target);
                }
                finally
                {
                    _rwLock.ExitReadLock();
                }
            }
            set
            {
                _rwLock.EnterWriteLock(); 
                
                try
                {
                    ThrowIfDisposed();

                    UU.DataStreamerPerNodeBufferSizeSet(Target, value);

                    _bufSndSize = _topSize * value;
                }
                finally
                {
                    _rwLock.ExitWriteLock();
                }
            }
        }

        /** <inheritDoc /> */
        public int PerNodeParallelOperations
        {
            get
            {
                _rwLock.EnterReadLock(); 
                
                try
                {
                    ThrowIfDisposed();

                    return UU.DataStreamerPerNodeParallelOperationsGet(Target);
                }
                finally
                {
                    _rwLock.ExitReadLock();
                }

            }
            set
            {
                _rwLock.EnterWriteLock(); 
                
                try
                {
                    ThrowIfDisposed();

                    UU.DataStreamerPerNodeParallelOperationsSet(Target, value);
                }
                finally
                {
                    _rwLock.ExitWriteLock();
                }

            }
        }

        /** <inheritDoc /> */
        public long AutoFlushFrequency
        {
            get
            {
                _rwLock.EnterReadLock(); 
                
                try
                {
                    ThrowIfDisposed();

                    return _flusher.Frequency;
                }
                finally
                {
                    _rwLock.ExitReadLock();
                }

            }
            set
            {
                _rwLock.EnterWriteLock(); 
                
                try
                {
                    ThrowIfDisposed();

                    _flusher.Frequency = value;
                }
                finally
                {
                    _rwLock.ExitWriteLock();
                }
            }
        }

        /** <inheritDoc /> */
        public IFuture Future
        {
            get
            {
                ThrowIfDisposed();

                return _closeFut;
            }
        }

        /** <inheritDoc /> */
        public IStreamReceiver<TK, TV> Receiver
        {
            get
            {
                ThrowIfDisposed();

                return _rcv;
            }
            set
            {
                IgniteArgumentCheck.NotNull(value, "value");

                var handleRegistry = Marshaller.Ignite.HandleRegistry;

                _rwLock.EnterWriteLock();

                try
                {
                    ThrowIfDisposed();

                    if (_rcv == value)
                        return;

                    var rcvHolder = new StreamReceiverHolder(value,
                        (rec, grid, cache, stream, keepPortable) =>
                            StreamReceiverHolder.InvokeReceiver((IStreamReceiver<TK, TV>) rec, grid, cache, stream,
                                keepPortable));

                    var rcvHnd0 = handleRegistry.Allocate(rcvHolder);

                    try
                    {
                        DoOutOp(OpReceiver, w =>
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

                    if (_rcv != null)
                        handleRegistry.Release(_rcvHnd);

                    _rcv = value;
                    _rcvHnd = rcvHnd0;
                }
                finally
                {
                    _rwLock.ExitWriteLock();
                }
            }
        }

        /** <inheritDoc /> */
        public IFuture AddData(TK key, TV val)
        {
            ThrowIfDisposed(); 
            
            IgniteArgumentCheck.NotNull(key, "key");

            return Add0(new DataStreamerEntry<TK, TV>(key, val), 1);
        }

        /** <inheritDoc /> */
        public IFuture AddData(KeyValuePair<TK, TV> pair)
        {
            ThrowIfDisposed();

            return Add0(new DataStreamerEntry<TK, TV>(pair.Key, pair.Value), 1);
        }
        
        /** <inheritDoc /> */
        public IFuture AddData(ICollection<KeyValuePair<TK, TV>> entries)
        {
            ThrowIfDisposed();

            IgniteArgumentCheck.NotNull(entries, "entries");

            return Add0(entries, entries.Count);
        }

        /** <inheritDoc /> */
        public IFuture RemoveData(TK key)
        {
            ThrowIfDisposed();

            IgniteArgumentCheck.NotNull(key, "key");

            return Add0(new DataStreamerRemoveEntry<TK>(key), 1);
        }

        /** <inheritDoc /> */
        public void TryFlush()
        {
            ThrowIfDisposed();

            DataStreamerBatch<TK, TV> batch0 = _batch;

            if (batch0 != null)
                Flush0(batch0, false, PlcFlush);
        }

        /** <inheritDoc /> */
        public void Flush()
        {
            ThrowIfDisposed();

            DataStreamerBatch<TK, TV> batch0 = _batch;

            if (batch0 != null)
                Flush0(batch0, true, PlcFlush);
            else 
            {
                // Batch is null, i.e. data streamer is closing. Wait for close to complete.
                _closedEvt.Wait();
            }
        }

        /** <inheritDoc /> */
        public void Close(bool cancel)
        {
            _flusher.Stop();

            while (true)
            {
                DataStreamerBatch<TK, TV> batch0 = _batch;

                if (batch0 == null)
                {
                    // Wait for concurrent close to finish.
                    _closedEvt.Wait();

                    return;
                }

                if (Flush0(batch0, true, cancel ? PlcCancelClose : PlcClose))
                {
                    _closeFut.OnDone(null, null);

                    _rwLock.EnterWriteLock(); 
                    
                    try
                    {
                        base.Dispose(true);

                        if (_rcv != null)
                            Marshaller.Ignite.HandleRegistry.Release(_rcvHnd);

                        _closedEvt.Set();
                    }
                    finally
                    {
                        _rwLock.ExitWriteLock();
                    }

                    Marshaller.Ignite.HandleRegistry.Release(_hnd);

                    break;
                }
            }
        }

        /** <inheritDoc /> */
        public IDataStreamer<TK1, TV1> WithKeepPortable<TK1, TV1>()
        {
            if (_keepPortable)
            {
                var result = this as IDataStreamer<TK1, TV1>;

                if (result == null)
                    throw new InvalidOperationException(
                        "Can't change type of portable streamer. WithKeepPortable has been called on an instance of " +
                        "portable streamer with incompatible generic arguments.");

                return result;
            }

            return new DataStreamerImpl<TK1, TV1>(UU.ProcessorDataStreamer(Marshaller.Ignite.InteropProcessor,
                _cacheName, true), Marshaller, _cacheName, true);
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
                    if (_batch != null)
                        _batch.Send(this, PlcCancelClose);
                }
                catch (Exception)
                {
                    // Finalizers should never throw
                }

                Marshaller.Ignite.HandleRegistry.Release(_hnd, true);
                Marshaller.Ignite.HandleRegistry.Release(_rcvHnd, true);

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
            _rwLock.EnterWriteLock(); 
            
            try
            {
                ThrowIfDisposed();

                if (_topVer < topVer)
                {
                    _topVer = topVer;
                    _topSize = topSize;

                    _bufSndSize = topSize * UU.DataStreamerPerNodeBufferSizeGet(Target);
                }
            }
            finally
            {
                _rwLock.ExitWriteLock();
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
            int bufSndSize0 = _bufSndSize;

            while (true)
            {
                var batch0 = _batch;

                if (batch0 == null)
                    throw new InvalidOperationException("Data streamer is stopped.");

                int size = batch0.Add(val, cnt);

                if (size == -1)
                {
                    // Batch is blocked, perform CAS.
                    Interlocked.CompareExchange(ref _batch,
                        new DataStreamerBatch<TK, TV>(batch0), batch0);

                    continue;
                }
                if (size >= bufSndSize0)
                    // Batch is too big, schedule flush.
                    Flush0(batch0, false, PlcContinue);

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
        private bool Flush0(DataStreamerBatch<TK, TV> curBatch, bool wait, int plc)
        {
            // 1. Try setting new current batch to help further adders. 
            bool res = Interlocked.CompareExchange(ref _batch, 
                (plc == PlcContinue || plc == PlcFlush) ? 
                new DataStreamerBatch<TK, TV>(curBatch) : null, curBatch) == curBatch;

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
            _rwLock.EnterReadLock();

            try
            {
                ThrowIfDisposed();

                DoOutOp(OpUpdate, action);
            }
            finally
            {
                _rwLock.ExitReadLock();
            }
        }

        /// <summary>
        /// Flusher.
        /// </summary>
        private class Flusher<TK1, TV1>
        {
            /** State: running. */
            private const int StateRunning = 0;

            /** State: stopping. */
            private const int StateStopping = 1;

            /** State: stopped. */
            private const int StateStopped = 2;

            /** Data streamer. */
            private readonly WeakReference _ldrRef;

            /** Finish flag. */
            private int _state;

            /** Flush frequency. */
            private long _freq;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="ldrRef">Data streamer weak reference..</param>
            public Flusher(WeakReference ldrRef)
            {
                _ldrRef = ldrRef;

                lock (this)
                {
                    _state = StateRunning;
                }
            }

            /// <summary>
            /// Main flusher routine.
            /// </summary>
            private void Run()
            {
                bool force = false;
                long curFreq = 0;
                
                try
                {
                    while (true)
                    {
                        if (curFreq > 0 || force)
                        {
                            var ldr = _ldrRef.Target as DataStreamerImpl<TK1, TV1>;

                            if (ldr == null)
                                return;

                            ldr.TryFlush();

                            force = false;
                        }

                        lock (this)
                        {
                            // Stop immediately.
                            if (_state == StateStopping)
                                return;

                            if (curFreq == _freq)
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

                                        if (ticks > int.MaxValue)
                                            ticks = int.MaxValue;
                                    }
                                    catch (ArgumentOutOfRangeException)
                                    {
                                        // Handle possible overflow.
                                        ticks = int.MaxValue;
                                    }

                                    Monitor.Wait(this, TimeSpan.FromTicks(ticks));
                                }
                            }
                            else
                            {
                                if (curFreq != 0)
                                    force = true;

                                curFreq = _freq;
                            } 
                        }
                    }
                }
                finally
                {
                    // Let streamer know about stop.
                    lock (this)
                    {
                        _state = StateStopped;

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
                    return Interlocked.Read(ref _freq);
                }

                set
                {
                    lock (this)
                    {
                        if (_freq != value)
                        {
                            _freq = value;

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
                    if (_state == StateRunning)
                    {
                        _state = StateStopping;

                        Monitor.PulseAll(this);
                    }

                    while (_state != StateStopped)
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
