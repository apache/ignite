/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl.Cache.Query.Continuous
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Event;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Cache.Query.Continuous;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Impl.Resource;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;
    using CQU = ContinuousQueryUtils;

    /// <summary>
    /// Continuous query handle interface.
    /// </summary>
    internal interface IContinuousQueryHandleImpl : IDisposable
    {
        /// <summary>
        /// Process callback.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <returns>Result.</returns>
        void Apply(IPortableStream stream);
    }

    /// <summary>
    /// Continuous query handle.
    /// </summary>
    internal class ContinuousQueryHandleImpl<K, V> : IContinuousQueryHandleImpl, IContinuousQueryFilter, 
        IContinuousQueryHandle<ICacheEntry<K, V>>
    {
        /** Marshaller. */
        private readonly PortableMarshaller marsh;

        /** Keep portable flag. */
        private readonly bool keepPortable;

        /** Real listener. */
        private readonly ICacheEntryEventListener<K, V> lsnr;

        /** Real filter. */
        private readonly ICacheEntryEventFilter<K, V> filter;

        /** GC handle. */
        private long hnd;

        /** Native query. */
        private volatile IUnmanagedTarget nativeQry;
        
        /** Initial query cursor. */
        private volatile IQueryCursor<ICacheEntry<K, V>> initialQueryCursor;

        /** Disposed flag. */
        private bool disposed;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="qry">Query.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="keepPortable">Keep portable flag.</param>
        public ContinuousQueryHandleImpl(ContinuousQuery<K, V> qry, PortableMarshaller marsh, bool keepPortable)
        {
            this.marsh = marsh;
            this.keepPortable = keepPortable;

            lsnr = qry.Listener;
            filter = qry.Filter;
        }

        /// <summary>
        /// Start execution.
        /// </summary>
        /// <param name="grid">Grid instance.</param>
        /// <param name="writer">Writer.</param>
        /// <param name="cb">Callback invoked when all necessary data is written to stream.</param>
        /// <param name="qry">Query.</param>
        public void Start(GridImpl grid, PortableWriterImpl writer, Func<IUnmanagedTarget> cb, 
            ContinuousQuery<K, V> qry)
        {
            // 1. Inject resources.
            ResourceProcessor.Inject(lsnr, grid);
            ResourceProcessor.Inject(filter, grid);

            // 2. Allocate handle.
            hnd = grid.HandleRegistry.Allocate(this);

            // 3. Write data to stream.
            writer.WriteLong(hnd);
            writer.WriteBoolean(qry.Local);
            writer.WriteBoolean(filter != null);

            ContinuousQueryFilterHolder filterHolder = filter == null || qry.Local ? null : 
                new ContinuousQueryFilterHolder(typeof (K), typeof (V), filter, keepPortable);

            writer.WriteObject(filterHolder);

            writer.WriteInt(qry.BufferSize);
            writer.WriteLong((long)qry.TimeInterval.TotalMilliseconds);
            writer.WriteBoolean(qry.AutoUnsubscribe);

            // 4. Call Java.
            nativeQry = cb();

            // 5. Initial query.
            var nativeInitialQryCur = UU.ContinuousQueryGetInitialQueryCursor(nativeQry);
            initialQueryCursor = nativeInitialQryCur == null
                ? null
                : new QueryCursor<K, V>(nativeInitialQryCur, marsh, keepPortable);
        }

        /** <inheritdoc /> */
        public void Apply(IPortableStream stream)
        {
            ICacheEntryEvent<K, V>[] evts = CQU.ReadEvents<K, V>(stream, marsh, keepPortable);

            lsnr.OnEvent(evts); 
        }

        /** <inheritdoc /> */
        public bool Evaluate(IPortableStream stream)
        {
            Debug.Assert(filter != null, "Evaluate should not be called if filter is not set.");

            ICacheEntryEvent<K, V> evt = CQU.ReadEvent<K, V>(stream, marsh, keepPortable);

            return filter.Evaluate(evt);
        }

        /** <inheritdoc /> */
        public void Inject(GridImpl grid)
        {
            throw new NotSupportedException("Should not be called.");
        }

        /** <inheritdoc /> */
        public long Allocate()
        {
            throw new NotSupportedException("Should not be called.");
        }

        /** <inheritdoc /> */
        public void Release()
        {
            marsh.Grid.HandleRegistry.Release(hnd);
        }

        /** <inheritdoc /> */
        public IQueryCursor<ICacheEntry<K, V>> InitialQueryCursor
        {
            get { return GetInitialQueryCursor(); }
        }

        /** <inheritdoc /> */
        public IQueryCursor<ICacheEntry<K, V>> GetInitialQueryCursor()
        {
            lock (this)
            {
                if (disposed)
                    throw new ObjectDisposedException("Continuous query handle has been disposed.");

                var cur = initialQueryCursor;

                if (cur == null)
                    throw new InvalidOperationException("GetInitialQueryCursor() can be called only once.");

                initialQueryCursor = null;

                return cur;
            }
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            lock (this)
            {
                if (disposed)
                    return;

                Debug.Assert(nativeQry != null);

                try
                {
                    UU.ContinuousQueryClose(nativeQry);
                }
                finally
                {
                    nativeQry.Dispose();

                    disposed = true;
                }
            }
        }
    }
}
