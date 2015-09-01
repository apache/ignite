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
    using Apache.Ignite.Core.Cache.Event;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Impl.Resource;
    using CQU = ContinuousQueryUtils;

    /// <summary>
    /// Continuous query filter interface. Required to hide generic nature of underliyng real filter.
    /// </summary>
    internal interface IContinuousQueryFilter
    {
        /// <summary>
        /// Evaluate filter.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <returns>Result.</returns>
        bool Evaluate(IPortableStream stream);

        /// <summary>
        /// Inject grid.
        /// </summary>
        /// <param name="grid"></param>
        void Inject(GridImpl grid);

        /// <summary>
        /// Allocate handle for the filter.
        /// </summary>
        /// <returns></returns>
        long Allocate();

        /// <summary>
        /// Release filter.
        /// </summary>
        void Release();
    }

    /// <summary>
    /// Continuous query filter generic implementation.
    /// </summary>
    internal class ContinuousQueryFilter<K, V> : IContinuousQueryFilter        
    {
        /** Actual filter. */
        private readonly ICacheEntryEventFilter<K, V> filter;

        /** Keep portable flag. */
        private readonly bool keepPortable;

        /** Grid hosting the filter. */
        private volatile GridImpl grid;

        /** GC handle. */
        private long? hnd;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="filter">Actual filter.</param>
        /// <param name="keepPortable">Keep portable flag.</param>
        public ContinuousQueryFilter(ICacheEntryEventFilter<K, V> filter, bool keepPortable)
        {
            this.filter = filter;
            this.keepPortable = keepPortable;
        }

        /** <inheritDoc /> */
        public bool Evaluate(IPortableStream stream)
        {
            ICacheEntryEvent<K, V> evt = CQU.ReadEvent<K, V>(stream, grid.Marshaller, keepPortable);

            return filter.Evaluate(evt);
        }

        /** <inheritDoc /> */
        public void Inject(GridImpl grid)
        {
            this.grid = grid;

            ResourceProcessor.Inject(filter, grid);
        }

        /** <inheritDoc /> */
        public long Allocate()
        {
            lock (this)
            {
                if (!hnd.HasValue)
                    hnd = grid.HandleRegistry.Allocate(this);

                return hnd.Value;
            }
        }

        /** <inheritDoc /> */
        public void Release()
        {
            lock (this)
            {
                if (hnd.HasValue)
                {
                    grid.HandleRegistry.Release(hnd.Value);

                    hnd = null;
                }
            }
        }
    }
}
