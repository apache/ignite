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
        void Inject(Ignite grid);

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
    internal class ContinuousQueryFilter<TK, TV> : IContinuousQueryFilter        
    {
        /** Actual filter. */
        private readonly ICacheEntryEventFilter<TK, TV> _filter;

        /** Keep portable flag. */
        private readonly bool _keepPortable;

        /** Ignite hosting the filter. */
        private volatile Ignite _ignite;

        /** GC handle. */
        private long? _hnd;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="filter">Actual filter.</param>
        /// <param name="keepPortable">Keep portable flag.</param>
        public ContinuousQueryFilter(ICacheEntryEventFilter<TK, TV> filter, bool keepPortable)
        {
            _filter = filter;
            _keepPortable = keepPortable;
        }

        /** <inheritDoc /> */
        public bool Evaluate(IPortableStream stream)
        {
            ICacheEntryEvent<TK, TV> evt = CQU.ReadEvent<TK, TV>(stream, _ignite.Marshaller, _keepPortable);

            return _filter.Evaluate(evt);
        }

        /** <inheritDoc /> */
        public void Inject(Ignite grid)
        {
            _ignite = grid;

            ResourceProcessor.Inject(_filter, grid);
        }

        /** <inheritDoc /> */
        public long Allocate()
        {
            lock (this)
            {
                if (!_hnd.HasValue)
                    _hnd = _ignite.HandleRegistry.Allocate(this);

                return _hnd.Value;
            }
        }

        /** <inheritDoc /> */
        public void Release()
        {
            lock (this)
            {
                if (_hnd.HasValue)
                {
                    _ignite.HandleRegistry.Release(_hnd.Value);

                    _hnd = null;
                }
            }
        }
    }
}
