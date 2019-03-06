/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Impl.Cache.Query.Continuous
{
    using Apache.Ignite.Core.Cache.Event;
    using Apache.Ignite.Core.Impl.Binary.IO;
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
        bool Evaluate(IBinaryStream stream);

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

        /** Keep binary flag. */
        private readonly bool _keepBinary;

        /** Ignite hosting the filter. */
        private volatile Ignite _ignite;

        /** GC handle. */
        private long? _hnd;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="filter">Actual filter.</param>
        /// <param name="keepBinary">Keep binary flag.</param>
        public ContinuousQueryFilter(ICacheEntryEventFilter<TK, TV> filter, bool keepBinary)
        {
            _filter = filter;
            _keepBinary = keepBinary;
        }

        /** <inheritDoc /> */
        public bool Evaluate(IBinaryStream stream)
        {
            // ReSharper disable once InconsistentlySynchronizedField
            ICacheEntryEvent<TK, TV> evt = CQU.ReadEvent<TK, TV>(stream, _ignite.Marshaller, _keepBinary);

            return _filter.Evaluate(evt);
        }

        /** <inheritDoc /> */
        public void Inject(Ignite grid)
        {
            // ReSharper disable once InconsistentlySynchronizedField
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
