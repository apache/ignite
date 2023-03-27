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

namespace Apache.Ignite.Core.Impl.Cache.Query
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Impl.Cache.Platform;

    /// <summary>
    /// Query cursor over platform cache.
    /// </summary>
    internal sealed class PlatformCacheQueryCursor<TK, TV> : IQueryCursor<ICacheEntry<TK, TV>>
    {
        /** */
        private readonly IPlatformCache _platformCache;
        
        /** */
        private readonly Action _dispose;

        /** */
        private readonly ICacheEntryFilter<TK, TV> _filter;
        
        /** */
        private readonly int? _partition;

        /** */
        private bool _disposed;

        /** */
        private bool _iterCalled;

        /// <summary>
        /// Initializes a new instance of <see cref="PlatformCacheQueryCursor{TK,TV}"/>.
        /// </summary>
        /// <param name="platformCache">Platform cache</param>
        /// <param name="filter">Filter.</param>
        /// <param name="partition">Partition.</param>
        /// <param name="dispose">Dispose action.</param>
        internal PlatformCacheQueryCursor(IPlatformCache platformCache, ICacheEntryFilter<TK, TV> filter = null, 
            int? partition = null, Action dispose = null)
        {
            Debug.Assert(platformCache != null);
            
            _platformCache = platformCache;
            _filter = filter;
            _partition = partition;
            _dispose = dispose;

            if (_dispose == null)
            {
                GC.SuppressFinalize(this);
            }
        }

        /** <inheritdoc /> */
        public IList<ICacheEntry<TK, TV>> GetAll()
        {
            return GetEnumerable().ToList();
        }

        /** <inheritdoc /> */
        public IEnumerator<ICacheEntry<TK, TV>> GetEnumerator()
        {
            return GetEnumerable().GetEnumerator();
        }

        /** <inheritdoc /> */
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            ReleaseUnmanagedResources();
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Gets the enumerable.
        /// </summary>
        private IEnumerable<ICacheEntry<TK, TV>> GetEnumerable()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(GetType().Name, 
                    "Object has been disposed. Query cursor can not be enumerated multiple times.");
            }
            
            if (_iterCalled)
            {
                throw new InvalidOperationException("Query cursor can not be enumerated multiple times.");
            }
            
            _iterCalled = true;

            return GetEnumerableInternal();
        }

        /// <summary>
        /// Gets the enumerable.
        /// </summary>
        private IEnumerable<ICacheEntry<TK, TV>> GetEnumerableInternal()
        {
            try
            {
                foreach (var entry in _platformCache.GetEntries<TK, TV>(_partition))
                {
                    if (_filter == null || _filter.Invoke(entry))
                    {
                        yield return entry;
                    }
                }
            }
            finally
            {
                Dispose();
            }
        }

        /// <summary>
        /// Releases unmanaged resources.
        /// </summary>
        private void ReleaseUnmanagedResources()
        {
            if (!_disposed)
            {
                _disposed = true;

                if (_dispose != null)
                {
                    _dispose();
                }
            }
        }

        /// <summary>
        /// Finalizer.
        /// </summary>
        ~PlatformCacheQueryCursor()
        {
            ReleaseUnmanagedResources();
        }
    }
}