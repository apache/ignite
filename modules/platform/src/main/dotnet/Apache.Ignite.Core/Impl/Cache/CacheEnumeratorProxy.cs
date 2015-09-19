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

namespace Apache.Ignite.Core.Impl.Cache
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Cache;

    /// <summary>
    /// Cache enumerator proxy. Required to support reset and early native iterator cleanup.
    /// </summary>
    internal class CacheEnumeratorProxy<TK, TV> : IEnumerator<ICacheEntry<TK, TV>>
    {
        /** Target cache. */
        private readonly CacheImpl<TK, TV> _cache;

        /** Local flag. */
        private readonly bool _loc;

        /** Peek modes. */
        private readonly int _peekModes;

        /** Target enumerator. */
        private CacheEnumerator<TK, TV> _target;

        /** Dispose flag. */
        private bool _disposed;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="cache">Target cache.</param>
        /// <param name="loc">Local flag.</param>
        /// <param name="peekModes">Peek modes.</param>
        public CacheEnumeratorProxy(CacheImpl<TK, TV> cache, bool loc, int peekModes)
        {
            _cache = cache;
            _loc = loc;
            _peekModes = peekModes;

            CreateTarget();
        }

        /** <inheritdoc /> */
        public bool MoveNext()
        {
            CheckDisposed();

            // No target => closed or finished.
            if (_target == null)
                return false;
            
            if (!_target.MoveNext())
            {
                // Failed to advance => end is reached.
                CloseTarget();

                return false;
            }

            return true;
        }

        /** <inheritdoc /> */
        public ICacheEntry<TK, TV> Current
        {
            get
            {
                CheckDisposed();

                if (_target == null)
                    throw new InvalidOperationException("Invalid enumerator state (did you call MoveNext()?)");

                return _target.Current;
            }
        }

        /** <inheritdoc /> */
        object IEnumerator.Current
        {
            get { return Current; }
        }

        /** <inheritdoc /> */
        public void Reset()
        {
            CheckDisposed();

            if (_target != null)
                CloseTarget();

            CreateTarget();
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            if (!_disposed)
            {
                if (_target != null)
                    CloseTarget();

                _disposed = true;
            }
        }

        /// <summary>
        /// Get target enumerator.
        /// </summary>
        /// <returns>Target enumerator.</returns>
        private void CreateTarget()
        {
            Debug.Assert(_target == null, "Previous target is not cleaned.");

            _target = _cache.CreateEnumerator(_loc, _peekModes);
        }

        /// <summary>
        /// Close the target.
        /// </summary>
        private void CloseTarget()
        {
            Debug.Assert(_target != null);

            _target.Dispose();

            _target = null;
        }

        /// <summary>
        /// Check whether object is disposed.
        /// </summary>
        private void CheckDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException("Cache enumerator has been disposed.");
        }
    }
}
