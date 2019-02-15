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

namespace Apache.Ignite.Core.Impl.Cache
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
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
        [SuppressMessage("Microsoft.Usage", "CA1816:CallGCSuppressFinalizeCorrectly",
            Justification = "There is no finalizer.")]
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
