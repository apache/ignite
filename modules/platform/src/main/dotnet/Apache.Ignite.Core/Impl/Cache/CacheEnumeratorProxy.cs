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
    internal class CacheEnumeratorProxy<K, V> : IEnumerator<ICacheEntry<K, V>>
    {
        /** Target cache. */
        private readonly CacheImpl<K, V> cache;

        /** Local flag. */
        private readonly bool loc;

        /** Peek modes. */
        private readonly int peekModes;

        /** Target enumerator. */
        private CacheEnumerator<K, V> target;

        /** Dispose flag. */
        private bool disposed;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="cache">Target cache.</param>
        /// <param name="loc">Local flag.</param>
        /// <param name="peekModes">Peek modes.</param>
        public CacheEnumeratorProxy(CacheImpl<K, V> cache, bool loc, int peekModes)
        {
            this.cache = cache;
            this.loc = loc;
            this.peekModes = peekModes;

            CreateTarget();
        }

        /** <inheritdoc /> */
        public bool MoveNext()
        {
            CheckDisposed();

            // No target => closed or finished.
            if (target == null)
                return false;
            
            if (!target.MoveNext())
            {
                // Failed to advance => end is reached.
                CloseTarget();

                return false;
            }

            return true;
        }

        /** <inheritdoc /> */
        public ICacheEntry<K, V> Current
        {
            get
            {
                CheckDisposed();

                if (target == null)
                    throw new InvalidOperationException("Invalid enumerator state (did you call MoveNext()?)");

                return target.Current;
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

            if (target != null)
                CloseTarget();

            CreateTarget();
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            if (!disposed)
            {
                if (target != null)
                    CloseTarget();

                disposed = true;
            }
        }

        /// <summary>
        /// Get target enumerator.
        /// </summary>
        /// <returns>Target enumerator.</returns>
        private void CreateTarget()
        {
            Debug.Assert(target == null, "Previous target is not cleaned.");

            target = cache.CreateEnumerator(loc, peekModes);
        }

        /// <summary>
        /// Close the target.
        /// </summary>
        private void CloseTarget()
        {
            Debug.Assert(target != null);

            target.Dispose();

            target = null;
        }

        /// <summary>
        /// Check whether object is disposed.
        /// </summary>
        private void CheckDisposed()
        {
            if (disposed)
                throw new ObjectDisposedException("Cache enumerator has been disposed.");
        }
    }
}
