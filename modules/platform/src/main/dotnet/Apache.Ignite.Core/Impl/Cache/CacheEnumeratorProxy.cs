/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Cache
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;

    using GridGain.Cache;

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
