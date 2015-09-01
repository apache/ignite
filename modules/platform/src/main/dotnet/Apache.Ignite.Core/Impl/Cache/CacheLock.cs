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
    using System.Diagnostics;
    using System.Threading;

    using GridGain.Cache;
    using GridGain.Impl.Unmanaged;
    using UU = GridGain.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Cache lock implementation.
    /// </summary>
    internal class CacheLock : ICacheLock
    {
        /** Unique lock ID.*/
        private readonly long id;

        /** Cache. */
        private readonly IUnmanagedTarget cache;

        /** State (-1 for disposed, >=0 for number of currently executing methods). */
        private int state;

        /** Current number of lock contenders. */
        private int counter;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheLock"/> class.
        /// </summary>
        /// <param name="id">Lock id.</param>
        /// <param name="cache">Cache.</param>
        public CacheLock(long id, IUnmanagedTarget cache)
        {
            Debug.Assert(cache != null);

            this.id = id;
            this.cache = cache;
        }

        /** <inheritDoc /> */
        public void Enter()
        {
            lock (this)
            {
                ThrowIfDisposed();

                state++;
            }

            var res = false;

            try
            {
                UU.CacheEnterLock(cache, id);

                res = true;
            }
            finally 
            {
                lock (this)
                {
                    if (res)
                        counter++;

                    state--;
                }
            }
        }

        /** <inheritDoc /> */
        public bool TryEnter()
        {
            return TryEnter(TimeSpan.FromMilliseconds(-1));
        }

        /** <inheritDoc /> */
        public bool TryEnter(TimeSpan timeout)
        {
            lock (this)
            {
                ThrowIfDisposed();

                state++;
            }
            
            var res = false;

            try
            {
                return res = UU.CacheTryEnterLock(cache, id, (long)timeout.TotalMilliseconds);
            }
            finally 
            {
                lock (this)
                {
                    if (res)
                        counter++;

                    state--;
                }
            }
        }

        /** <inheritDoc /> */
        public void Exit()
        {
            lock (this)
            {
                ThrowIfDisposed();

                UU.CacheExitLock(cache, id);

                counter--;
            }
        }

        /** <inheritDoc /> */
        public void Dispose()
        {
            lock (this)
            {
                ThrowIfDisposed();

                if (state > 0 || counter > 0)
                    throw new SynchronizationLockException(
                        "The lock is being disposed while still being used. " +
                        "It either is being held by a thread and/or has active waiters waiting to acquire the lock.");

                UU.CacheCloseLock(cache, id);

                state = -1;

                GC.SuppressFinalize(this);
            }
        }

        /// <summary>
        /// Finalizes an instance of the <see cref="CacheLock"/> class.
        /// </summary>
        ~CacheLock()
        {
            UU.CacheCloseLock(cache, id);
        }

        /// <summary>
        /// Throws <see cref="ObjectDisposedException"/> if this instance has been disposed.
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (state < 0)
                throw new ObjectDisposedException("CacheLock", "CacheLock has been disposed.");
        }
    }
}