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
    using System.Diagnostics;
    using System.Threading;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Cache lock implementation.
    /// </summary>
    internal class CacheLock : ICacheLock
    {
        /** Unique lock ID.*/
        private readonly long _id;

        /** Cache. */
        private readonly IUnmanagedTarget _cache;

        /** State (-1 for disposed, >=0 for number of currently executing methods). */
        private int _state;

        /** Current number of lock contenders. */
        private int _counter;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheLock"/> class.
        /// </summary>
        /// <param name="id">Lock id.</param>
        /// <param name="cache">Cache.</param>
        public CacheLock(long id, IUnmanagedTarget cache)
        {
            Debug.Assert(cache != null);

            _id = id;
            _cache = cache;
        }

        /** <inheritDoc /> */
        public void Enter()
        {
            lock (this)
            {
                ThrowIfDisposed();

                _state++;
            }

            var res = false;

            try
            {
                UU.CacheEnterLock(_cache, _id);

                res = true;
            }
            finally 
            {
                lock (this)
                {
                    if (res)
                        _counter++;

                    _state--;
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

                _state++;
            }
            
            var res = false;

            try
            {
                return res = UU.CacheTryEnterLock(_cache, _id, (long)timeout.TotalMilliseconds);
            }
            finally 
            {
                lock (this)
                {
                    if (res)
                        _counter++;

                    _state--;
                }
            }
        }

        /** <inheritDoc /> */
        public void Exit()
        {
            lock (this)
            {
                ThrowIfDisposed();

                UU.CacheExitLock(_cache, _id);

                _counter--;
            }
        }

        /** <inheritDoc /> */
        public void Dispose()
        {
            lock (this)
            {
                ThrowIfDisposed();

                if (_state > 0 || _counter > 0)
                    throw new SynchronizationLockException(
                        "The lock is being disposed while still being used. " +
                        "It either is being held by a thread and/or has active waiters waiting to acquire the lock.");

                UU.CacheCloseLock(_cache, _id);

                _state = -1;

                GC.SuppressFinalize(this);
            }
        }

        /// <summary>
        /// Finalizes an instance of the <see cref="CacheLock"/> class.
        /// </summary>
        ~CacheLock()
        {
            UU.CacheCloseLock(_cache, _id);
        }

        /// <summary>
        /// Throws <see cref="ObjectDisposedException"/> if this instance has been disposed.
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (_state < 0)
                throw new ObjectDisposedException("CacheLock", "CacheLock has been disposed.");
        }
    }
}