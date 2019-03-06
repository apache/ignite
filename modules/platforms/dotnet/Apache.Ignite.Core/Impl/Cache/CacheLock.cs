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
    using System.Diagnostics;
    using System.Threading;
    using Apache.Ignite.Core.Cache;

    /// <summary>
    /// Cache lock implementation.
    /// </summary>
    internal class CacheLock : ICacheLock
    {
        /** Unique lock ID.*/
        private readonly long _id;

        /** Cache lock. */
        private readonly ICacheLockInternal _lock;

        /** State (-1 for disposed, >=0 for number of currently executing methods). */
        private int _state;

        /** Current number of lock contenders. */
        private int _counter;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheLock"/> class.
        /// </summary>
        /// <param name="id">Lock id.</param>
        /// <param name="cacheLock">Cache lock.</param>
        public CacheLock(long id, ICacheLockInternal cacheLock)
        {
            Debug.Assert(cacheLock != null);

            _id = id;
            _lock = cacheLock;
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
                _lock.Enter(_id);

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
                return res = _lock.TryEnter(_id, timeout);
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

                _lock.Exit(_id);

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

                _lock.Close(_id);

                _state = -1;

                GC.SuppressFinalize(this);
            }
        }

        /// <summary>
        /// Finalizes an instance of the <see cref="CacheLock"/> class.
        /// </summary>
        ~CacheLock()
        {
            _lock.Close(_id);
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