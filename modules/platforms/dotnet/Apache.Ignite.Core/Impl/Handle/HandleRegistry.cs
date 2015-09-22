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

namespace Apache.Ignite.Core.Impl.Handle
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;

    /// <summary>
    /// Resource registry.
    /// </summary>
    public class HandleRegistry
    {
        /** Default critical resources capacity. */
        internal const int DfltFastCap = 1024;

        /** Array for fast-path. */
        private readonly object[] _fast;

        /** Dictionery for slow-path. */
        private readonly ConcurrentDictionary<long, object> _slow;

        /** Capacity of fast array. */
        private readonly int _fastCap;

        /** Counter for fast-path. */
        private int _fastCtr;

        /** Counter for slow-path. */
        private long _slowCtr;

        /** Close flag. */
        private int _closed;

        /// <summary>
        /// Constructor.
        /// </summary>
        public HandleRegistry() : this(DfltFastCap)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="fastCap">Amount of critical resources this registry can allocate in "fast" mode.</param>
        public HandleRegistry(int fastCap)
        {
            _fastCap = fastCap;
            _fast = new object[fastCap];

            _slow = new ConcurrentDictionary<long, object>();
            _slowCtr = fastCap;
        }

        /// <summary>
        /// Allocate a handle for resource.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <returns>Pointer.</returns>
        public long Allocate(object target)
        {
            return Allocate0(target, false, false);
        }

        /// <summary>
        /// Allocate a handle in safe mode.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <returns>Pointer.</returns>
        public long AllocateSafe(object target)
        {
            return Allocate0(target, false, true);
        }

        /// <summary>
        /// Allocate a handle for critical resource.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <returns>Pointer.</returns>
        public long AllocateCritical(object target)
        {
            return Allocate0(target, true, false);
        }

        /// <summary>
        /// Allocate a handle for critical resource in safe mode.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <returns>Pointer.</returns>
        public long AllocateCriticalSafe(object target)
        {
            return Allocate0(target, true, true);
        }

        /// <summary>
        /// Internal allocation routine.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="critical">Critical flag.</param>
        /// <param name="safe">Safe flag.</param>
        /// <returns>Pointer.</returns>
        private long Allocate0(object target, bool critical, bool safe)
        {
            if (Closed)
                throw ClosedException();

            // Try allocating on critical path.
            if (critical)
            {
                if (_fastCtr < _fastCap) // Non-volatile read could yield in old value, but increment resolves this.
                {
                    int fastIdx = Interlocked.Increment(ref _fastCtr);

                    if (fastIdx < _fastCap)
                    {
                        Thread.VolatileWrite(ref _fast[fastIdx], target);

                        if (safe && Closed)
                        {
                            Thread.VolatileWrite(ref _fast[fastIdx], null);

                            Release0(target, true);

                            throw ClosedException();
                        }

                        return fastIdx;
                    }
                }
            }
            
            // Critical allocation failed, fallback to slow mode.
            long slowIdx = Interlocked.Increment(ref _slowCtr);

            _slow[slowIdx] = target;

            if (safe && Closed)
            {
                _slow[slowIdx] = null;

                Release0(target, true);

                throw ClosedException();
            }

            return slowIdx;
        }


        /// <summary>
        /// Release handle.
        /// </summary>
        /// <param name="id">Identifier.</param>
        /// <param name="quiet">Whether release must be quiet or not.</param>
        public void Release(long id, bool quiet = false)
        {
            if (id < _fastCap)
            {
                object target = Thread.VolatileRead(ref _fast[id]);

                if (target != null)
                {
                    Thread.VolatileWrite(ref _fast[id], null);

                    Release0(target, quiet);
                }
            }
            else
            {
                object target;

                if (_slow.TryRemove(id, out target))
                    Release0(target, quiet);
            }
        }
        
        /// <summary>
        /// Internal release routine.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="quiet">Whether release must be quiet or not.</param>
        private static void Release0(object target, bool quiet)
        {
            IHandle target0 = target as IHandle;

            if (target0 != null)
            {
                if (quiet)
                {
                    try
                    {
                        target0.Release();
                    }
                    catch (Exception)
                    {
                        // No-op.
                    }
                }
                else
                    target0.Release();
            }
        }

        /// <summary>
        /// Gets handle target.
        /// </summary>
        /// <param name="id">Identifier.</param>
        /// <returns>Target.</returns>
        public T Get<T>(long id)
        {
            return Get<T>(id, false);
        }

        /// <summary>
        /// Gets handle target.
        /// </summary>
        /// <param name="id">Identifier.</param>
        /// <param name="throwOnAbsent">Whether to throw an exception if resource is not found.</param>
        /// <returns>Target.</returns>
        public T Get<T>(long id, bool throwOnAbsent)
        {
            object target;

            if (id < _fastCap)
            {
                target = Thread.VolatileRead(ref _fast[id]);

                if (target != null)
                    return (T)target;
            }
            else
            {
                if (_slow.TryGetValue(id, out target))
                    return (T) target;
            }

            if (throwOnAbsent)
                throw new InvalidOperationException("Resource handle has been released (is Ignite stopping?).");

            return default(T);
        }

        /// <summary>
        /// Close the registry. All resources allocated at the moment of close are
        /// guaranteed to be released.
        /// </summary>
        public void Close()
        {
            if (Interlocked.CompareExchange(ref _closed, 1, 0) == 0)
            {
                // Cleanup on fast-path.
                for (int i = 0; i < _fastCap; i++)
                {
                    object target = Thread.VolatileRead(ref _fast[i]);

                    if (target != null)
                    {
                        Thread.VolatileWrite(ref _fast[i], null);

                        Release0(target, true);
                    }
                }

                // Cleanup on slow-path.
                foreach (var item in _slow)
                {
                    object target = item.Value;

                    if (target != null)
                        Release0(target, true);
                }

                _slow.Clear();
            }
        }

        /// <summary>
        /// Closed flag.
        /// </summary>
        public bool Closed
        {
            get { return Thread.VolatileRead(ref _closed) == 1; }
        }

        /// <summary>
        /// Gets the current handle count.
        /// </summary>
        public int Count
        {
            get
            {
                Thread.MemoryBarrier();

                return _fast.Count(x => x != null) + _slow.Count;
            }
        }

        /// <summary>
        /// Gets a snapshot of currently referenced objects list.
        /// </summary>
        public List<KeyValuePair<long, object>> GetItems()
        {
            Thread.MemoryBarrier();

            return
                _fast.Select((x, i) => new KeyValuePair<long, object>(i, x))
                    .Where(x => x.Value != null)
                    .Concat(_slow)
                    .ToList();
        }

        /// <summary>
        /// Create new exception for closed state.
        /// </summary>
        /// <returns>Exception.</returns>
        private static Exception ClosedException()
        {
            return new InvalidOperationException("Cannot allocate a resource handle because Ignite is stopping.");
        }
    }
}
 