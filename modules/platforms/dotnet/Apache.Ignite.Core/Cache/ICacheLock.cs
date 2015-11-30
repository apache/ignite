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

namespace Apache.Ignite.Core.Cache
{
    using System;
    using System.Threading;

    /// <summary>
    /// Cache locking interface.
    /// <para/>
    /// All members are thread-safe and may be used concurrently from multiple threads.
    /// </summary>
    public interface ICacheLock : IDisposable
    {
        /// <summary>
        /// Acquires an exclusive lock.
        /// </summary>
        void Enter();

        /// <summary>
        /// Acquires an exclusive lock only if it is free at the time of invocation.
        /// </summary>
        /// <returns>True if the current thread acquires the lock; otherwise, false.</returns>
        bool TryEnter();

        /// <summary>
        /// Attempts, for the specified amount of time, to acquire an exclusive lock.
        /// </summary>
        /// <param name="timeout">
        /// A <see cref="TimeSpan" /> representing the amount of time to wait for the lock. 
        /// A value of –1 millisecond specifies an infinite wait.
        /// </param>
        /// <returns>True if the current thread acquires the lock; otherwise, false.</returns>
        bool TryEnter(TimeSpan timeout);

        /// <summary>
        /// Releases an exclusive lock on the specified object.
        /// <see cref="IDisposable.Dispose"/> does not call this method and will throw 
        /// <see cref="SynchronizationLockException"/> if this lock is acquired.
        /// </summary>
        void Exit();
    }
}