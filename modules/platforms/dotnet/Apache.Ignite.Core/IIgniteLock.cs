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

namespace Apache.Ignite.Core
{
    using System;
    using System.Threading;
    using Apache.Ignite.Core.Configuration;

    /// <summary>
    /// Distributed reentrant monitor (lock).
    /// <para />
    /// The functionality is similar to the standard <see cref="Monitor"/> class, but works across all cluster nodes.
    /// <para />
    /// This API corresponds to <c>IgniteLock</c> in Java.
    /// </summary>
    public interface IIgniteLock
    {
        /// <summary>
        /// Gets the lock configuration.
        /// </summary>
        LockConfiguration Configuration { get; }

        /// <summary>
        /// Acquires the distributed reentrant lock.
        /// </summary>
        void Enter();

        /// <summary>
        /// Acquires the lock only if it is free at the time of invocation.
        /// </summary>
        /// <returns>True if the lock was acquired; false otherwise.</returns>
        bool TryEnter();

        /// <summary>
        /// Acquires the lock if it is not held by another thread within the given waiting time.
        /// </summary>
        /// <param name="timeout">Time to wait for the lock.</param>
        /// <returns>True if the lock was acquired; false otherwise.</returns>
        bool TryEnter(TimeSpan timeout);

        /// <summary>
        /// Releases the lock.
        /// </summary>
        void Exit();

        /// <summary>
        /// Returns a value indicating whether any node that owned the lock failed before releasing the lock.
        /// </summary>
        bool IsBroken();

        /// <summary>
        /// Determines whether the current thread holds the lock.
        /// </summary>
        bool IsEntered();

        /// <summary>
        /// Removes the lock from the cluster.
        /// </summary>
        void Remove();

        /// <summary>
        /// Gets a value indicating whether the lock has been removed from the cluster.
        /// </summary>
        bool IsRemoved();
    }
}
