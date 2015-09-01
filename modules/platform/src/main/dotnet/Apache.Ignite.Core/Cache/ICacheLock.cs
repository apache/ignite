/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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