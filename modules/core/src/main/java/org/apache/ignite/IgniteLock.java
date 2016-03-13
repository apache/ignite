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

package org.apache.ignite;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * This interface provides a rich API for working with distributed reentrant locks.
 * <p>
 * <h1 class="header">Functionality</h1>
 * Distributed reentrant lock provides functionality similar to {@code java.util.concurrent.ReentrantLock}.
 * <h1 class="header">Creating Distributed ReentrantLock</h1>
 * Instance of cache reentrant lock can be created by calling the following method:
 * {@link Ignite#reentrantLock(String, boolean, boolean)}.
 */
public interface IgniteLock extends Lock, Closeable {

    /**
     * Name of atomic reentrant lock.
     *
     * @return Name of atomic reentrant lock.
     */
    public String name();

    /** {@inheritDoc} */
    @Override public void lock();

    /**
     * Acquires the lock unless the current thread is
     * {@linkplain Thread#interrupt interrupted}.
     *
     * <p>Acquires the lock if it is not held by another thread and returns
     * immediately, setting the lock hold count to one.
     *
     * <p>If the current thread already holds this lock then the hold count
     * is incremented by one and the method returns immediately.
     *
     * <p>If the lock is held by another thread then the
     * current thread becomes disabled for thread scheduling
     * purposes and lies dormant until one of four things happens:
     *
     * <ul>
     *
     * <li>The lock is acquired by the current thread; or
     *
     * <li>Some other thread {@linkplain Thread#interrupt interrupts} the
     * current thread.
     *
     * <li>Lock is broken (any node failed while owning this lock), and lock is created in
     * non-failoverSafe mode.
     *
     * <li>Local node is stopped.
     *
     * </ul>
     *
     * <p>If the lock is acquired by the current thread then the lock hold
     * count is set to one.
     *
     * <p>If the current thread:
     *
     * <ul>
     *
     * <li>has its interrupted status set on entry to this method; or
     *
     * <li>is {@linkplain Thread#interrupt interrupted} while acquiring
     * the lock; or
     *
     * <li>lock is broken before or during the attempt to acquire this lock; or
     *
     * <li>local node is stopped,
     *
     * </ul>
     *
     * then {@link IgniteInterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * <p>In this implementation, as this method is an explicit
     * interruption point, preference is given to responding to the
     * interrupt over normal or reentrant acquisition of the lock.
     *
     * @throws IgniteInterruptedException if the current thread is interrupted
     */
    @Override public void lockInterruptibly() throws IgniteInterruptedException;

    /** {@inheritDoc} */
    @Override public boolean tryLock();

    /**
     * Acquires the lock if it is not held by another thread within the given
     * waiting time and the current thread has not been
     * {@linkplain Thread#interrupt interrupted}.
     *
     * <p>Acquires the lock if it is not held by another thread and returns
     * immediately with the value {@code true}, setting the lock hold count
     * to one.
     *
     * <p>If the current thread
     * already holds this lock then the hold count is incremented by one and
     * the method returns {@code true}.
     *
     * <p>If the lock is held by another thread then the
     * current thread becomes disabled for thread scheduling
     * purposes and lies dormant until one of five things happens:
     *
     * <ul>
     *
     * <li>The lock is acquired by the current thread; or
     *
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread; or
     *
     * <li>Lock is broken (any node failed while owning this lock), and lock is created in
     * non-failoverSafe mode.
     *
     * <li>Local node is stopped.
     *
     * <li>The specified waiting time elapses
     *
     * </ul>
     *
     * <p>If the lock is acquired then the value {@code true} is returned and
     * the lock hold count is set to one.
     *
     * <p>If the current thread:
     *
     * <ul>
     *
     * <li>has its interrupted status set on entry to this method; or
     *
     * <li>is {@linkplain Thread#interrupt interrupted} while
     * acquiring the lock; or
     *
     * <li>lock is broken before or during the attempt to acquire this lock; or
     *
     * <li>local node is stopped,
     *
     * </ul>
     * then {@link IgniteInterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * <p>If the specified waiting time elapses then the value {@code false}
     * is returned.  If the time is less than or equal to zero, the method
     * will not wait at all.
     *
     * <p>In this implementation, as this method is an explicit
     * interruption point, preference is given to responding to the
     * interrupt over normal or reentrant acquisition of the lock, and
     * over reporting the elapse of the waiting time.
     *
     * @param timeout the time to wait for the lock
     * @param unit the time unit of the timeout argument
     * @return {@code true} if the lock was free and was acquired by the
     *         current thread, or the lock was already held by the current
     *         thread; and {@code false} if the waiting time elapsed before
     *         the lock could be acquired
     * @throws IgniteInterruptedException if the current thread is interrupted
     * @throws NullPointerException if the time unit is null
     */
    @Override public boolean tryLock(long timeout, TimeUnit unit) throws IgniteInterruptedException;

    /** {@inheritDoc} */
    @Override public void unlock();

    /**
     * Returns a {@link Condition} instance for use with this
     * {@link IgniteLock} instance.
     *
     * <ul>
     *
     * <li>If this lock is not held when any of the {@link Condition}
     * {@linkplain Condition#await() waiting} or {@linkplain
     * Condition#signal signalling} methods are called, then an {@link
     * IllegalMonitorStateException} is thrown.
     *
     * <li>When the condition {@linkplain Condition#await() waiting}
     * methods are called the lock is released and, before they
     * return, the lock is reacquired and the lock hold count restored
     * to what it was when the method was called.
     *
     * <li>If a thread is {@linkplain Thread#interrupt interrupted}
     * while waiting then the wait will terminate, an {@link
     * IgniteInterruptedException} will be thrown, and the thread's
     * interrupted status will be cleared.
     *
     * <li> Waiting threads are signalled in FIFO order.
     *
     * </ul>
     *
     * @param name Name of the distributed condition object
     *
     * @return the Condition object
     */
    public IgniteCondition getOrCreateCondition(String name);

    /**
     * This method is not supported in IgniteLock,
     * Any invocation of this method will result in {@linkplain UnsupportedOperationException}.
     * Correct way to obtain Condition object is through method {@linkplain IgniteLock#getOrCreateCondition(String)}
     *
     */
    @Override public Condition newCondition();

    /**
     * Queries the number of holds on this lock by the current thread.
     *
     * @return the number of holds on this lock by the current thread,
     *         or zero if this lock is not held by the current thread
     */
    public int getHoldCount();

    /**
     * Queries if this lock is held by the current thread.
     *
     * @return {@code true} if current thread holds this lock and
     *         {@code false} otherwise
     */
    public boolean isHeldByCurrentThread();

    /**
     * Queries if this lock is held by any thread on any node. This method is
     * designed for use in monitoring of the system state,
     * not for synchronization control.
     *
     * @return {@code true} if any thread on this or any other node holds this lock and
     *         {@code false} otherwise
     */
    public boolean isLocked();

    /**
     * Queries whether any threads on this node are waiting to acquire this lock. Note that
     * because cancellations may occur at any time, a {@code true}
     * return does not guarantee that any other thread will ever
     * acquire this lock.  This method is designed primarily for use in
     * monitoring of the system state.
     *
     * @return {@code true} if there may be other threads on this node waiting to
     *         acquire the lock
     */
    public boolean hasQueuedThreads();

    /**
     * Queries whether the given thread is waiting to acquire this
     * lock. Note that because cancellations may occur at any time, a
     * {@code true} return does not guarantee that this thread
     * will ever acquire this lock.  This method is designed primarily for use
     * in monitoring of the system state.
     *
     * @param thread the thread
     * @return {@code true} if the given thread is queued waiting for this lock
     * @throws NullPointerException if the thread is null
     */
    public boolean hasQueuedThread(Thread thread);

    /**
     * Queries whether any threads on this node are waiting on the given condition
     * associated with this lock. Note that because timeouts and
     * interrupts may occur at any time, a {@code true} return does
     * not guarantee that a future {@code signal} will awaken any
     * threads.  This method is designed primarily for use in
     * monitoring of the system state.
     *
     * @param condition the condition
     * @return {@code true} if there are any waiting threads on this node
     * @throws IllegalMonitorStateException if this lock is not held
     * @throws IllegalArgumentException if the given condition is
     *         not associated with this lock
     * @throws NullPointerException if the condition is null
     */
    public boolean hasWaiters(IgniteCondition condition);

    /**
     * Returns an estimate of the number of threads on this node that are waiting on the
     * given condition associated with this lock. Note that because
     * timeouts and interrupts may occur at any time, the estimate
     * serves only as an upper bound on the actual number of waiters.
     * This method is designed for use in monitoring of the system
     * state, not for synchronization control.
     *
     * @param condition the condition
     * @return the estimated number of waiting threads on this node
     * @throws IgniteIllegalStateException if this lock is not held
     * @throws IllegalArgumentException if the given condition is
     *         not associated with this lock
     * @throws NullPointerException if the condition is null
     */
    public int getWaitQueueLength(IgniteCondition condition);

    /**
     * Returns {@code true} if this lock is safe to use after node failure.
     * If not, IgniteInterruptedException is thrown on every other node after node failure.
     *
     * @return {@code true} if this reentrant lock has failoverSafe set true
     */
    public boolean isFailoverSafe();

    /**
     * Returns true if any node that owned the locked failed before releasing the lock.
     *
     * @return true if any node failed while owning the lock.
     */
    public boolean isBroken();

    /** {@inheritDoc} */
    @Override public String toString();

    /**
     * Gets status of reentrant lock.
     *
     * @return {@code true} if reentrant lock was removed from cache, {@code false} in other case.
     */
    public boolean removed();

    /**
     * Removes reentrant lock.
     *
     * @throws IgniteException If operation failed.
     */
    @Override public void close();
}
