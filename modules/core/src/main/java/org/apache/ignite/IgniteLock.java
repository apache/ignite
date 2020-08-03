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
 * {@link Ignite#reentrantLock(String, boolean, boolean, boolean)}.
 * <h1 class="header">Protection from failover</h1>
 * Ignite lock can automatically recover from node failure.
 * <ul>
 * <li>If failoverSafe flag is set to true upon creation,
 * in case a node owning the lock fails, lock will be automatically released and become available for threads on other
 * nodes to acquire. No exception will be thrown.
 * <li>If failoverSafe flag is set to false upon creation,
 * in case a node owning the lock fails, {@code IgniteException} will be thrown on every other node attempting to
 * perform any operation on this lock. No automatic recovery will be attempted,
 * and lock will be marked as broken (i.e. unusable), which can be checked using the method #isBroken().
 * Broken lock cannot be reused again.
 * </ul>
 *
 * <h1 class="header">Implementation issues</h1>
 * Ignite lock comes in two flavours: fair and non-fair. Non-fair lock assumes no ordering should be imposed
 * on acquiring threads; in case of contention, threads from all nodes compete for the lock once the lock is released.
 * In most cases this is the desired behaviour. However, in some cases, using the non-fair lock can lead to uneven load
 * distribution among nodes.
 * Fair lock solves this issue by imposing strict FIFO ordering policy at a cost of an additional transaction.
 * This ordering does not guarantee fairness of thread scheduling (similar to {@code java.util.concurrent.ReentrantLock}).
 * Thus, one of many threads on any node using a fair lock may obtain it multiple times in succession while other
 * active threads are not progressing and not currently holding the lock. Also note that the untimed tryLock method
 * does not honor the fairness setting. It will succeed if the lock is available even if other threads are waiting.
 *
 * </p>
 * As a rule of thumb, whenever there is a reasonable time window between successive calls to release and acquire
 * the lock, non-fair lock should be preferred:
 *
 * <pre> {@code
 *      while(someCondition){
 *          // do anything
 *          lock.lock();
 *          try{
 *              // ...
 *          }
 *          finally {
 *              lock.unlock();
 *          }
 *      }
 * }</pre>
 *
 * If successive calls to release/acquire are following immediately,
 * e.g.
 *
 * <pre> {@code
 *      while(someCondition){
 *          lock.lock();
 *          try {
 *              // do something
 *          }
 *          finally {
 *              lock.unlock();
 *          }
 *      }
 * }</pre>
 *
 * using the fair lock is reasonable in order to allow even distribution of load among nodes
 * (although overall throughput may be lower due to increased overhead).
 *
 */
public interface IgniteLock extends Lock, Closeable {
    /**
     * Name of atomic reentrant lock.
     *
     * @return Name of atomic reentrant lock.
     */
    public String name();

    /**
     * Acquires the distributed reentrant lock.
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
     * <li>Lock is broken (any node failed while owning this lock), and lock is created in
     * non-failoverSafe mode.
     *
     * <li>Local node is stopped.
     *
     * @throws IgniteException if the node is stopped or broken in non-failoverSafe mode
     */
    @Override void lock() throws IgniteException;

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
     * then {@link IgniteInterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * </ul>
     *
     * <p>{@link IgniteException} is thrown in case:
     *
     * <ul>
     *
     * <li>the lock is broken before or during the attempt to acquire this lock; or
     *
     * <li>local node is stopped,
     *
     * </ul>
     *
     * <p>In this implementation, as this method is an explicit
     * interruption point, preference is given to responding to the
     * interrupt over normal or reentrant acquisition of the lock.
     *
     * @throws IgniteInterruptedException if the current thread is interrupted
     * @throws IgniteException if the lock is broken in non-failoverSafe mode (any node failed while owning this lock),
     *         or local node is stopped
     */
    @Override public void lockInterruptibly() throws IgniteInterruptedException, IgniteException;

    /**
     * Acquires the lock only if it is free at the time of invocation.
     *
     * <p>Acquires the lock if it is available and returns immediately
     * with the value {@code true}.
     * If the lock is not available then this method will return
     * immediately with the value {@code false}.
     *
     * <p>A typical usage idiom for this method would be:
     *  <pre> {@code
     * Lock lock = ...;
     * if (lock.tryLock()) {
     *   try {
     *     // manipulate protected state
     *   } finally {
     *     lock.unlock();
     *   }
     * } else {
     *   // perform alternative actions
     * }}</pre>
     *
     * This usage ensures that the lock is unlocked if it was acquired, and
     * doesn't try to unlock if the lock was not acquired.
     *
     * If node is stopped, or any node failed while owning the lock in non-failoverSafe mode,
     * then {@link IgniteException} is thrown.
     *
     * @return {@code true} if the lock was acquired and
     *         {@code false} otherwise
     *
     * @throws IgniteException if node is stopped, or lock is already broken in non-failover safe mode
     */
    @Override public boolean tryLock() throws IgniteException;

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
     * </ul>
     * then {@link IgniteInterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * <p>{@link IgniteException} is thrown in case:
     *
     * <ul>
     *
     * <li>the lock is broken before or during the attempt to acquire this lock; or
     *
     * <li>local node is stopped,
     *
     * </ul>
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
     * @throws IgniteException if node is stopped, or lock is already broken in non-failover safe mode
     * @throws NullPointerException if the time unit is null
     */
    @Override public boolean tryLock(long timeout, TimeUnit unit) throws IgniteInterruptedException, IgniteException;

    /**
     * Releases the lock.
     *
     * If lock is not owned by current thread, then an {@link
     * IllegalMonitorStateException} is thrown.
     * If lock is already broken prior to invocation of this method, and
     * lock is created in non-failover safe mode, then {@link IgniteException} is thrown.
     *
     * @throws IllegalMonitorStateException if not owned by current thread
     * @throws IgniteException if node is stopped, or lock is already broken in non-failover safe mode
     */
    @Override void unlock() throws IgniteInterruptedException;

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
     * @throws IgniteException if the lock is not initialized or already removed
     */
    public IgniteCondition getOrCreateCondition(String name) throws IgniteException;

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
     * @throws IgniteException if the lock is not initialized or already removed
     */
    public int getHoldCount() throws IgniteException;

    /**
     * Queries if this lock is held by the current thread.
     *
     * @return {@code true} if current thread holds this lock and
     *         {@code false} otherwise
     * @throws IgniteException if the lock is not initialized or already removed
     */
    public boolean isHeldByCurrentThread() throws IgniteException;

    /**
     * Queries if this lock is held by any thread on any node. This method is
     * designed for use in monitoring of the system state,
     * not for synchronization control.
     *
     * @return {@code true} if any thread on this or any other node holds this lock and
     *         {@code false} otherwise
     * @throws IgniteException if the lock is not initialized or already removed
     */
    public boolean isLocked() throws IgniteException;

    /**
     * Queries whether any threads on this node are waiting to acquire this lock. Note that
     * because cancellations may occur at any time, a {@code true}
     * return does not guarantee that any other thread will ever
     * acquire this lock.  This method is designed primarily for use in
     * monitoring of the system state.
     *
     * @return {@code true} if there may be other threads on this node waiting to
     *         acquire the lock
     * @throws IgniteException if the lock is not initialized or already removed
     */
    public boolean hasQueuedThreads() throws IgniteException;

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
     * @throws IgniteException if the lock is not initialized or already removed
     */
    public boolean hasQueuedThread(Thread thread) throws IgniteException;

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
     * @throws IgniteException if the lock is not initialized or already removed
     */
    public boolean hasWaiters(IgniteCondition condition) throws IgniteException;

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
     * @throws IllegalMonitorStateException if this lock is not held
     * @throws IllegalArgumentException if the given condition is
     *         not associated with this lock
     * @throws NullPointerException if the condition is null
     * @throws IgniteException if the lock is not initialized or already removed
     */
    public int getWaitQueueLength(IgniteCondition condition) throws IgniteException;

    /**
     * Returns {@code true} if this lock is safe to use after node failure.
     * If not, IgniteInterruptedException is thrown on every other node after node failure.
     *
     * @return {@code true} if this reentrant lock has failoverSafe set true
     * @throws IgniteException if the lock is not initialized or already removed
     */
    public boolean isFailoverSafe();

    /**
     * Returns {@code true} if this lock is fair. Fairness flag can only be set on lock creation.
     *
     * @return {@code true} if this reentrant lock has fairness flag set true.
     * @throws IgniteException if the lock is not initialized or already removed
     */
    public boolean isFair();

    /**
     * Returns true if any node that owned the locked failed before releasing the lock.
     *
     * @return true if any node failed while owning the lock since the lock on this node was initialized.
     * @throws IgniteException if the lock is not initialized or already removed
     */
    public boolean isBroken() throws IgniteException;

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
