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

package org.apache.ignite.internal.util;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.TimeUnit;

import org.apache.ignite.internal.tostring.S;

/**
 * <p>Spin read-write lock.
 * Its blocking methods use the spinwait strategy. When they do so, they are not interruptible (that is, they do not
 * break their loop on interruption signal).
 * <p>The locks are reentrant (that is, the same thread can acquire the same lock a few times in a row and then
 * release them same number of times.
 * <p>Write lock acquire requests are prioritized over read lock acquire requests. That is, if both read and write lock
 * acquire requests are received when the write lock is held by someone else, then, on its release,
 * the write lock attempt will be served first.
 */
public class IgniteSpinReadWriteLock {
    /** Signals that nobody currently owns the read lock. */
    private static final long NO_OWNER = -1;

    /**
     * State -1 means that the write lock is acquired.
     *
     * @see #state
     */
    private static final int WRITE_LOCKED = -1;

    /**
     * State 0 means that both read and write locks are available for acquiring.
     *
     * @see #state
     */
    private static final int AVAILABLE = 0;

    /** How much time to sleep on each iteration of a spin loop (milliseconds). */
    private static final int SLEEP_MILLIS = 10;

    /** {@link VarHandle} used to access the {@code pendingWLocks} field. */
    private static final VarHandle PENDING_WLOCKS_VH;

    /** {@link VarHandle} used to access the {@code state} field. */
    private static final VarHandle STATE_VH;

    static {
        try {
            STATE_VH = MethodHandles.lookup()
                    .findVarHandle(IgniteSpinReadWriteLock.class, "state", int.class);

            PENDING_WLOCKS_VH = MethodHandles.lookup()
                    .findVarHandle(IgniteSpinReadWriteLock.class, "pendingWLocks", int.class);
        }
        catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** Number of times read lock was acquired, per thread (used to track reentrance). */
    private final ThreadLocal<Integer> readLockEntryCnt = ThreadLocal.withInitial(() -> 0);

    /**
     * Main state of the lock.
     * <ul>
     *     <li>Positive when the read lock has been acquired by at least one thread; in such case, this number equals
     *     to the number of threads holding the read lock. In such state, the read lock may be acquired by any thread,
     *     while an attempt to acquire the write lock will block or fail.</li>
     *     <li>Zero when neither the read lock, nor the write lock has been acquired by any thread. This state allows
     *     a thread to acquire either the read or the write lock at will.</li>
     *     <li>-1 when the write lock has been acquired by exactly one thread. In such state, any attempt to acquire the read
     *     or the write lock by any thread (but the thread holding the write lock) will block or fail.</li>
     * </ul>
     */
    private volatile int state;

    /**
     * Number of pending write attempts to acquire the write lock. Currently it is only used to prioritize
     * write lock attempts over read lock attempts when the write lock has been released (so, if both an attempt to acquire
     * the write lock and an attempt to acquire the read lock are waiting for write lock to be released, a write lock attempt
     * will be served first when the release happens).
     */
    private volatile int pendingWLocks;

    /** ID of the thread holding write lock (or {@link #NO_OWNER} if the write lock is not held). */
    private long writeLockOwner = NO_OWNER;

    /** Number of times the write lock holder locked the write lock (used to track reentrance). */
    private int writeLockEntryCnt;

    /**
     * Acquires the read lock. If the write lock is held by another thread, this blocks until the write lock is released
     * (and until all concurrent write locks are acquired and released, as this class pripritizes write lock attempts
     * over read lock attempts).
     */
    @SuppressWarnings("BusyWait")
    public void readLock() {
        int cnt = readLockEntryCnt.get();

        // Read lock reentry or acquiring read lock while holding write lock.
        if (alreadyHoldingAnyLock(cnt)) {
            incrementCurrentThreadReadLockCount(cnt);

            return;
        }

        boolean interrupted = false;

        while (true) {
            int curState = state;

            assert curState >= WRITE_LOCKED;

            if (writeLockedOrGoingToBe(curState)) {
                try {
                    Thread.sleep(SLEEP_MILLIS);
                }
                catch (InterruptedException ignored) {
                    interrupted = true;
                }

                continue;
            }

            if (tryAdvanceStateToReadLocked(curState)) {
                if (interrupted)
                    Thread.currentThread().interrupt();

                break;
            }
        }

        readLockEntryCnt.set(1);
    }

    /**
     * Whether the current thread already holds any lock.
     *
     * @param currentThreadReadLockAcquiredCount how many times current thread acquired (without releasing yet)
     *                                           the read lock
     * @return true if current thread already holds any lock
     */
    private boolean alreadyHoldingAnyLock(int currentThreadReadLockAcquiredCount) {
        return currentThreadReadLockAcquiredCount > 0 || writeLockedByCurrentThread();
    }

    /***/
    private void incrementCurrentThreadReadLockCount(int cnt) {
        assert state > 0 || state == WRITE_LOCKED;

        readLockEntryCnt.set(cnt + 1);
    }

    /***/
    private boolean writeLockedOrGoingToBe(int curState) {
        return curState == WRITE_LOCKED || pendingWLocks > 0;
    }

    /***/
    private boolean tryAdvanceStateToReadLocked(int curState) {
        return compareAndSet(STATE_VH, curState, curState + 1);
    }

    /**
     * Tries to acquire the read lock. No spinwait is used if the lock cannot be acquired immediately.
     *
     * @return {@code true} if acquired, {@code false} if write lock is already held by someone else
     */
    public boolean tryReadLock() {
        int cnt = readLockEntryCnt.get();

        // Read lock reentry or acquiring read lock while holding write lock.
        if (alreadyHoldingAnyLock(cnt)) {
            incrementCurrentThreadReadLockCount(cnt);

            return true;
        }

        while (true) {
            int curState = state;

            if (writeLockedOrGoingToBe(curState))
                return false;

            if (tryAdvanceStateToReadLocked(curState)) {
                readLockEntryCnt.set(1);

                return true;
            }
        }
    }

    /**
     * Releases the read lock.
     *
     * @throws IllegalMonitorStateException thrown if the current thread does not hold the read lock
     */
    public void readUnlock() {
        int cnt = readLockEntryCnt.get();

        if (cnt == 0)
            throw new IllegalMonitorStateException();

        // Read unlock when holding write lock is performed here.
        if (cnt > 1 || writeLockedByCurrentThread()) {
            assert state > 0 || state == WRITE_LOCKED;

            readLockEntryCnt.set(cnt - 1);

            return;
        }

        while (true) {
            int curState = state;

            assert curState > 0;

            if (compareAndSet(STATE_VH, curState, curState - 1)) {
                readLockEntryCnt.set(0);

                return;
            }
        }
    }

    /**
     * Acquires the write lock waiting, if needed. The thread will block until all other threads release both read
     * and write locks.
     */
    @SuppressWarnings("BusyWait")
    public void writeLock() {
        if (writeLockedByCurrentThread()) {
            incrementWriteLockCount();

            return;
        }

        boolean interrupted = false;

        incrementPendingWriteLocks();
        try {
            while (!trySwitchStateToWriteLocked()) {
                try {
                    Thread.sleep(SLEEP_MILLIS);
                }
                catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
        }
        finally {
            decrementPendingWriteLocks();
        }

        if (interrupted)
            Thread.currentThread().interrupt();

        finishWriteLockAcquire();
    }

    /***/
    private void incrementWriteLockCount() {
        assert state == WRITE_LOCKED;

        writeLockEntryCnt++;
    }

    /***/
    private void incrementPendingWriteLocks() {
        while (true) {
            int curPendingWLocks = pendingWLocks;

            if (compareAndSet(PENDING_WLOCKS_VH, curPendingWLocks, curPendingWLocks + 1))
                break;
        }
    }

    /***/
    private boolean trySwitchStateToWriteLocked() {
        return compareAndSet(STATE_VH, AVAILABLE, WRITE_LOCKED);
    }

    /***/
    private void decrementPendingWriteLocks() {
        while (true) {
            int curPendingWLocks = pendingWLocks;

            assert curPendingWLocks > 0;

            if (compareAndSet(PENDING_WLOCKS_VH, curPendingWLocks, curPendingWLocks - 1))
                break;
        }
    }

    /***/
    private void finishWriteLockAcquire() {
        assert writeLockOwner == NO_OWNER;

        writeLockOwner = Thread.currentThread().getId();
        writeLockEntryCnt = 1;
    }

    /**
     * Acquires the write lock without sleeping between unsuccessful attempts. Instead, the spinwait eats cycles of
     * the core it gets at full speed. It is non-interruptible as its {@link #writeLock()} cousin.
     */
    public void writeLockBusy() {
        if (writeLockedByCurrentThread()) {
            incrementWriteLockCount();

            return;
        }

        incrementPendingWriteLocks();
        try {
            while (!trySwitchStateToWriteLocked());
        }
        finally {
            decrementPendingWriteLocks();
        }

        finishWriteLockAcquire();
    }

    /**
     * @return {@code True} if blocked by current thread.
     */
    public boolean writeLockedByCurrentThread() {
        return writeLockOwner == Thread.currentThread().getId();
    }

    /**
     * Tries to acquire the write lock. Never blocks: if any lock has already been acquired by someone else,
     * returns {@code false} immediately.
     *
     * @return {@code true} if the write lock has been acquired, {@code false} otherwise
     */
    public boolean tryWriteLock() {
        if (writeLockedByCurrentThread()) {
            incrementWriteLockCount();

            return true;
        }

        if (trySwitchStateToWriteLocked()) {
            finishWriteLockAcquire();

            return true;
        }

        return false;
    }

    /**
     * Tries to acquire the write lock with timeout. If it gets the write lock before the timeout expires, then
     * returns {@code true}. If the timeout expires before the lock becomes available, returns {@code false}.
     *
     * @param timeout Timeout.
     * @param unit Unit.
     * @return {@code true} if the write lock has been acquired in time; {@code false} otherwise
     * @throws InterruptedException If interrupted.
     */
    @SuppressWarnings("BusyWait")
    public boolean tryWriteLock(long timeout, TimeUnit unit) throws InterruptedException {
        if (writeLockedByCurrentThread()) {
            incrementWriteLockCount();

            return true;
        }

        incrementPendingWriteLocks();
        try {
            long startNanos = System.nanoTime();

            long timeoutNanos = unit.toNanos(timeout);

            while (true) {
                if (trySwitchStateToWriteLocked()) {
                    finishWriteLockAcquire();

                    return true;
                }

                Thread.sleep(SLEEP_MILLIS);

                if (System.nanoTime() - startNanos >= timeoutNanos)
                    return false;
            }
        }
        finally {
            decrementPendingWriteLocks();
        }
    }

    /**
     * Releases the write lock.
     *
     * @throws IllegalMonitorStateException thrown if the current thread does not hold the write lock
     */
    public void writeUnlock() {
        if (!writeLockedByCurrentThread())
            throw new IllegalMonitorStateException();

        if (writeLockEntryCnt > 1) {
            writeLockEntryCnt--;

            return;
        }

        writeLockEntryCnt = 0;
        writeLockOwner = NO_OWNER;

        // Current thread holds write and read locks and is releasing
        // write lock now.
        int update = readLockEntryCnt.get() > 0 ? 1 : AVAILABLE;

        boolean b = compareAndSet(STATE_VH, WRITE_LOCKED, update);

        assert b;
    }

    /**
     * @param varHandle VarHandle.
     * @param expect Expected.
     * @param update Update.
     * @return {@code True} on success.
     */
    private boolean compareAndSet(VarHandle varHandle, int expect, int update) {
        return varHandle.compareAndSet(this, expect, update);
    }

    /**
     * Returns the count of pending write lock requests count. Only used by tests, should not be used in production code.
     *
     * @return count of pending requests to get the write lock
     */
    int pendingWriteLocksCount() {
        return pendingWLocks;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteSpinReadWriteLock.class, this);
    }
}
