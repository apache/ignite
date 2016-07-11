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

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 */
@SuppressWarnings({"NakedNotify", "SynchronizationOnLocalVariableOrMethodParameter", "CallToThreadYield", "WaitWhileNotSynced"})
public class OffheapReadWriteLock {
    /**
     * TODO benchmark optimal spin count.
     */
    public static final int SPIN_CNT = IgniteSystemProperties.getInteger("IGNITE_OFFHEAP_RWLOCK_SPIN_COUNT", 32);

    /** */
    public static final boolean USE_RANDOM_RW_POLICY = IgniteSystemProperties.getBoolean("IGNITE_OFFHEAP_RANDOM_RW_POLICY", false);

    /** Lock size. */
    public static final int LOCK_SIZE = 8;

    /** Maximum number of waiting threads, read or write. */
    public static final int MAX_WAITERS = 0xFFFF;

    /** */
    private final ReentrantLock[] locks;

    /** */
    private final Condition[] readConditions;

    /** */
    private final Condition[] writeConditions;

    /** */
    private final AtomicInteger[] balancers;

    /** */
    private int monitorsMask;

    /**
     * @param concLvl Concurrency level, must be a power of two.
     */
    public OffheapReadWriteLock(int concLvl) {
        if ((concLvl & concLvl - 1) != 0)
            throw new IllegalArgumentException("Concurrency level must be a power of 2: " + concLvl);

        monitorsMask = concLvl - 1;

        locks = new ReentrantLock[concLvl];
        readConditions = new Condition[concLvl];
        writeConditions = new Condition[concLvl];
        balancers = new AtomicInteger[concLvl];

        for (int i = 0; i < locks.length; i++) {
            ReentrantLock lock = new ReentrantLock();

            locks[i] = lock;
            readConditions[i] = lock.newCondition();
            writeConditions[i] = lock.newCondition();
            balancers[i] = new AtomicInteger(0);
        }
    }

    /**
     * @param lock Lock pointer to initialize.
     */
    public void init(long lock) {
        GridUnsafe.putLong(lock, 0);
    }

    /**
     * @param lock Lock address.
     */
    public void readLock(long lock) {
        long state = GridUnsafe.getLongVolatile(null, lock);

        // Check write waiters first.
        int writeWaitCnt = writersWaitCount(state);

        if (writeWaitCnt == 0) {
            for (int i = 0; i < SPIN_CNT; i++) {
                if (canReadLock(state)) {
                    if (GridUnsafe.compareAndSwapLong(null, lock, state, updateState(state, 1, 0, 0)))
                        return;
                    else
                        // Retry CAS, do not count as spin cycle.
                        i--;
                }

                state = GridUnsafe.getLongVolatile(null, lock);
            }
        }

        int idx = lockIndex(lock);

        ReentrantLock lockObj = locks[idx];

        lockObj.lock();

        try {
            updateReadersWaitCount(lock, lockObj, 1);

            while (true) {
                state = waitCanLock(lock, lockObj, readConditions[idx], true);

                assert canReadLock(state);

                if (GridUnsafe.compareAndSwapLong(null, lock, state, updateState(state, 1, -1, 0)))
                    break;
            }
        }
        finally {
            lockObj.unlock();
        }
    }

    /**
     * @param lock Lock address.
     */
    public void readUnlock(long lock) {
        while (true) {
            long state = GridUnsafe.getLongVolatile(null, lock);

            assert state > 0;

            long updated = updateState(state, -1, 0, 0);

            if (GridUnsafe.compareAndSwapLong(null, lock, state, updated)) {
                // Notify monitor if we were CASed to zero and there is a write waiter.
                if (lockCount(updated) == 0 && writersWaitCount(updated) > 0) {
                    int idx = lockIndex(lock);

                    ReentrantLock lockObj = locks[idx];

                    lockObj.lock();

                    try {
                        writeConditions[idx].signalAll();
                    }
                    finally {
                        lockObj.unlock();
                    }
                }

                return;
            }
        }
    }

    /**
     * @param lock Lock address.
     */
    public void writeLock(long lock) {
        for (int i = 0; i < SPIN_CNT; i++) {
            long state = GridUnsafe.getLongVolatile(null, lock);

            if (canWriteLock(state)) {
                if (GridUnsafe.compareAndSwapLong(null, lock, state, updateState(state, -1, 0, 0)))
                    return;
                else
                    // Retry CAS, do not count as spin cycle.
                    i--;
            }
        }

        int idx = lockIndex(lock);

        ReentrantLock lockObj = locks[idx];

        lockObj.lock();

        try {
            updateWritersWaitCount(lock, lockObj, 1);

            while (true) {
                long state = waitCanLock(lock, lockObj, writeConditions[idx], false);

                // Update lock and write wait count simultaneously.
                if (GridUnsafe.compareAndSwapLong(null, lock, state, updateState(state, -1, 0, -1)))
                    break;
            }
        }
        finally {
            lockObj.unlock();
        }
    }

    /**
     * @param lock Lock address.
     */
    public void writeUnlock(long lock) {
        long updated;

        while (true) {
            long state = GridUnsafe.getLongVolatile(null, lock);

            assert lockCount(state) == -1;

            updated = updateState(state, 1, 0, 0);

            if (GridUnsafe.compareAndSwapLong(null, lock, state, updated))
                break;
        }

        int writeWaitCnt = writersWaitCount(updated);
        int readWaitCnt = readersWaitCount(updated);

        if (writeWaitCnt > 0 || readWaitCnt > 0) {
            int idx = lockIndex(lock);

            ReentrantLock lockObj = locks[idx];

            lockObj.lock();

            try {
                if (writeWaitCnt == 0)
                    readConditions[idx].signalAll();
                else if (readWaitCnt == 0)
                    writeConditions[idx].signalAll();
                else {
                    // We have both writers and readers.
                    if (USE_RANDOM_RW_POLICY) {
                        boolean write = (balancers[idx].incrementAndGet() & 0x1) == 0;

                        (write ? writeConditions : readConditions)[idx].signalAll();
                    }
                    else
                        writeConditions[idx].signalAll();
                }
            }
            finally {
                lockObj.unlock();
            }
        }
    }

    /**
     * Upgrades a read lock to a write lock. If this thread is the only read-owner of the read lock,
     * this method will atomically upgrade the read lock to the write lock. In this case {@code true}
     * will be returned. If not, the read lock will be released and write lock will be acquired, leaving
     * a potential gap for other threads to modify a protected resource. In this case this method will return
     * {@code false}.
     * <p>
     * After this method has been called, there is no need to call to {@link #readUnlock(long)} because
     * read lock will be released in any case.
     *
     * @param lock Lock to upgrade.
     * @return {@code True} if successfully traded read lock to write lock without leaving a gap.
     *      Returns {@code false} otherwise, in this case the resource state must be re-validated.
     */
    public boolean upgradeToWriteLock(long lock) {
        for (int i = 0; i < SPIN_CNT; i++) {
            long state = GridUnsafe.getLongVolatile(null, lock);

            if (lockCount(state) == 1) {
                if (GridUnsafe.compareAndSwapLong(null, lock, state, updateState(state, -2, 0, 0)))
                    return true;
                else
                    // Retry CAS, do not count as spin cycle.
                    i--;
            }
        }

        int idx = lockIndex(lock);

        ReentrantLock lockObj = locks[idx];

        lockObj.lock();

        try {
            // First, add write waiter.
            while (true) {
                long state = GridUnsafe.getLongVolatile(null, lock);

                if (lockCount(state) == 1) {
                    if (GridUnsafe.compareAndSwapLong(null, lock, state, updateState(state, -2, 0, 0)))
                        return true;
                }

                // Remove read lock and add write waiter simultaneously.
                if (GridUnsafe.compareAndSwapLong(null, lock, state, updateState(state, -1, 0, 1)))
                    break;
            }

            while (true) {
                long state = waitCanLock(lock, lockObj, writeConditions[idx], false);

                // Update lock and write wait count simultaneously.
                if (GridUnsafe.compareAndSwapLong(null, lock, state, updateState(state, -1, 0, -1)))
                    break;
            }
        }
        finally {
            lockObj.unlock();
        }

        return false;
    }

    /**
     *
     */
    private long waitCanLock(long lock, ReentrantLock lockObj, Condition waitCond, boolean read) {
        assert lockObj.isHeldByCurrentThread();

        boolean interrupted = false;

        try {
            while (true) {
                try {
                    long state = GridUnsafe.getLongVolatile(null, lock);

                    if (read && canReadLock(state) || !read && canWriteLock(state))
                        return state;

                    waitCond.await();
                }
                catch (InterruptedException ignore) {
                    interrupted = true;
                }
            }
        }
        finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    /**
     * @param lock Lock address.
     * @return Lock monitor object.
     */
    private int lockIndex(long lock) {
        return U.safeAbs(U.hash(lock)) & monitorsMask;
    }

    /**
     * @param state Lock state.
     * @return {@code True} if write lock is not acquired.
     */
    private boolean canReadLock(long state) {
        return lockCount(state) >= 0;
    }

    /**
     * @param state Lock state.
     * @return {@code True} if no read locks are acquired.
     */
    private boolean canWriteLock(long state) {
        return lockCount(state) == 0;
    }

    /**
     * @param state State.
     * @return Lock count.
     */
    private int lockCount(long state) {
        return (int)state;
    }

    /**
     * @param state State.
     * @return Writers wait count.
     */
    private int writersWaitCount(long state) {
        return (int)((state >>> 48) & 0xFFFF);
    }

    /**
     * @param state State.
     * @return Readers wait count.
     */
    private int readersWaitCount(long state) {
        return (int)((state >>> 32) & 0xFFFF);
    }

    /**
     * @param state State to update.
     * @param lockDelta Lock counter delta.
     * @param readersWaitDelta Readers wait delta.
     * @param writersWaitDelta Writers wait delta.
     * @return Modified state.
     */
    private long updateState(long state, int lockDelta, int readersWaitDelta, int writersWaitDelta) {
        int lock = lockCount(state);
        int readersWait = readersWaitCount(state);
        int writersWait = writersWaitCount(state);

        lock += lockDelta;
        readersWait += readersWaitDelta;
        writersWait += writersWaitDelta;

        if (readersWait > MAX_WAITERS)
            throw new IllegalStateException("Failed to add read waiter (too many waiting threads): " + MAX_WAITERS);

        if (writersWait > MAX_WAITERS)
            throw new IllegalStateException("Failed to add write waiter (too many waiting threads): " + MAX_WAITERS);

        assert readersWait >= 0 : readersWait;
        assert writersWait >= 0 : writersWait;
        assert lock >= -1;

        return ((long)writersWait << 48) | ((long)readersWait << 32) | (lock & 0xFFFFFFFFL);
    }

    /**
     * Updates readers wait count.
     *
     * @param lock Lock to update.
     * @param delta Delta to update.
     */
    private void updateReadersWaitCount(long lock, ReentrantLock lockObj, int delta) {
        assert lockObj.isHeldByCurrentThread();

        while (true) {
            // Safe to do non-volatile read because of CAS below.
            long state = GridUnsafe.getLongVolatile(null, lock);

            long updated = updateState(state, 0, delta, 0);

            if (GridUnsafe.compareAndSwapLong(null, lock, state, updated))
                return;
        }
    }

    /**
     * Updates writers wait count.
     *
     * @param lock Lock to update.
     * @param delta Delta to update.
     */
    private void updateWritersWaitCount(long lock, ReentrantLock lockObj, int delta) {
        assert lockObj.isHeldByCurrentThread();

        while (true) {
            long state = GridUnsafe.getLongVolatile(null, lock);

            long updated = updateState(state, 0, 0, delta);

            if (GridUnsafe.compareAndSwapLong(null, lock, state, updated))
                return;
        }
    }
}
