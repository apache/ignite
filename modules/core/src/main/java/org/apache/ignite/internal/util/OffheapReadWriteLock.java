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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Lock state structure is as follows:
 * <pre>
 *     +----------------+---------------+---------+----------+
 *     | WRITE WAIT CNT | READ WAIT CNT |   TAG   | LOCK CNT |
 *     +----------------+---------------+---------+----------+
 *     |     2 bytes    |     2 bytes   | 2 bytes |  2 bytes |
 *     +----------------+---------------+---------+----------+
 * </pre>
 */
public class OffheapReadWriteLock {
    /**
     * TODO benchmark optimal spin count.
     */
    public static final int SPIN_CNT = IgniteSystemProperties.getInteger("IGNITE_OFFHEAP_RWLOCK_SPIN_COUNT", 32);

    /** */
    public static final boolean USE_RANDOM_RW_POLICY = IgniteSystemProperties.getBoolean("IGNITE_OFFHEAP_RANDOM_RW_POLICY", false);

    /** Always lock tag. */
    public static final int TAG_LOCK_ALWAYS = -1;

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
    public void init(long lock, int tag) {
        tag &= 0xFFFF;

        assert tag != 0;

        GridUnsafe.putLong(lock, (long)tag << 16);
    }

    /**
     * @param lock Lock address.
     */
    public boolean readLock(long lock, int tag) {
        long state = GridUnsafe.getLongVolatile(null, lock);

        assert state != 0;

        // Check write waiters first.
        int writeWaitCnt = writersWaitCount(state);

        if (writeWaitCnt == 0) {
            for (int i = 0; i < SPIN_CNT; i++) {
                if (!checkTag(state, tag))
                    return false;

                if (canReadLock(state)) {
                    if (GridUnsafe.compareAndSwapLong(null, lock, state, updateState(state, 1, 0, 0)))
                        return true;
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

            return waitAcquireReadLock(lock, idx, tag);
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

            if (lockCount(state) <= 0)
                throw new IllegalMonitorStateException("Attempted to release a read lock while not holding it " +
                    "[lock=" + U.hexLong(lock) + ", state=" + U.hexLong(state) + ']');

            long updated = updateState(state, -1, 0, 0);

            assert updated != 0;

            if (GridUnsafe.compareAndSwapLong(null, lock, state, updated)) {
                // Notify monitor if we were CASed to zero and there is a write waiter.
                if (lockCount(updated) == 0 && writersWaitCount(updated) > 0) {
                    int idx = lockIndex(lock);

                    ReentrantLock lockObj = locks[idx];

                    lockObj.lock();

                    try {
                        // Note that we signal all waiters for this stripe. Since not all waiters in this
                        // stripe/index belong to this particular lock, we can't wake up just one of them.
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
    public boolean tryWriteLock(long lock, int tag) {
        long state = GridUnsafe.getLongVolatile(null, lock);

        return checkTag(state, tag) && canWriteLock(state) &&
            GridUnsafe.compareAndSwapLong(null, lock, state, updateState(state, -1, 0, 0));
    }

    /**
     * @param lock Lock address.
     */
    public boolean writeLock(long lock, int tag) {
        assert tag != 0;

        for (int i = 0; i < SPIN_CNT; i++) {
            long state = GridUnsafe.getLongVolatile(null, lock);

            assert state != 0;

            if (!checkTag(state, tag))
                return false;

            if (canWriteLock(state)) {
                if (GridUnsafe.compareAndSwapLong(null, lock, state, updateState(state, -1, 0, 0)))
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
            updateWritersWaitCount(lock, lockObj, 1);

            return waitAcquireWriteLock(lock, idx, tag);
        }
        finally {
            lockObj.unlock();
        }
    }

    /**
     * @param lock Lock to check.
     * @return {@code True} if write lock is held by any thread for the given offheap RW lock.
     */
    public boolean isWriteLocked(long lock) {
        return lockCount(GridUnsafe.getLongVolatile(null, lock)) == -1;
    }

    /**
     * @param lock Lock to check.
     * @return {@code True} if at least one read lock is held by any thread for the given offheap RW lock.
     */
    public boolean isReadLocked(long lock) {
        return lockCount(GridUnsafe.getLongVolatile(null, lock)) > 0;
    }

    /**
     * @param lock Lock address.
     */
    public void writeUnlock(long lock, int tag) {
        long updated;

        assert tag != 0;

        while (true) {
            long state = GridUnsafe.getLongVolatile(null, lock);

            if (lockCount(state) != -1)
                throw new IllegalMonitorStateException("Attempted to release write lock while not holding it " +
                    "[lock=" + U.hexLong(lock) + ", state=" + U.hexLong(state) + ']');

            updated = releaseWithTag(state, tag);

            assert updated != 0;

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
                signalNextWaiter(writeWaitCnt, readWaitCnt, idx);
            }
            finally {
                lockObj.unlock();
            }
        }
    }

    /**
     * @param writeWaitCnt Writers wait count.
     * @param readWaitCnt Readers wait count.
     * @param idx Lock index.
     */
    private void signalNextWaiter(int writeWaitCnt, int readWaitCnt, int idx) {
        // Note that we signal all waiters for this stripe. Since not all waiters in this stripe/index belong
        // to this particular lock, we can't wake up just one of them.
        if (writeWaitCnt == 0) {
            Condition readCondition = readConditions[idx];

            readCondition.signalAll();
        }
        else if (readWaitCnt == 0) {
            Condition writeCond = writeConditions[idx];

            writeCond.signalAll();
        }
        else {
            // We have both writers and readers.
            if (USE_RANDOM_RW_POLICY) {
                boolean write = (balancers[idx].incrementAndGet() & 0x1) == 0;

                Condition cond = (write ? writeConditions : readConditions)[idx];

                cond.signalAll();
            }
            else {
                Condition cond = writeConditions[idx];

                cond.signalAll();
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
     * @return {@code null} if tag validation failed, {@code true} if successfully traded the read lock to
     *      the write lock without leaving a gap. Returns {@code false} otherwise, in this case the resource
     *      state must be re-validated.
     */
    public Boolean upgradeToWriteLock(long lock, int tag) {
        for (int i = 0; i < SPIN_CNT; i++) {
            long state = GridUnsafe.getLongVolatile(null, lock);

            if (!checkTag(state, tag))
                return null;

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

                if (!checkTag(state, tag))
                    return null;

                if (lockCount(state) == 1) {
                    if (GridUnsafe.compareAndSwapLong(null, lock, state, updateState(state, -2, 0, 0)))
                        return true;
                    else
                        continue;
                }

                // Remove read lock and add write waiter simultaneously.
                if (GridUnsafe.compareAndSwapLong(null, lock, state, updateState(state, -1, 0, 1)))
                    break;
            }

            return waitAcquireWriteLock(lock, idx, tag);
        }
        finally {
            lockObj.unlock();
        }
    }

    /**
     * Acquires read lock in waiting loop.
     *
     * @param lock Lock address.
     * @param lockIdx Lock index.
     * @param tag Validation tag.
     * @return {@code True} if lock was acquired, {@code false} if tag validation failed.
     */
    private boolean waitAcquireReadLock(long lock, int lockIdx, int tag) {
        ReentrantLock lockObj = locks[lockIdx];
        Condition waitCond = readConditions[lockIdx];

        assert lockObj.isHeldByCurrentThread();

        boolean interrupted = false;

        try {
            while (true) {
                try {
                    long state = GridUnsafe.getLongVolatile(null, lock);

                    if (!checkTag(state, tag)) {
                        // We cannot lock with this tag, release waiter.
                        long updated = updateState(state, 0, -1, 0);

                        if (GridUnsafe.compareAndSwapLong(null, lock, state, updated)) {
                            int writeWaitCnt = writersWaitCount(updated);
                            int readWaitCnt = readersWaitCount(updated);

                            signalNextWaiter(writeWaitCnt, readWaitCnt, lockIdx);

                            return false;
                        }
                    }
                    else if (canReadLock(state)) {
                        long updated = updateState(state, 1, -1, 0);

                        if (GridUnsafe.compareAndSwapLong(null, lock, state, updated))
                            return true;
                    }
                    else
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
     * Acquires write lock in waiting loop.
     *
     * @param lock Lock address.
     * @param lockIdx Lock index.
     * @param tag Validation tag.
     * @return {@code True} if lock was acquired, {@code false} if tag validation failed.
     */
    private boolean waitAcquireWriteLock(long lock, int lockIdx, int tag) {
        ReentrantLock lockObj = locks[lockIdx];
        Condition waitCond = writeConditions[lockIdx];

        assert lockObj.isHeldByCurrentThread();

        boolean interrupted = false;

        try {
            while (true) {
                try {
                    long state = GridUnsafe.getLongVolatile(null, lock);

                    if (!checkTag(state, tag)) {
                        // We cannot lock with this tag, release waiter.
                        long updated = updateState(state, 0, 0, -1);

                        if (GridUnsafe.compareAndSwapLong(null, lock, state, updated)) {
                            int writeWaitCnt = writersWaitCount(updated);
                            int readWaitCnt = readersWaitCount(updated);

                            signalNextWaiter(writeWaitCnt, readWaitCnt, lockIdx);

                            return false;
                        }
                    }
                    else if (canWriteLock(state)) {
                        long updated = updateState(state, -1, 0, -1);

                        if (GridUnsafe.compareAndSwapLong(null, lock, state, updated))
                            return true;
                    }
                    else
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
     * Returns index of lock object corresponding to the stripe of this lock address.
     *
     * @param lock Lock address.
     * @return Lock monitor object that corresponds to the stripe for this lock address.
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
     * @param tag Tag.
     */
    private boolean checkTag(long state, int tag) {
        // If passed in tag is negative, lock regardless of the state.
        return tag < 0 || tag(state) == tag;
    }

    /**
     * @param state State.
     * @return Lock count.
     */
    private int lockCount(long state) {
        return (short)(state & 0xFFFF);
    }

    /**
     * @param state Lock state.
     * @return Lock tag.
     */
    private int tag(long state) {
        return (int)((state >>> 16) & 0xFFFF);
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
        int tag = tag(state);
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

        return buildState(writersWait, readersWait, tag, lock);
    }

    /**
     * @param state State to update.
     * @return Modified state.
     */
    private long releaseWithTag(long state, int newTag) {
        int lock = lockCount(state);
        int readersWait = readersWaitCount(state);
        int writersWait = writersWaitCount(state);
        int tag = newTag == TAG_LOCK_ALWAYS ? tag(state) : newTag & 0xFFFF;

        lock += 1;

        assert readersWait >= 0 : readersWait;
        assert writersWait >= 0 : writersWait;
        assert lock >= -1;

        return buildState(writersWait, readersWait, tag, lock);
    }

    /**
     * Creates state from counters.
     *
     * @param writersWait Writers wait count.
     * @param readersWait Readers wait count.
     * @param tag Tag.
     * @param lock Lock count.
     * @return State.
     */
    private long buildState(int writersWait, int readersWait, int tag, int lock) {
        assert (tag & 0xFFFF0000) == 0;

        return ((long)writersWait << 48) | ((long)readersWait << 32) | ((tag & 0x0000FFFFL) << 16) | (lock & 0xFFFFL);
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
