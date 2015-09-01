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

import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import sun.misc.Unsafe;

/**
 *
 */
@GridToStringExclude
public class GridSpinReadWriteLock {
    /** */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    private static final long PENDING_WLOCKS_OFFS;

    /** */
    private static final long STATE_OFFS;

    /**
     *
     */
    static {
        try {
            STATE_OFFS = UNSAFE.objectFieldOffset(GridSpinReadWriteLock.class.getDeclaredField("state"));

            PENDING_WLOCKS_OFFS =
                UNSAFE.objectFieldOffset(GridSpinReadWriteLock.class.getDeclaredField("pendingWLocks"));
        }
        catch (NoSuchFieldException e) {
            throw new Error(e);
        }
    }

    /** */
    private final ThreadLocal<Integer> readLockEntryCnt = new ThreadLocal<Integer>() {
        @Override protected Integer initialValue() {
            return 0;
        }
    };

    /** */
    @SuppressWarnings("UnusedDeclaration")
    private volatile int state;

    /** */
    @SuppressWarnings("UnusedDeclaration")
    private volatile int pendingWLocks;

    /** */
    private long writeLockOwner = -1;

    /** */
    private int writeLockEntryCnt;

    /**
     * Acquires write lock.
     */
    @SuppressWarnings("BusyWait")
    public void readLock() {
        int cnt = readLockEntryCnt.get();

        // Read lock reentry or acquiring read lock while holding write lock.
        if (cnt > 0 || Thread.currentThread().getId() == writeLockOwner) {
            assert state > 0 || state == -1;

            readLockEntryCnt.set(cnt + 1);

            return;
        }

        boolean interrupted = false;

        while (true) {
            int cur = state;

            assert cur >= -1;

            if (cur == -1 || pendingWLocks > 0) {
                try {
                    Thread.sleep(10);
                }
                catch (InterruptedException ignored) {
                    interrupted = true;
                }

                continue;
            }

            if (compareAndSet(STATE_OFFS, cur, cur + 1)) {
                if (interrupted)
                    Thread.currentThread().interrupt();

                break;
            }
        }

        readLockEntryCnt.set(1);
    }

    /**
     * Tries to acquire read lock.
     *
     * @return {@code true} if acquired.
     */
    public boolean tryReadLock() {
        int cnt = readLockEntryCnt.get();

        // Read lock reentry or acquiring read lock while holding write lock.
        if (cnt > 0 || Thread.currentThread().getId() == writeLockOwner) {
            assert state > 0 || state == -1;

            readLockEntryCnt.set(cnt + 1);

            return true;
        }

        while (true) {
            int cur = state;

            if (cur == -1 || pendingWLocks > 0)
                return false;

            if (compareAndSet(STATE_OFFS, cur, cur + 1)) {
                readLockEntryCnt.set(1);

                return true;
            }
        }
    }

    /**
     * Read unlock.
     */
    public void readUnlock() {
        int cnt = readLockEntryCnt.get();

        if (cnt == 0)
            throw new IllegalMonitorStateException();

        // Read unlock when holding write lock is performed here.
        if (cnt > 1 || Thread.currentThread().getId() == writeLockOwner) {
            assert state > 0 || state == -1;

            readLockEntryCnt.set(cnt - 1);

            return;
        }

        while (true) {
            int cur = state;

            assert cur > 0;

            if (compareAndSet(STATE_OFFS, cur, cur - 1)) {
                readLockEntryCnt.set(0);

                return;
            }
        }
    }

    /**
     * Acquires write lock.
     */
    @SuppressWarnings("BusyWait")
    public void writeLock() {
        long threadId = Thread.currentThread().getId();

        if (threadId == writeLockOwner) {
            assert state == -1;

            writeLockEntryCnt++;

            return;
        }

        // Increment pending write locks.
        while (true) {
            int pendingWLocks0 = pendingWLocks;

            if (compareAndSet(PENDING_WLOCKS_OFFS, pendingWLocks0, pendingWLocks0 + 1))
                break;
        }

        boolean interrupted = false;

        while (!compareAndSet(STATE_OFFS, 0, -1)) {
            try {
                Thread.sleep(10);
            }
            catch (InterruptedException ignored) {
                interrupted = true;
            }
        }

        // Decrement pending write locks.
        while (true) {
            int pendingWLocks0 = pendingWLocks;

            assert pendingWLocks0 > 0;

            if (compareAndSet(PENDING_WLOCKS_OFFS, pendingWLocks0, pendingWLocks0 - 1))
                break;
        }

        if (interrupted)
            Thread.currentThread().interrupt();

        assert writeLockOwner == -1;

        writeLockOwner = threadId;
        writeLockEntryCnt = 1;
    }

    /**
     * Acquires write lock without sleeping between unsuccessful attempts.
     */
    public void writeLock0() {
        long threadId = Thread.currentThread().getId();

        if (threadId == writeLockOwner) {
            assert state == -1;

            writeLockEntryCnt++;

            return;
        }

        // Increment pending write locks.
        while (true) {
            int pendingWLocks0 = pendingWLocks;

            if (compareAndSet(PENDING_WLOCKS_OFFS, pendingWLocks0, pendingWLocks0 + 1))
                break;
        }

        for (;;) {
            if (compareAndSet(STATE_OFFS, 0, -1))
                break;
        }

        // Decrement pending write locks.
        while (true) {
            int pendingWLocks0 = pendingWLocks;

            assert pendingWLocks0 > 0;

            if (compareAndSet(PENDING_WLOCKS_OFFS, pendingWLocks0, pendingWLocks0 - 1))
                break;
        }

        assert writeLockOwner == -1;

        writeLockOwner = threadId;
        writeLockEntryCnt = 1;
    }

    /**
     * @return {@code True} if blocked by current thread.
     */
    public boolean writeLockedByCurrentThread() {
        return writeLockOwner == Thread.currentThread().getId();
    }

    /**
     * Tries to acquire write lock.
     *
     * @return {@code True} if write lock has been acquired.
     */
    public boolean tryWriteLock() {
        long threadId = Thread.currentThread().getId();

        if (threadId == writeLockOwner) {
            assert state == -1;

            writeLockEntryCnt++;

            return true;
        }

        if (compareAndSet(STATE_OFFS, 0, -1)) {
            assert writeLockOwner == -1;

            writeLockOwner = threadId;
            writeLockEntryCnt = 1;

            return true;
        }

        return false;
    }

    /**
     * @param timeout Timeout.
     * @param unit Unit.
     * @return {@code True} if write lock has been acquired.
     * @throws InterruptedException If interrupted.
     */
    @SuppressWarnings("BusyWait")
    public boolean tryWriteLock(long timeout, TimeUnit unit) throws InterruptedException {
        long threadId = Thread.currentThread().getId();

        if (threadId == writeLockOwner) {
            assert state == -1;

            writeLockEntryCnt++;

            return true;
        }

        try {
            // Increment pending write locks.
            while (true) {
                int pendingWLocks0 = pendingWLocks;

                if (compareAndSet(PENDING_WLOCKS_OFFS, pendingWLocks0, pendingWLocks0 + 1))
                    break;
            }

            long end = U.currentTimeMillis() + unit.toMillis(timeout);

            while (true) {
                if (compareAndSet(STATE_OFFS, 0, -1)) {
                    assert writeLockOwner == -1;

                    writeLockOwner = threadId;
                    writeLockEntryCnt = 1;

                    return true;
                }

                Thread.sleep(10);

                if (end <= U.currentTimeMillis())
                    return false;
            }
        }
        finally {
            // Decrement pending write locks.
            while (true) {
                int pendingWLocks0 = pendingWLocks;

                assert pendingWLocks0 > 0;

                if (compareAndSet(PENDING_WLOCKS_OFFS, pendingWLocks0, pendingWLocks0 - 1))
                    break;
            }
        }
    }

    /**
     * Releases write lock.
     */
    public void writeUnlock() {
        long threadId = Thread.currentThread().getId();

        if (threadId != writeLockOwner)
            throw new IllegalMonitorStateException();

        if (writeLockEntryCnt > 1) {
            writeLockEntryCnt--;

            return;
        }

        writeLockEntryCnt = 0;
        writeLockOwner = -1;

        // Current thread holds write and read locks and is releasing
        // write lock now.
        int update = readLockEntryCnt.get() > 0 ? 1 : 0;

        boolean b = compareAndSet(STATE_OFFS, -1, update);

        assert b;
    }

    /**
     * @param offs Offset.
     * @param expect Expected.
     * @param update Update.
     * @return {@code True} on success.
     */
    private boolean compareAndSet(long offs, int expect, int update) {
        return UNSAFE.compareAndSwapInt(this, offs, expect, update);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridSpinReadWriteLock.class, this);
    }
}