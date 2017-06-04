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
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * Reentrant read-write lock (with higher priority for writers) that allows to
 * release locks from threads other than the acquiring.
 */
public final class GridReentrantRWLock {
    /** */
    private final Sync sync = new Sync();

    /**
     * @param time Max time to wait.
     * @param unit Time unit.
     * @return {@code true} If read lock taken successfully.
     * @throws InterruptedException If interrupted.
     */
    public boolean tryReadLock(long time, TimeUnit unit) throws InterruptedException {
        return sync.tryAcquireSharedNanos(0, unit.toNanos(time));
    }

    /**
     * @return {@code true} If read lock taken successfully.
     */
    public boolean tryReadLock() {
        return sync.tryAcquireShared(0) >= 0;
    }

    /**
     * @param time Max time to wait.
     * @param unit Time unit.
     * @return {@code true} If write lock taken successfully.
     * @throws InterruptedException If interrupted.
     */
    public boolean tryWriteLock(long time, TimeUnit unit) throws InterruptedException {
        return sync.tryAcquireNanos(0, unit.toNanos(time));
    }

    /**
     * @return {@code true} If write lock taken successfully.
     */
    public boolean tryWriteLock() {
        return sync.tryAcquire(0);
    }

    /**
     * @throws InterruptedException If interrupted.
     */
    public void readLock() throws InterruptedException {
        sync.acquireSharedInterruptibly(0);
    }

    /**
     * @throws InterruptedException If interrupted.
     */
    public void writeLock() throws InterruptedException {
        sync.acquireInterruptibly(0);
    }

    /**
     * Read unlock.
     */
    public void readUnlock() {
        sync.releaseShared(0);
    }

    /**
     * Write unlock.
     */
    public void writeUnlock() {
        sync.release(0);
    }

    /**
     */
    private static final class Sync extends AbstractQueuedSynchronizer {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param state State.
         * @return Read locks.
         */
        private static int readLocks(int state) {
            return state & 0xFFFF;
        }

        /**
         * @param state State.
         * @return Write locks.
         */
        private static int writeLocks(int state) {
            return (state >>> 16) & 0xFFFF;
        }

        /**
         * @param readLocks Read locks.
         * @param writeLocks Write locks.
         * @return State.
         */
        private static int state(int readLocks, int writeLocks) {
            assert readLocks >= 0 && readLocks <= 0xFFFF &&
                writeLocks >= 0 && writeLocks <= 0xFFFF;

            return (writeLocks << 16) | readLocks;
        }

        /** {@inheritDoc} */
        @Override protected boolean isHeldExclusively() {
            return writeLocks(getState()) != 0;
        }

        /** {@inheritDoc} */
        @Override protected boolean tryAcquire(int ignore) {
            for (;;) {
                final int s = getState();
                final int readLocks = readLocks(s);
                final int writeLocks = writeLocks(s);

                if (readLocks != 0) {
                    assert writeLocks == 0;

                    return false; // No luck.
                }

                if (writeLocks != 0 && getExclusiveOwnerThread() != Thread.currentThread())
                    return false; // No luck.

                int t = state(0, writeLocks + 1); // Enter or reenter.

                if (compareAndSetState(s, t)) {
                    if (writeLocks == 0) {
                        assert getExclusiveOwnerThread() == null;

                        setExclusiveOwnerThread(Thread.currentThread());
                    }

                    return true;
                }
            }
        }

        /** {@inheritDoc} */
        @Override protected boolean tryRelease(int ignored) {
            for (;;) {
                final int s = getState();
                int writeLocks = writeLocks(s);

                if (writeLocks == 0 || readLocks(s) != 0)
                    throw new IllegalMonitorStateException();

                writeLocks--;

                int t = state(0, writeLocks);

                // It is not a problem if we reset the owner thread earlier than
                // successful CAS, otherwise we will have a race with tryAcquire.
                if (writeLocks == 0)
                    setExclusiveOwnerThread(null);

                if (compareAndSetState(s, t))
                    return writeLocks == 0;
            }
        }

        /** {@inheritDoc} */
        @Override protected int tryAcquireShared(int ignored) {
            for (;;) {
                final int s = getState();
                final int writeLocks = writeLocks(s);

                // Respect writers over readers to avoid starvation.
                if (writeLocks != 0 || hasQueuedPredecessors())
                    return -1; // No luck.

                int t = state(readLocks(s) + 1, 0);

                if (compareAndSetState(s, t))
                    return 1;
            }
        }

        /** {@inheritDoc} */
        @Override protected boolean tryReleaseShared(int ignored) {
            for (;;) {
                final int s = getState();
                final int readLocks = readLocks(s);

                if (readLocks == 0 || writeLocks(s) != 0)
                    throw new IllegalMonitorStateException();

                int t = state(readLocks - 1, 0);

                if (compareAndSetState(s, t))
                    return true;
            }
        }
    }
}
