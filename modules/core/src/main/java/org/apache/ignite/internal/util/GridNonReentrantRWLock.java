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
 * Simple non-reentrant read-write lock that allows to release locks from threads
 * other than the acquiring. Write lock has higher priority than read lock.
 */
public final class GridNonReentrantRWLock {
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
        /** {@inheritDoc} */
        @Override protected boolean isHeldExclusively() {
            return getState() == Integer.MIN_VALUE;
        }

        /** {@inheritDoc} */
        @Override protected boolean tryAcquire(int ignored) {
            for (;;) {
                final int s = getState();
                int t;

                if (s > 0)
                    t = -s; // Mark that now we have a writer in a queue.
                else if (s == 0)
                    t = Integer.MIN_VALUE; // Try to exclusively lock.
                else
                    return false; // Already exclusively locked by someone else or have a queued writer.

                if (compareAndSetState(s, t))
                    return s == 0;
            }
        }

        /** {@inheritDoc} */
        @Override protected boolean tryRelease(int ignored) {
            if (!compareAndSetState(Integer.MIN_VALUE, 0))
                throw new IllegalMonitorStateException();

            return true;
        }

        /** {@inheritDoc} */
        @Override protected int tryAcquireShared(int ignored) {
            for (;;) {
                final int s = getState();

                if (s < 0)
                    return -1; // Write locked or have a queued writer.

                if (compareAndSetState(s, s + 1))
                    return 1;
            }
        }

        /** {@inheritDoc} */
        @Override protected boolean tryReleaseShared(int ignored) {
            for (;;) {
                final int s = getState();
                int t;

                if (s < 0) {
                    if (s == Integer.MIN_VALUE)
                        throw new IllegalMonitorStateException("Write locked.");

                    t = s + 1;
                }
                else {
                    if (s == 0)
                        throw new IllegalMonitorStateException("Unlocked.");

                    t = s - 1;
                }

                if (compareAndSetState(s, t))
                    return true;
            }
        }
    }
}
