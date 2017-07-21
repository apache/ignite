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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.NotNull;

/**
 * ReadWriteLock with striping mechanics.
 * Compared to {@link ReentrantReadWriteLock} it has slightly improved performance of {@link ReadWriteLock#readLock()}
 * operations at the cost of {@link ReadWriteLock#writeLock()} operations and memory consumption.
 * It also supports reentrancy semantics like {@link ReentrantReadWriteLock}.
 */
public class StripedCompositeReadWriteLock implements ReadWriteLock {
    /** Index generator. */
    private static final AtomicInteger IDX_GEN = new AtomicInteger();

    /** Index. */
    private static final ThreadLocal<Integer> IDX = new ThreadLocal<Integer>() {
        @Override protected Integer initialValue() {
            return IDX_GEN.incrementAndGet();
        }
    };

    /** Locks. */
    private final ReentrantReadWriteLock[] locks;

    /** Composite write lock. */
    private final WriteLock writeLock;

    /**
     * Creates a new instance with given concurrency level.
     *
     * @param concurrencyLvl Number of internal read locks.
     */
    public StripedCompositeReadWriteLock(int concurrencyLvl) {
        locks = new ReadLock[concurrencyLvl];

        for (int i = 0; i < concurrencyLvl; i++)
            locks[i] = new ReadLock();

        writeLock = new WriteLock();
    }

    /** {@inheritDoc} */
    @NotNull @Override public Lock readLock() {
        int idx;

        if (Thread.currentThread() instanceof IgniteThread) {
            IgniteThread igniteThread = (IgniteThread)Thread.currentThread();

            idx = igniteThread.compositeRwLockIndex();

            if (idx == IgniteThread.GRP_IDX_UNASSIGNED) {
                idx = IDX_GEN.incrementAndGet();

                igniteThread.compositeRwLockIndex(idx);
            }
        }
        else
            idx = IDX.get();

        return locks[idx % locks.length].readLock();
    }

    /** {@inheritDoc} */
    @NotNull @Override public Lock writeLock() {
        return writeLock;
    }

    /**
     * Queries if the write lock is held by the current thread.
     *
     * @return {@code true} if the current thread holds the write lock and
     *         {@code false} otherwise
     */
    public boolean isWriteLockedByCurrentThread() {
        return locks[locks.length - 1].isWriteLockedByCurrentThread();
    }

    /**
     * Read lock.
     */
    @SuppressWarnings("unused")
    private static class ReadLock extends ReentrantReadWriteLock {
        /** */
        private static final long serialVersionUID = 0L;

        /** Padding. */
        private long p0, p1, p2, p3, p4, p5, p6, p7;
    }

    /**
     * Write lock.
     */
    private class WriteLock implements Lock {
        /** {@inheritDoc} */
        @Override public void lock() {
            try {
                lock0(false);
            }
            catch (InterruptedException ignore) {
                assert false : "Should never happen";
            }
        }

        /** {@inheritDoc} */
        @Override public void lockInterruptibly() throws InterruptedException {
            lock0(true);
        }

        /** {@inheritDoc} */
        @Override public void unlock() {
            unlock0(locks.length - 1);
        }

        /**
         * Internal lock routine.
         *
         * @param canInterrupt Whether to acquire the lock interruptibly.
         * @throws InterruptedException If interrupted.
         */
        private void lock0(boolean canInterrupt) throws InterruptedException {
            int i = 0;

            try {
                for (; i < locks.length; i++) {
                    if (canInterrupt)
                        locks[i].writeLock().lockInterruptibly();
                    else
                        locks[i].writeLock().lock();
                }
            }
            catch (InterruptedException e) {
                unlock0(i - 1);

                throw e;
            }
        }

        /**
         * Internal unlock routine.
         *
         * @param fromIdx Start index.
         */
        private void unlock0(int fromIdx) {
            for (int i = fromIdx; i >= 0; i--)
                locks[i].writeLock().unlock();
        }

        /** {@inheritDoc} */
        @Override public boolean tryLock() {
            int i = 0;

            try {
                for (; i < locks.length; i++) {
                    if (!locks[i].writeLock().tryLock())
                        break;
                }
            }
            finally {
                if (0 < i && i < locks.length)
                    unlock0(i - 1);
            }

            return i == locks.length;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("NullableProblems")
        @Override public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            int i = 0;

            long end = unit.toNanos(time) + System.nanoTime();

            try {
                for (; i < locks.length && System.nanoTime() < end; i++) {
                    if (!locks[i].writeLock().tryLock(time, unit))
                        break;
                }
            }
            finally {
                if (0 < i && i < locks.length)
                    unlock0(i - 1);
            }

            return i == locks.length;
        }

        /** {@inheritDoc} */
        @NotNull @Override public Condition newCondition() {
            throw new UnsupportedOperationException();
        }
    }
}
