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

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * ReadWriteLock with striping mechanics.
 * Compared to {@link ReentrantReadWriteLock} it has slightly improved performance of {@link ReadWriteLock#readLock()}
 * operations at the cost of {@link ReadWriteLock#writeLock()} operations and memory consumption.
 * It also supports reentrancy semantics like {@link ReentrantReadWriteLock}.
 */
public class StripedCompositeReadWriteLock implements ReadWriteLock {

    /**
     * Thread local index generator.
     */
    private static final ThreadLocal<Integer> IDX = new ThreadLocal<Integer>() {
        @Override protected Integer initialValue() {
            return ThreadLocalRandom.current().nextInt(100000);
        }
    };

    /**
     * Locks.
     */
    private final ReentrantReadWriteLock[] locks;

    /**
     * Composite write lock.
     */
    private final CompositeWriteLock compositeWriteLock;

    /**
     * Creates a new instance with given concurrency level.
     *
     * @param concurrencyLvl Number of internal read locks.
     */
    public StripedCompositeReadWriteLock(int concurrencyLvl) {
        locks = new PaddedReentrantReadWriteLock[concurrencyLvl];

        for (int i = 0; i < concurrencyLvl; i++)
            locks[i] = new PaddedReentrantReadWriteLock();

        compositeWriteLock = new CompositeWriteLock();
    }

    /** {@inheritDoc} */
    @NotNull @Override public Lock readLock() {
        int idx = IDX.get() % locks.length;

        return locks[idx].readLock();
    }

    /** {@inheritDoc} */
    @NotNull @Override public Lock writeLock() {
        return compositeWriteLock;
    }

    /**
     * {@inheritDoc}
     *
     * Compared to {@link ReentrantReadWriteLock}, this class contains padding to ensure that different instances will
     * always be located in different CPU cache lines.
     */
    private static class PaddedReentrantReadWriteLock extends ReentrantReadWriteLock {

        /**
         *
         */
        private static final long serialVersionUID = 0L;

        /**
         * Padding.
         */
        private long p0, p1, p2, p3, p4, p5, p6, p7;
    }

    /**
     * {@inheritDoc}
     *
     * Methods of this class will lock all {@link #locks}.
     */
    private class CompositeWriteLock implements Lock {

        /** {@inheritDoc} */
        @Override public void lock() {
            try {
                lock(false);
            }
            catch (InterruptedException e) {
                // This should never happen.
                throw new RuntimeException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void lockInterruptibly() throws InterruptedException {
            lock(true);
        }

        /**
         * @param interruptibly true if {@link Thread#interrupt()} should be considered.
         * @throws InterruptedException
         */
        private void lock(boolean interruptibly) throws InterruptedException {
            int i = 0;
            try {
                for (; i < locks.length; i++)
                    if (interruptibly)
                        locks[i].writeLock().lockInterruptibly();
                    else
                        locks[i].writeLock().lock();
            }
            catch (Throwable e) {
                for (i--; i >= 0; i--)
                    locks[i].writeLock().unlock();

                throw e;
            }
        }

        /** {@inheritDoc} */
        @Override public boolean tryLock() {
            int i = 0;

            boolean unlock = false;

            try {
                for (; i < locks.length; i++)
                    if (!locks[i].writeLock().tryLock()) {
                        unlock = true;
                        break;
                    }
            }
            catch (Throwable e) {
                for (i--; i >= 0; i--)
                    locks[i].writeLock().unlock();

                throw e;
            }

            if (unlock) {
                for (i--; i >= 0; i--)
                    locks[i].writeLock().unlock();

                return false;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            long timeLeft = unit.convert(time, TimeUnit.NANOSECONDS);

            long prevTime = System.nanoTime();

            int i = 0;

            boolean unlock = false;

            try {
                for (; i < locks.length; i++) {
                    if (timeLeft < 0 || !locks[i].writeLock().tryLock(timeLeft, TimeUnit.NANOSECONDS)) {
                        unlock = true;
                        break;
                    }

                    long currentTime = System.nanoTime();
                    timeLeft -= (currentTime - prevTime);
                    prevTime = currentTime;
                }
            }
            catch (Throwable e) {
                for (i--; i >= 0; i--)
                    locks[i].writeLock().unlock();

                throw e;
            }

            if (unlock) {
                for (i--; i >= 0; i--)
                    locks[i].writeLock().unlock();

                return false;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public void unlock() {
            for (int i = locks.length - 1; i >= 0; i--)
                locks[i].writeLock().unlock();
        }

        /** {@inheritDoc} */
        @NotNull @Override public Condition newCondition() {
            throw new UnsupportedOperationException();
        }
    }
}
