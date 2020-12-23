/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.IgniteUtils.nl;

/** ReentrantReadWriteLock adapter with readLock tracking. */
public class ReentrantReadWriteLockWithTracking implements ReadWriteLock {
    /** Lock hold message. */
    public static final String LOCK_HOLD_MESSAGE = "ReadLock held the lock more than ";

    /** Lock print threshold. */
    private long readLockThreshold;

    /** Delegate instance. */
    private final ReentrantReadWriteLock delegate = new ReentrantReadWriteLock();

    /** Read lock holder. */
    private ReentrantReadWriteLock.ReadLock readLock;

    /** Write lock holder. */
    private ReentrantReadWriteLock.WriteLock writeLock = new ReentrantReadWriteLock.WriteLock(delegate) {};

    /**
     * ReentrantRWLock wrapper, provides additional trace info on {@link ReadLockWithTracking#unlock()} method, if someone
     * holds the lock more than {@code readLockThreshold}.
     *
     * @param log Ignite logger.
     * @param readLockThreshold ReadLock threshold timeout.
     */
    public ReentrantReadWriteLockWithTracking(IgniteLogger log, long readLockThreshold) {
        readLock = new ReadLockWithTracking(delegate, log, readLockThreshold);

        this.readLockThreshold = readLockThreshold;
    }

    /** Delegator implementation. */
    public ReentrantReadWriteLockWithTracking() {
        readLock = new ReentrantReadWriteLock.ReadLock(delegate) {};
    }

    /** {@inheritDoc} */
    @Override public ReentrantReadWriteLock.ReadLock readLock() {
        return readLock;
    }

    /** {@inheritDoc} */
    @Override public ReentrantReadWriteLock.WriteLock writeLock() {
        return writeLock;
    }

    /** */
    public long lockWaitThreshold() {
        return readLockThreshold;
    }

    /**
     * Queries if the write lock is held by the current thread.
     *
     * @return {@code true} if the current thread holds the write lock and
     *         {@code false} otherwise
     */
    public boolean isWriteLockedByCurrentThread() {
        return delegate.isWriteLockedByCurrentThread();
    }

    /**
     * Queries the number of reentrant read holds on this lock by the
     * current thread.  A reader thread has a hold on a lock for
     * each lock action that is not matched by an unlock action.
     *
     * @return the number of holds on the read lock by the current thread,
     *         or zero if the read lock is not held by the current thread
     */
    public int getReadHoldCount() {
        return delegate.getReadHoldCount();
    }

    /**
     * Queries the number of read locks held for this lock. This
     * method is designed for use in monitoring system state, not for
     * synchronization control.
     * @return the number of read locks held
     */
    public int getReadLockCount() {
        return delegate.getReadLockCount();
    }

    /** Tracks long rlock holders. */
    public static class ReadLockWithTracking extends ReentrantReadWriteLock.ReadLock {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private static final ThreadLocal<T2<Integer, Long>> READ_LOCK_HOLDER_TS =
            ThreadLocal.withInitial(() -> new T2<>(0, 0L));

        /** */
        private IgniteLogger log;

        /** */
        private long readLockThreshold;

        /** */
        protected ReadLockWithTracking(ReentrantReadWriteLock lock, @Nullable IgniteLogger log, long readLockThreshold) {
            super(lock);

            this.log = log;

            this.readLockThreshold = readLockThreshold;
        }

        /** */
        private void inc() {
            T2<Integer, Long> val = READ_LOCK_HOLDER_TS.get();

            int cntr = val.get1();

            if (cntr == 0)
                val.set2(U.currentTimeMillis());

            val.set1(++cntr);

            READ_LOCK_HOLDER_TS.set(val);
        }

        /** */
        private void dec() {
            T2<Integer, Long> val = READ_LOCK_HOLDER_TS.get();

            int cntr = val.get1();

            if (--cntr == 0) {
                long timeout = U.currentTimeMillis() - val.get2();

                if (timeout > readLockThreshold) {
                    GridStringBuilder sb = new GridStringBuilder();

                    sb.a(LOCK_HOLD_MESSAGE + timeout + " ms." + nl());

                    U.printStackTrace(Thread.currentThread().getId(), sb);

                    U.warn(log, sb.toString());
                }
            }

            val.set1(cntr);

            READ_LOCK_HOLDER_TS.set(val);
        }

        /** {@inheritDoc} */
        @Override public void lock() {
            super.lock();

            inc();
        }

        /** {@inheritDoc} */
        @Override public void lockInterruptibly() throws InterruptedException {
            super.lockInterruptibly();

            inc();
        }

        /** {@inheritDoc} */
        @Override public boolean tryLock() {
            if (super.tryLock()) {
                inc();

                return true;
            }
            else
                return false;
        }

        /** {@inheritDoc} */
        @Override public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
            if (super.tryLock(timeout, unit)) {
                inc();

                return true;
            }
            else
                return false;
        }

        /** {@inheritDoc} */
        @Override public void unlock() {
            super.unlock();

            dec();
        }
    }
}
