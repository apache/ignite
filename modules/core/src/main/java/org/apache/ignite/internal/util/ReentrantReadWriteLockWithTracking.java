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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.IgniteUtils.nl;

/** */
public class ReentrantReadWriteLockWithTracking implements ReadWriteLock {
    /** Lock hold message. */
    public static final String LOCK_HOLD_MESSAGE = "ReadLock held the lock more than ";

    /** Lock print threshold. */
    private long readLockThreshold;

    /** Delegate instance. */
    private final ReentrantReadWriteLock delegate = new ReentrantReadWriteLock();

    /** Read lock holder. */
    private ReentrantReadWriteLockWithTracking.ReadLock readLock;

    /** Write lock holder. */
    private ReentrantReadWriteLockWithTracking.WriteLock writeLock = new ReentrantReadWriteLockWithTracking.WriteLock(delegate);

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
        readLock = new ReentrantReadWriteLockWithTracking.ReadLock(delegate);
    }

    /** {@inheritDoc} */
    @Override public ReentrantReadWriteLockWithTracking.ReadLock readLock() {
        return readLock;
    }

    /** {@inheritDoc} */
    @Override public ReentrantReadWriteLockWithTracking.WriteLock writeLock() {
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

    /** */
    public static class WriteLock implements Lock {
        /** Delegate instance. */
        private final ReentrantReadWriteLock delegate;

        /** */
        public WriteLock(ReentrantReadWriteLock lock) {
            delegate = lock;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("LockAcquiredButNotSafelyReleased")
        @Override public void lock() {
            delegate.writeLock().lock();
        }

        /** {@inheritDoc} */
        @SuppressWarnings("LockAcquiredButNotSafelyReleased")
        @Override public void lockInterruptibly() throws InterruptedException {
            delegate.writeLock().lockInterruptibly();
        }

        /** {@inheritDoc} */
        @Override public boolean tryLock() {
            return delegate.writeLock().tryLock();
        }

        /** {@inheritDoc} */
        @Override public boolean tryLock(long time, @NotNull TimeUnit unit) throws InterruptedException {
            return delegate.writeLock().tryLock(time, unit);
        }

        /** {@inheritDoc} */
        @Override public void unlock() {
            delegate.writeLock().unlock();
        }

        /** {@inheritDoc} */
        @NotNull @Override public Condition newCondition() {
            return delegate.writeLock().newCondition();
        }

        /**
         * Queries if this write lock is held by the current thread.
         * Identical in effect to {@link
         * ReentrantReadWriteLock#isWriteLockedByCurrentThread}.
         *
         * @return {@code true} if the current thread holds this lock and
         *         {@code false} otherwise
         */
        public boolean isHeldByCurrentThread() {
            return delegate.writeLock().isHeldByCurrentThread();
        }
    }

    /** Tracks long rlock holders. */
    public static class ReadLockWithTracking extends ReadLock {
        /**
         * Delegate instance.
         */
        private final ReentrantReadWriteLock delegate;

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

            delegate = lock;

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
        @SuppressWarnings("LockAcquiredButNotSafelyReleased")
        @Override public void lock() {
            delegate.readLock().lock();

            inc();
        }

        /** {@inheritDoc} */
        @SuppressWarnings("LockAcquiredButNotSafelyReleased")
        @Override public void lockInterruptibly() throws InterruptedException {
            delegate.readLock().lockInterruptibly();

            inc();
        }

        /** {@inheritDoc} */
        @Override public boolean tryLock() {
            if (delegate.readLock().tryLock()) {
                inc();

                return true;
            }
            else
                return false;
        }

        /** {@inheritDoc} */
        @Override public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
            if (delegate.readLock().tryLock(timeout, unit)) {
                inc();

                return true;
            }
            else
                return false;
        }

        /** {@inheritDoc} */
        @Override public void unlock() {
            delegate.readLock().unlock();

            dec();
        }
    }

    /** Default implementation. */
    public static class ReadLock implements Lock {
        /** Delegate instance. */
        private final ReentrantReadWriteLock delegate;

        /** {@inheritDoc} */
        protected ReadLock(ReentrantReadWriteLock lock) {
            delegate = lock;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("LockAcquiredButNotSafelyReleased")
        @Override public void lock() {
            delegate.readLock().lock();
        }

        /** {@inheritDoc} */
        @SuppressWarnings("LockAcquiredButNotSafelyReleased")
        @Override public void lockInterruptibly() throws InterruptedException {
            delegate.readLock().lockInterruptibly();
        }

        /** {@inheritDoc} */
        @Override public boolean tryLock() {
            return delegate.readLock().tryLock();
        }

        /** {@inheritDoc} */
        @Override public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.readLock().tryLock(timeout, unit);
        }

        /** {@inheritDoc} */
        @Override public void unlock() {
            delegate.readLock().unlock();
        }

        /** {@inheritDoc} */
        @Override public Condition newCondition() {
            return delegate.readLock().newCondition();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return delegate.readLock().toString();
        }
    }
}
