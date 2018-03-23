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

package org.apache.ignite.internal.processors.datastructures;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCondition;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

/** New version of grid cache reentrant lock super class. */
abstract class GridCacheLockEx2 implements IgniteLock, GridCacheRemovable {
    /** {@inheritDoc} */
    @Override public boolean onRemoved() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void needCheckNotRemoved() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void suspend() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void restart(IgniteInternalCache cache) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Condition newCondition() {
        throw new UnsupportedOperationException("IgniteLock does not allow creation of nameless conditions. ");
    }

    /** {@inheritDoc} */
    @Override public IgniteCondition getOrCreateCondition(String name) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean hasWaiters(IgniteCondition cond) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int getWaitQueueLength(IgniteCondition cond) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean isFailoverSafe() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isBroken() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean removed() {
        return false;
    }

    /**
     * Remove all information about one node.
     *
     * @param id Node id.
     */
    abstract void onNodeRemoved(UUID id);

    /**
     * Return release message handler.
     *
     * @return Release message handler.
     */
    abstract IgniteInClosure<GridCacheIdMessage> getReleaser();

    /** Reused latch where await can return an exception in case if the releasing thread failed. */
    static final class Latch {
        /** */
        private final ReentrantLock lock = new ReentrantLock();

        /** */
        private final Condition cond = lock.newCondition();

        /** */
        private int cnt = 0;

        /** The exception will non-null if release is impossible. */
        private IgniteException e;

        /** Release latch. */
        void release() {
            lock.lock();

            try {
                cnt--;

                cond.signal();
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * Sending exception to the waiting thread.
         *
         * @param e The reason why the release is impossible.
         */
        void fail(@Nullable IgniteException e) {
            lock.lock();

            try {
                cnt--;

                this.e = e;

                cond.signal();
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * Waiting for release or failed.
         *
         * @throws IgniteException If release is impossible.
         */
        void awaitUninterruptibly() {
            lock.lock();

            try {
                cnt++;

                if (cnt > 0)
                    cond.awaitUninterruptibly();

                if (e != null)
                    throw e;
            }
            finally {
                e = null;

                lock.unlock();
            }
        }

        /**
         * Waiting for release or failed.
         *
         * @throws IgniteException If release is impossible.
         * @throws InterruptedException If interrupted.
         */
        void await() throws InterruptedException {
            lock.lock();

            try {
                cnt++;

                if (cnt > 0)
                    cond.await();

                if (e != null)
                    throw e;
            }
            finally {
                e = null;

                lock.unlock();
            }
        }

        /**
         * Waiting for release or failed.
         *
         * @param timeout The maximum time to wait.
         * @param unit The time unit of the {@code timeout} argument.
         * @return {@code true} if the release was called and {@code false} if the waiting time elapsed before the count
         *     reached zero.
         * @throws IgniteException If release is impossible.
         * @throws InterruptedException If interrupted.
         */
        boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            assert unit != null;

            lock.lock();

            try {
                boolean flag = true;

                cnt++;

                if (cnt > 0)
                    flag = cond.await(timeout, unit);

                if (flag && e != null)
                    throw e;

                return flag;
            }
            finally {
                e = null;

                lock.unlock();
            }
        }

        /**
         * Queries whether any threads are waiting to acquire this lock. Note that because cancellations may occur at
         * any time, a {@code true} return does not guarantee that any other thread will ever acquire this lock.  This
         * method is designed primarily for use in monitoring of the system state.
         *
         * @return {@code true} if there may be other threads waiting to acquire the lock
         */
        boolean hasQueuedThreads() {
            if (lock.tryLock()) {
                try {
                    return cnt != 0;
                }
                finally {
                    lock.unlock();
                }
            }
            return true;
        }
    }

    /** Thread local counter for reentrant locking. */
    static final class ReentrantCount extends ThreadLocal<Integer> {
        /** {@inheritDoc} */
        @Override protected Integer initialValue() {
            return 0;
        }

        /** Increment thread local count. */
        void increment() {
            int cnt = get();

            assert cnt != Integer.MAX_VALUE : "Maximum lock count exceeded";

            set(cnt + 1);
        }

        /** Decrement thread local count. */
        void decrement() {
            int val = get();

            assert val > 0;

            set(val - 1);
        }
    }
}
