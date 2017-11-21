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
    @Override public Condition newCondition() {
        throw new UnsupportedOperationException("IgniteLock does not allow creation of nameless conditions. ");
    }

    /** {@inheritDoc} */
    @Override public IgniteCondition getOrCreateCondition(String name) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean hasWaiters(IgniteCondition condition) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int getWaitQueueLength(IgniteCondition condition) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean hasQueuedThread(Thread thread) throws IgniteException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean hasQueuedThreads() throws IgniteException {
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
    static class Latch {
        /** */
        private final ReentrantLock lock = new ReentrantLock();

        /** */
        private final Condition condition = lock.newCondition();

        /** */
        private int count = 0;

        /** The exception will non-null if release is impossible. */
        private IgniteException exception;

        /** Release latch. */
        void release() {
            lock.lock();
            try {
                count--;

                condition.signal();
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * Sending exception to the waiting thread.
         *
         * @param exception The reason why the release is impossible.
         */
        void fail(@Nullable IgniteException exception) {
            lock.lock();
            try {
                count--;

                this.exception = exception;

                condition.signal();
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * Waiting for release or faild.
         *
         * @throws IgniteException If release is impossible.
         */
        void awaitUninterruptibly() {
            lock.lock();
            try {
                count++;

                if (count > 0)
                    condition.awaitUninterruptibly();

                if (exception != null)
                    throw exception;
            }
            finally {
                exception = null;
                lock.unlock();
            }
        }

        /**
         * Waiting for release or faild.
         *
         * @throws IgniteException If release is impossible.
         * @throws InterruptedException If interrupted.
         */
        void await() throws InterruptedException {
            lock.lock();
            try {
                count++;
                if (count > 0)
                    condition.await();

                if (exception != null)
                    throw exception;
            }
            finally {
                exception = null;
                lock.unlock();
            }
        }

        /**
         * Waiting for release or faild.
         *
         * @param timeout The maximum time to wait.
         * @param unit The time unit of the {@code timeout} argument.
         * @return {@code true} if the release was called and {@code false} if the waiting time elapsed before the count
         * reached zero.
         * @throws IgniteException If release is impossible.
         * @throws InterruptedException If interrupted.
         */
        boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            assert unit != null;

            lock.lock();
            try {
                boolean flag = true;

                count++;

                if (count > 0)
                    flag = condition.await(timeout, unit);

                if (flag && exception != null)
                    throw exception;

                return flag;
            }
            finally {
                exception = null;
                lock.unlock();
            }
        }
    }

    /** Thread local counter for reentrant locking. */
    static class ReentrantCount extends ThreadLocal<Integer> {
        /** {@inheritDoc} */
        @Override protected Integer initialValue() {
            return 0;
        }

        /** Increment thread local count. */
        void increment() {
            int count = get();

            assert count != Integer.MAX_VALUE : "Maximum lock count exceeded";

            set(count + 1);
        }

        /** Decrement thread local count. */
        void decrement() {
            int val = get();

            assert val > 0;

            set(val - 1);
        }
    }
}
