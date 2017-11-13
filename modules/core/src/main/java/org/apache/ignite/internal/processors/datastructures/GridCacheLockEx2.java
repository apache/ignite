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

/** New version of grid cache reentrant lock super class. */
public abstract class GridCacheLockEx2 implements IgniteLock, GridCacheRemovable {
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
    @Override public int getHoldCount() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean isHeldByCurrentThread() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean hasQueuedThreads() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean hasQueuedThread(Thread thread) {
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
     * @param id node id.
     */
    abstract void onNodeRemoved(UUID id);

    /**
     * Return release message processer.
     *
     * @return release message processer.
     */
    public abstract IgniteInClosure<GridCacheIdMessage> getReleaser();

    /** */
    protected static class Latch {
        /** */
        private final ReentrantLock lock = new ReentrantLock();

        /** */
        private final Condition condition = lock.newCondition();

        /** */
        private int count = 0;

        /** */
        private IgniteException exception;

        /** */
        public void release() {
            lock.lock();
            try {
                count++;

                condition.signal();
            }
            finally {
                lock.unlock();
            }
        }

        /** */
        public void fail(IgniteException exception) {
            lock.lock();
            try {
                count++;
                this.exception = exception;

                condition.signal();
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * @throws IgniteException */
        public void await() throws InterruptedException {
            lock.lock();
            try {
                if (count-- <= 0) {
                    condition.await();
                }
                if (exception != null) {
                    throw exception;
                }
            }
            finally {
                exception = null;
                lock.unlock();
            }
        }

        /**
         * @throws IgniteException */
        public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            lock.lock();
            try {
                boolean flag = true;
                if (count-- <= 0) {
                    flag = condition.await(timeout, unit);
                }

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
}
