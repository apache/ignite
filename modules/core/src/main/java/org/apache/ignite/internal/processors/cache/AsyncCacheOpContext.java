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

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.jetbrains.annotations.Nullable;

/**
 * Context for managing async cache operations.
 */
@GridToStringExclude
public class AsyncCacheOpContext {
    /** Last asynchronous future. */
    private ThreadLocal<FutureHolder> lastFut = ThreadLocal.withInitial(FutureHolder::new);

    /**
     * Returns last async future holder.
     */
    public FutureHolder lastFuture() {
        return lastFut.get();
    }

    /**
     * Awaits for previous async operation to be completed.
     */
    public void awaitLastFut() {
        FutureHolder holder = lastFut.get();

        IgniteInternalFuture<Void> fut = holder.future();

        if (fut != null && !fut.isDone()) {
            try {
                // Ignore any exception from previous async operation as it should be handled by user.
                fut.get();
            }
            catch (IgniteCheckedException ignored) {
                // No-op.
            }
        }
    }

    /** */
    public void stop() {
        // Nulling thread local reference to ensure values will be eventually GCed
        // no matter what references these futures are holding.
        lastFut = null;
    }

    /**
     * Replaces previous async operation future on transaction suspend.
     */
/*
    public @Nullable FutureHolder suspendLastFut() {
        FutureHolder holder = lastFut.get();

        IgniteInternalFuture<Void> fut = holder.future();

        if (fut != null && !fut.isDone()) {
            lastFut.remove();

            return holder;
        }
        else
            return null;
    }
*/

    /**
     * Replaces previous async operation future on transaction resume.
     */
/*
    public void resumeLastFut(FutureHolder holder) {
        IgniteInternalFuture<Void> resumedFut = holder.future();

        if (resumedFut == null || resumedFut.isDone())
            return;

        FutureHolder threadHolder = lastFut.get();

        IgniteInternalFuture<Void> threadFut = threadHolder.future();

        if (threadFut != null && !threadFut.isDone()) {
            threadHolder.lock();

            try {
                GridCompoundFuture<Void, Void> f = new GridCompoundFuture<>();

                f.add(threadFut);
                f.add(resumedFut);
                f.markInitialized();

                saveFuture(threadHolder, f);
            }
            finally {
                threadHolder.unlock();
            }
        }
        else
            lastFut.set(holder);
    }
*/

    /**
     * Saves future in thread local holder and adds listener
     * that will clear holder when future is finished.
     *
     * @param holder Future holder.
     * @param fut Future to save.
     */
    public void saveFuture(final FutureHolder holder, IgniteInternalFuture<?> fut) {
        assert holder != null;
        assert fut != null;
        assert holder.holdsLock();

        IgniteInternalFuture<Void> fut0 = (IgniteInternalFuture<Void>)fut;

        holder.future(fut0);

        if (fut0.isDone())
            holder.future(null);
        else {
            fut0.listen(new CI1<IgniteInternalFuture<Void>>() {
                @Override public void apply(IgniteInternalFuture<Void> f) {
                    if (!holder.tryLock())
                        return;

                    try {
                        if (holder.future() == f)
                            holder.future(null);
                    }
                    finally {
                        holder.unlock();
                    }
                }
            });
        }
    }

    /**
     * Holder for last async operation future.
     */
    public static class FutureHolder {
        /** Lock. */
        private final ReentrantLock lock = new ReentrantLock();

        /** Future. */
        private IgniteInternalFuture<Void> fut;

        /**
         * Tries to acquire lock.
         *
         * @return Whether lock was actually acquired.
         */
        public boolean tryLock() {
            return lock.tryLock();
        }

        /**
         * Acquires lock.
         */
        @SuppressWarnings("LockAcquiredButNotSafelyReleased")
        public void lock() {
            lock.lock();
        }

        /**
         * Releases lock.
         */
        public void unlock() {
            lock.unlock();
        }

        /**
         * @return Whether lock is held by current thread.
         */
        public boolean holdsLock() {
            return lock.isHeldByCurrentThread();
        }

        /**
         * Gets future.
         *
         * @return Future.
         */
        public IgniteInternalFuture<Void> future() {
            return fut;
        }

        /**
         * Sets future.
         *
         * @param fut Future.
         */
        public void future(@Nullable IgniteInternalFuture<Void> fut) {
            this.fut = fut;
        }
    }
}
