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

import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import org.jetbrains.annotations.Nullable;

/**
 * Synchronization primitive allowing concurrent updates and taking consistent snapshots.
 */
public abstract class GridSnapshotLock<X> {
    /** */
    private volatile Sync<X> sync = new Sync<>();

    /**
     * Must be called before update begin.
     */
    public void beginUpdate() {
        Sync<X> sync0;

        while (!(sync0 = sync).tryAcquireForUpdate())
            sync0.awaitResult();
    }

    /**
     * @return {@code true} If it is possible to proceed the update operation.
     */
    public boolean tryBeginUpdate() {
        return sync.tryAcquireForUpdate();
    }

    /**
     * Signal that update operation finished.
     */
    public void endUpdate() {
        Sync<X> sync0 = sync;

        if (sync0.releaseAfterUpdate())
            takeSnapshotAndReplaceSync(sync0);
    }

    /**
     * @return Consistent snapshot of data.
     */
    public X snapshot() {
        Sync<X> sync0 = sync;

        if (sync0.flip())
            takeSnapshotAndReplaceSync(sync0);

        return  sync0.get();
    }

    /**
     * @return Snapshot.
     */
    protected abstract X doSnapshot();

    /**
     * @param sync0 Current sync.
     */
    private void takeSnapshotAndReplaceSync(Sync<X> sync0) {
        try {
            sync0.set(doSnapshot(), null);
        }
        catch (RuntimeException e) {
            sync0.set(null, e);
        }
        finally {
            sync = new Sync();

            sync0.signalAll();
        }
    }

    /**
     * Mix of CountDownLatch, ReadWriteLock and Future. Must be recreated after each {@link #flip()}.
     */
    @SuppressWarnings("PackageVisibleInnerClass")
    private static class Sync<X> extends AbstractQueuedSynchronizer {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private X x;

        /** */
        private RuntimeException e;

        {
            // State here represents number of concurrent updaters + 1. When sync is flipped this value is negated.
            // When state is 0 than value x was set for this sync, its lifecycle is finished and it should be recreated.
            setState(1);
        }

        /**
         * Waits until value will be set and returns it.
         *
         * @return Value which was set using {@link #set(Object, RuntimeException)}.
         */
        public X get() {
            awaitResult();

            if (e != null)
                throw e;

            return x;
        }

        /**
         * Wait until x or e will be set.
         */
        public void awaitResult() {
            acquireShared(1);
        }

        /**
         * Sets value for this sync.
         *
         * @param x Value.
         * @param e Exception.
         */
        public void set(@Nullable X x, @Nullable RuntimeException e) {
            this.x = x;
            this.e = e;

            boolean res = compareAndSetState(-1, 0); // Set finish state.

            assert res;
        }

        /**
         * Tries to acquire this sync for update. Multiple threads allowed to do update at the same time.
         *
         * @return {@code true} If operation succeeded.
         */
        public boolean tryAcquireForUpdate() {
            for (;;) {
                int curr = getState();

                if (curr <= 0) // We are flipped or finished.
                    return false;

                if (compareAndSetState(curr, curr + 1))
                    return true;
            }
        }

        /**
         * Releases this sync after update.
         *
         * @return {@code true} If this sync was flipped and current thread was the last updater.
         */
        public boolean releaseAfterUpdate() {
            for (;;) {
                int curr = getState();

                assert curr != 0;

                int next = curr < 0 ? curr + 1 : curr - 1;

                assert next != 0;

                if (compareAndSetState(curr, next))
                    return next == -1; // -1 means that sync was flipped and we are the last updater.
            }
        }

        /**
         * Flips this sync so that no subsequent updates can happen.
         *
         * @return {@code true} If there are no updaters currently in progress to wait.
         */
        public boolean flip() {
            for (;;) {
                int curr = getState();

                if (curr <= 0)
                    return false;

                if (compareAndSetState(curr, -curr))
                    return curr == 1; // 1 means no active updaters, we should take snapshot ourself.
            }
        }

        /**
         * Wakes up all queued on this sync threads and allows them to proceed.
         */
        public void signalAll() {
            releaseShared(1);
        }

        /** {@inheritDoc} */
        @Override protected final int tryAcquireShared(int ignored) {
            return getState() == 0 ? 1 : -1; // 0 means x already set and we should not block any threads.
        }

        /** {@inheritDoc} */
        @Override protected final boolean tryReleaseShared(int ignored) {
            return true;
        }
    }
}