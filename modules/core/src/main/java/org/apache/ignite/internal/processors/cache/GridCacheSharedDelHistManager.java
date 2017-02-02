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

import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Periodically removes expired entities from delete history queues
 */
public class GridCacheSharedDelHistManager extends GridCacheSharedManagerAdapter {

    /** Ttl cleanup worker thread sleep interval, ms. */
    private static final long CLEANUP_WORKER_SLEEP_INTERVAL = 500;

    /** Limit of expired entries processed by worker for certain cache in one pass. */
    private static final int CLEANUP_WORKER_ENTRIES_PROCESS_LIMIT = 1000;

    /** Cleanup worker. */
    private GridCacheSharedDelHistManager.CleanupWorker cleanupWorker;

    /** List of registered removed queues. */
    private List<T2<GridCacheContext, BlockingQueue<T3<KeyCacheObject, GridCacheVersion, Long>>>> delList = new
            CopyOnWriteArrayList<>();

    /** {@inheritDoc} */
    @Override protected synchronized void onKernalStop0(boolean cancel) {
        stopCleanupWorker();
    }

    /**
     * Register queue of removed items for periodical check on expired entries.
     *
     * @param queue queue with removed items.
     * @param cctx current cache context.
     * */
    public synchronized void register(BlockingQueue<T3<KeyCacheObject, GridCacheVersion, Long>> queue, GridCacheContext cctx) {
        if (cleanupWorker == null)
            startCleanupWorker();

        delList.add(new T2<>(cctx, queue));
    }

    /**
     *
     */
    private void startCleanupWorker() {
        cleanupWorker = new GridCacheSharedDelHistManager.CleanupWorker();

        new IgniteThread(cleanupWorker).start();
    }

    /**
     *
     */
    private void stopCleanupWorker() {
        if (null != cleanupWorker) {
            U.cancel(cleanupWorker);
            U.join(cleanupWorker, log);

            cleanupWorker = null;
        }
    }

    /**
     * Entry cleanup worker.
     */
    private class CleanupWorker extends GridWorker {
        /**
         * Creates cleanup worker.
         */
        CleanupWorker() {
            super(cctx.gridName(), "cache-hist-remove-cleanup-worker", cctx.logger(GridCacheSharedDelHistManager
                    .class));
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            while (!isCancelled()) {
                int deleteTotal = 0;

                for (T2<GridCacheContext, BlockingQueue<T3<KeyCacheObject, GridCacheVersion, Long>>> pair : delList) {
                    Iterator<T3<KeyCacheObject, GridCacheVersion, Long>> it = pair.getValue().iterator();
                    T3<KeyCacheObject, GridCacheVersion, Long> item;
                    long currTime = U.currentTimeMillis();
                    deleteTotal = 0;
                    while (it.hasNext()) {
                        item = it.next();
                        if (item.get3() < currTime) {
                            pair.getKey().dht().removeVersionedEntry(item.get1(), item.get2());
                            it.remove();
                            if (deleteTotal++ > CLEANUP_WORKER_ENTRIES_PROCESS_LIMIT)
                                break;
                        } else
                            break;
                    }

                    if (isCancelled())
                        return;
                }

                if (deleteTotal == 0)
                    U.sleep(CLEANUP_WORKER_SLEEP_INTERVAL);
            }
        }
    }
}
