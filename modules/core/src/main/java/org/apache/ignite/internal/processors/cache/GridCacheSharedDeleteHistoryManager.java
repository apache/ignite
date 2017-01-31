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

import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jsr166.ConcurrentLinkedDeque8;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CACHE_RM_ITEM_TTL;

/**
 * Periodically removes expired entities from delete history queues
 */
public class GridCacheSharedDeleteHistoryManager extends GridCacheSharedManagerAdapter {

    /** Ttl cleanup worker thread sleep interval, ms. */
    private static final long CLEANUP_WORKER_SLEEP_INTERVAL = 500;

    /** Ttl of removed cache item (ms). */
    public static final int CACHE_RM_ITEM_TTL = Integer.getInteger(IGNITE_CACHE_RM_ITEM_TTL, 10_000);

    /** List of registered delete queues. */
    private List<T2<ConcurrentLinkedDeque8<GridDhtLocalPartition.GridDhtLocalKeyCacheItemHolder>, GridCacheContext>>
            delList = new CopyOnWriteArrayList<>();

    /** Mutex on worker thread creation. */
    private final Object mux = new Object();

    /** Cleanup worker. */
    private GridTimeoutProcessor.CancelableTask cleanupWorkerTask;

    /** {@inheritDoc} */
    @Override protected synchronized void onKernalStop0(boolean cancel) {
        stopCleanupWorker();
    }

    /**
     * Register queue of deleted items for periodical check on expired entries.
     *
     * @param rmvQueue queue of deffered delete items.
     * */
    public void register(
            ConcurrentLinkedDeque8<GridDhtLocalPartition.GridDhtLocalKeyCacheItemHolder> rmvQueue,
            GridCacheContext cctx) {

        synchronized (mux) {
            if (cleanupWorkerTask == null)
                startCleanupWorker();
        }

        delList.add(new T2<>(rmvQueue, cctx));
    }

    /**
     * Start clean worker
     */
    private void startCleanupWorker() {
        cleanupWorkerTask = cctx.kernalContext().timeout().schedule(new CleanupWorker(), CACHE_RM_ITEM_TTL,
                CLEANUP_WORKER_SLEEP_INTERVAL);
    }

    /**
     * Stop cleanup worker
     */
    private void stopCleanupWorker() {
        U.closeQuiet(cleanupWorkerTask);
    }

    /**
     * Entry cleanup worker.
     */
    private class CleanupWorker implements Runnable {

        @Override public void run() {
            for (T2<ConcurrentLinkedDeque8<GridDhtLocalPartition.GridDhtLocalKeyCacheItemHolder>,
                    GridCacheContext> item : delList) {

                final ConcurrentLinkedDeque8<GridDhtLocalPartition.GridDhtLocalKeyCacheItemHolder> rmvQueue = item
                        .getKey();
                final GridCacheContext cacheContext = item.getValue();

                cacheContext.closures().runLocalSafe(new Runnable() {
                    @Override
                    public void run() {
                        /*GridDhtLocalPartition.GridDhtLocalKeyCacheItemHolder item = rmvQueue.peekFirst();

                        while (item != null && item.expireTime() < U.currentTimeMillis()) {
                            item = rmvQueue.pollFirst();
                            if (item != null)
                                cacheContext.dht().removeVersionedEntry(item.keyCacheObject(), item.cacheVersion());
                        }*/

                        Iterator<GridDhtLocalPartition.GridDhtLocalKeyCacheItemHolder> it = rmvQueue.iterator();
                        while (it.hasNext())
                        {
                            GridDhtLocalPartition.GridDhtLocalKeyCacheItemHolder item = it.next();
                            if (item != null) {
                                cacheContext.dht().removeVersionedEntry(item.keyCacheObject(), item.cacheVersion());
                                it.remove();
                            }
                        }
                    }
                }, true);
            }
        }
    }

}

