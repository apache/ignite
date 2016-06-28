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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;

/**
 * Eagerly removes expired entries from cache when
 * {@link CacheConfiguration#isEagerTtl()} flag is set.
 */
@SuppressWarnings("NakedNotify")
public class GridCacheTtlManager extends GridCacheManagerAdapter {
    /** Entries pending removal. */
    private IgniteCacheOffheapManager.PendingEntries pentries;

    /** Cleanup worker thread. */
    private CleanupWorker cleanupWorker;

    /** */
    private boolean cleanupEnabled;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        boolean cleanupDisabled = cctx.kernalContext().isDaemon() ||
            !cctx.config().isEagerTtl() ||
            CU.isAtomicsCache(cctx.name()) ||
            CU.isMarshallerCache(cctx.name()) ||
            CU.isUtilityCache(cctx.name()) ||
            (!cctx.affinityNode() && cctx.config().getNearConfiguration() == null);

        if (cleanupDisabled)
            return;

        cleanupEnabled = true;

        // TODO GG-11133 for near cache use existing collection.
        pentries = cctx.offheap().createPendingEntries();

        cleanupWorker = new CleanupWorker();
    }

    /**
     * @return {@code True} If eager cleanup enabled.
     */
    public boolean eagerCleanupEnabled() {
        return cleanupEnabled;
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        if (cleanupWorker != null)
            new IgniteThread(cleanupWorker).start();
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        U.cancel(cleanupWorker);
        U.join(cleanupWorker, log);
    }

    /**
     * @return The size of pending entries.
     */
    public int pendingSize() {
        return pentries.pendingSize();
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> TTL processor memory stats [grid=" + cctx.gridName() + ", cache=" + cctx.name() + ']');
        X.println(">>>   pendingEntriesSize: " + pentries.pendingSize());
    }

    /**
     * Expires entries by TTL.
     */
    public void expire() {
        if (!cleanupEnabled)
            return;

        long now = U.currentTimeMillis();

        GridCacheVersion obsoleteVer = null;

        try {
            while (true) {
                // TODO GG-11133 skip cursor creation if there are no pending entries (can maintain and check size).
                IgniteCacheOffheapManager.ExpiredEntriesCursor expiredEntriesCursor = pentries.expired(now);

                // TODO GG-11231 (do not use 'while (cursor.next()) as workaround for GG-11231).
                if (expiredEntriesCursor.next()) {
                    GridCacheEntryEx entry = expiredEntriesCursor.get();

                    if (obsoleteVer == null)
                        obsoleteVer = cctx.versions().next();

                    if (log.isTraceEnabled())
                        log.trace("Trying to remove expired entry from cache: " + entry);

                    boolean touch = true;

                    while (true) {
                        try {
                            if (entry.onTtlExpired(obsoleteVer))
                                touch = false;

                            break;
                        }
                        catch (GridCacheEntryRemovedException e0) {
                            entry = entry.context().cache().entryEx(entry.key());
                        }
                    }

                    if (touch)
                        entry.context().evicts().touch(entry, null);
                }
                else
                    break;
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to process expired entries: " + e, e);
        }
    }

    /**
     * Entry cleanup worker.
     */
    private class CleanupWorker extends GridWorker {
        /**
         * Creates cleanup worker.
         */
        protected CleanupWorker() {
            super(cctx.gridName(), "ttl-cleanup-worker-" + cctx.name(), cctx.logger(GridCacheTtlManager.class));
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            while (!isCancelled()) {
                expire();

                long time = pentries.firstExpired();

                if (time > 0) {
                    long waitTime = time - U.currentTimeMillis();

                    if (waitTime > 0)
                        U.sleep(waitTime);
                }
                else
                    U.sleep(500);
            }
        }
    }
}