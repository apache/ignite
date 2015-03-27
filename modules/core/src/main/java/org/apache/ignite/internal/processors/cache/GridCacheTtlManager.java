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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.util.worker.*;
import org.apache.ignite.thread.*;

import java.util.*;

/**
 * Eagerly removes expired entries from cache when {@link org.apache.ignite.configuration.CacheConfiguration#isEagerTtl()} flag is set.
 */
@SuppressWarnings("NakedNotify")
public class GridCacheTtlManager extends GridCacheManagerAdapter {
    /** Entries pending removal. */
    private final GridConcurrentSkipListSet<EntryWrapper> pendingEntries = new GridConcurrentSkipListSet<>();

    /** Cleanup worker thread. */
    private CleanupWorker cleanupWorker;

    /** Sync mutex. */
    private final Object mux = new Object();

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        if (cctx.kernalContext().isDaemon() || !cctx.config().isEagerTtl())
            return;

        cleanupWorker = new CleanupWorker();
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
     * Adds tracked entry to ttl processor.
     *
     * @param entry Entry to add.
     */
    public void addTrackedEntry(GridCacheMapEntry entry) {
        EntryWrapper wrapper = new EntryWrapper(entry);

        pendingEntries.add(wrapper);

        // If entry is on the first position, notify waiting thread.
        if (wrapper == pendingEntries.firstx()) {
            synchronized (mux) {
                mux.notifyAll();
            }
        }
    }

    /**
     * @param entry Entry to remove.
     */
    public void removeTrackedEntry(GridCacheMapEntry entry) {
        // Remove must be called while holding lock on entry before updating expire time.
        // No need to wake up waiting thread in this case.
        pendingEntries.remove(new EntryWrapper(entry));
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> TTL processor memory stats [grid=" + cctx.gridName() + ", cache=" + cctx.name() + ']');
        X.println(">>>   pendingEntriesSize: " + pendingEntries.size());
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
                long now = U.currentTimeMillis();

                GridCacheVersion obsoleteVer = null;

                for (Iterator<EntryWrapper> it = pendingEntries.iterator(); it.hasNext(); ) {
                    EntryWrapper wrapper = it.next();

                    if (wrapper.expireTime <= now) {
                        if (log.isDebugEnabled())
                            log.debug("Trying to remove expired entry from cache: " + wrapper);

                        if (obsoleteVer == null)
                            obsoleteVer = cctx.versions().next();

                        if (wrapper.entry.onTtlExpired(obsoleteVer))
                            wrapper.entry.context().cache().removeEntry(wrapper.entry);

                        if (wrapper.entry.context().cache().configuration().isStatisticsEnabled())
                            wrapper.entry.context().cache().metrics0().onEvict();

                        it.remove();
                    }
                    else
                        break;
                }

                synchronized (mux) {
                    while (true) {
                        // Access of the first element must be inside of
                        // synchronization block, so we don't miss out
                        // on thread notification events sent from
                        // 'addTrackedEntry(..)' method.
                        EntryWrapper first = pendingEntries.firstx();

                        if (first != null) {
                            long waitTime = first.expireTime - U.currentTimeMillis();

                            if (waitTime > 0)
                                mux.wait(waitTime);
                            else
                                break;
                        }
                        else
                            mux.wait(5000);
                    }
                }
            }
        }
    }

    /**
     * Entry wrapper.
     */
    private static class EntryWrapper implements Comparable<EntryWrapper> {
        /** Entry expire time. */
        private final long expireTime;

        /** Entry. */
        private final GridCacheMapEntry entry;

        /**
         * @param entry Cache entry to create wrapper for.
         */
        private EntryWrapper(GridCacheMapEntry entry) {
            expireTime = entry.expireTimeUnlocked();

            assert expireTime != 0;

            this.entry = entry;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(EntryWrapper o) {
            int res = Long.compare(expireTime, o.expireTime);

            if (res == 0)
                res = Long.compare(entry.startVersion(), o.entry.startVersion());

            return res;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof EntryWrapper))
                return false;

            EntryWrapper that = (EntryWrapper)o;

            return expireTime == that.expireTime && entry.startVersion() == that.entry.startVersion();

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = (int)(expireTime ^ (expireTime >>> 32));

            res = 31 * res + (int)(entry.startVersion() ^ (entry.startVersion() >>> 32));

            return res;
        }
    }
}
