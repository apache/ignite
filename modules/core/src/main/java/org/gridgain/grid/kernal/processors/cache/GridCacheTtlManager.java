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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.thread.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.worker.*;

import java.util.*;

/**
 * Eagerly removes expired entries from cache when {@link CacheConfiguration#isEagerTtl()} flag is set.
 */
@SuppressWarnings("NakedNotify")
public class GridCacheTtlManager<K, V> extends GridCacheManagerAdapter<K, V> {
    /** Entries pending removal. */
    private final GridConcurrentSkipListSet<EntryWrapper<K, V>> pendingEntries = new GridConcurrentSkipListSet<>();

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
    public void addTrackedEntry(GridCacheMapEntry<K, V> entry) {
        EntryWrapper<K, V> wrapper = new EntryWrapper<>(entry);

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
    public void removeTrackedEntry(GridCacheMapEntry<K, V> entry) {
        // Remove must be called while holding lock on entry before updating expire time.
        // No need to wake up waiting thread in this case.
        pendingEntries.remove(new EntryWrapper<>(entry));
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
        @Override protected void body() throws InterruptedException, GridInterruptedException {
            while (!isCancelled()) {
                long now = U.currentTimeMillis();

                GridCacheVersion obsoleteVer = null;

                for (Iterator<EntryWrapper<K, V>> it = pendingEntries.iterator(); it.hasNext(); ) {
                    EntryWrapper<K, V> wrapper = it.next();

                    if (wrapper.expireTime <= now) {
                        if (log.isDebugEnabled())
                            log.debug("Trying to remove expired entry from cache: " + wrapper);

                        if (obsoleteVer == null)
                            obsoleteVer = cctx.versions().next();

                        if (wrapper.entry.onTtlExpired(obsoleteVer))
                            wrapper.entry.context().cache().removeEntry(wrapper.entry);

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
                        EntryWrapper<K, V> first = pendingEntries.firstx();

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
    private static class EntryWrapper<K, V> implements Comparable<EntryWrapper<K, V>> {
        /** Entry expire time. */
        private final long expireTime;

        /** Entry. */
        private final GridCacheMapEntry<K, V> entry;

        /**
         * @param entry Cache entry to create wrapper for.
         */
        private EntryWrapper(GridCacheMapEntry<K, V> entry) {
            expireTime = entry.expireTimeUnlocked();

            assert expireTime != 0;

            this.entry = entry;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(EntryWrapper<K, V> o) {
            if (expireTime == o.expireTime) {
                if (entry.startVersion() == o.entry.startVersion())
                    return 0;

                return entry.startVersion() < o.entry.startVersion() ? -1 : 1;
            }
            else
                return expireTime < o.expireTime ? -1 : 1;
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
