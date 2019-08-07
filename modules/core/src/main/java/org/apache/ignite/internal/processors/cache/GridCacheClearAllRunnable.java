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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Base runnable for {@link IgniteInternalCache#clearLocally(boolean, boolean, boolean)} routine.
 */
public class GridCacheClearAllRunnable<K, V> implements Runnable {
    /** Cache to be cleared. */
    protected final GridCacheAdapter<K, V> cache;

    /**  Obsolete version. */
    protected final GridCacheVersion obsoleteVer;

    /** Mod for the given runnable. */
    protected final int id;

    /** Mods count across all spawned clearLocally runnables. */
    protected final int totalCnt;

    /** Whether to clear readers. */
    protected final boolean readers;

    /** Cache context. */
    protected final GridCacheContext<K, V> ctx;

    /** Logger. */
    protected final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param cache Cache to be cleared.
     * @param obsoleteVer Obsolete version.
     * @param id Mod for the given runnable.
     * @param totalCnt Mods count across all spawned clearLocally runnables.
     */
    public GridCacheClearAllRunnable(GridCacheAdapter<K, V> cache, GridCacheVersion obsoleteVer,
        int id, int totalCnt, boolean readers) {
        assert cache != null;
        assert obsoleteVer != null;
        assert id >= 0;
        assert totalCnt > 0;
        assert id < totalCnt;

        this.cache = cache;
        this.obsoleteVer = obsoleteVer;
        this.id = id;
        this.totalCnt = totalCnt;
        this.readers = readers;

        ctx = cache.context();
        log = ctx.logger(getClass());
    }

    /** {@inheritDoc} */
    @Override public void run() {
        for (GridCacheEntryEx gridCacheEntryEx : cache.entries())
            clearEntry(gridCacheEntryEx);

        if (!ctx.isNear()) {
            if (id == 0)
                ctx.offheap().clearCache(ctx, readers);
        }
    }

    /**
     * Clear entry from cache.
     *
     * @param e Entry.
     */
    protected void clearEntry(GridCacheEntryEx e) {
        ctx.shared().database().checkpointReadLock();

        try {
            e.clear(obsoleteVer, readers);
        }
        catch (IgniteCheckedException ex) {
            U.error(log, "Failed to clearLocally entry from cache (will continue to clearLocally other entries): " + e, ex);
        }
        finally {
            ctx.shared().database().checkpointReadUnlock();
        }
    }

    /**
     * Check whether this worker owns particular key.
     *
     * @param key Key.
     * @return {@code True} in case this worker should process this key.
     */
    protected boolean owns(KeyCacheObject key) {
        assert key != null;

        // Avoid hash code and remainder calculation in case there is no actual split.
        return totalCnt == 1 || key.hashCode() % totalCnt == id;
    }

    /**
     * @return ID for the given runnable.
     */
    public int id() {
        return id;
    }

    /**
     * @return Total count across all spawned clearLocally runnables.
     */
    public int totalCount() {
        return totalCnt;
    }

    /**
     * @return Whether to clean readers.
     */
    public boolean readers() {
        return readers;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheClearAllRunnable.class, this);
    }
}
