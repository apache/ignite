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

package org.apache.ignite.internal.visor.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheSwapManager;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Data transfer object for {@link IgniteCache}.
 */
public class VisorCacheV4 extends VisorCacheV2 {
    /** */
    private static final long serialVersionUID = 0L;

    /** Number of primary entries in offheap. */
    private int offHeapPrimaryEntriesCnt;

    /** Number of backup entries in offheap. */
    private int offHeapBackupEntriesCnt;

    /** Number of primary entries in swap. */
    private int swapPrimaryEntriesCnt;

    /** Number of backup entries in swap. */
    private int swapBackupEntriesCnt;

    /** {@inheritDoc} */
    @Override public VisorCache from(IgniteEx ignite, String cacheName, int sample) throws IgniteCheckedException {
        VisorCache c = super.from(ignite, cacheName, sample);

        if (c != null && c instanceof VisorCacheV4) {
            VisorCacheV4 cacheV4 = (VisorCacheV4)c;

            GridCacheAdapter ca = ignite.context().cache().internalCache(cacheName);

            // Process only started caches.
            if (ca != null && ca.context().started()) {
                GridCacheSwapManager swap = ca.context().swap();

                cacheV4.offHeapPrimaryEntriesCnt = swap.offheapEntriesCount(true, false, AffinityTopologyVersion.NONE);
                cacheV4.offHeapBackupEntriesCnt = swap.offheapEntriesCount(false, true, AffinityTopologyVersion.NONE);

                cacheV4.swapPrimaryEntriesCnt = swap.swapEntriesCount(true, false, AffinityTopologyVersion.NONE);
                cacheV4.swapBackupEntriesCnt = swap.swapEntriesCount(false, true, AffinityTopologyVersion.NONE);
            }
        }

        return c;
    }

    /** {@inheritDoc} */
    @Override protected VisorCache initHistory(VisorCache c) {
        super.initHistory(c);

        if (c instanceof VisorCacheV4) {
            VisorCacheV4 cacheV4 = (VisorCacheV4)c;

            cacheV4.offHeapPrimaryEntriesCnt = offHeapPrimaryEntriesCnt;
            cacheV4.offHeapBackupEntriesCnt = offHeapBackupEntriesCnt;
            cacheV4.swapPrimaryEntriesCnt = swapPrimaryEntriesCnt;
            cacheV4.swapBackupEntriesCnt = swapBackupEntriesCnt;
        }

        return c;
    }

    /** {@inheritDoc} */
    @Override public VisorCache history() {
        return initHistory(new VisorCacheV4());
    }

    /**
     * @return Off-heap heap primary entries count.
     */
    public int offHeapPrimaryEntriesCount() {
        return offHeapPrimaryEntriesCnt;
    }

    /**
     * @return Off-heap heap backup entries count.
     */
    public int offHeapBackupEntriesCount() {
        return offHeapBackupEntriesCnt;
    }

    /**
     * @return Swap primary entries count.
     */
    public int swapPrimaryEntriesCount() {
        return swapPrimaryEntriesCnt;
    }

    /**
     * @return Swap backup entries count.
     */
    public int swapBackupEntriesCount() {
        return swapBackupEntriesCnt;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheV4.class, this);
    }
}
