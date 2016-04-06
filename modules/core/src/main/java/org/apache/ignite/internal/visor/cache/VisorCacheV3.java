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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheSwapManager;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.lang.IgnitePair;

/**
 * Data transfer object for {@link IgniteCache}.
 */
public class VisorCacheV3 extends VisorCacheV2 {
    /** */
    private static final long serialVersionUID = 0L;

    /** Primary partitions IDs with offheap and swap entries count. */
    private Collection<GridTuple3<Integer, Long, Long>> primaryPartsOffheapSwap;

    /** Backup partitions IDs with offheap and swap entries count. */
    private Collection<GridTuple3<Integer, Long, Long>> backupPartsOffheapSwap;

    /** {@inheritDoc} */
    @Override public VisorCache from(IgniteEx ignite, String cacheName, int sample) throws IgniteCheckedException {
        VisorCache c = super.from(ignite, cacheName, sample);

        if (c != null && c instanceof VisorCacheV3) {
            VisorCacheV3 cacheV3 = (VisorCacheV3)c;

            GridCacheAdapter ca = ignite.context().cache().internalCache(cacheName);

            // Process only started caches.
            if (ca != null && ca.context().started()) {
                GridCacheSwapManager swap = ca.context().swap();

                cacheV3.primaryPartsOffheapSwap = new ArrayList<>(c.primaryPartitions().size());

                for (IgnitePair<Integer> part: c.primaryPartitions()) {
                    int p = part.get1();

                    cacheV3.primaryPartsOffheapSwap.add(new GridTuple3<>(p, swap.offheapEntriesCount(p), swap.swapEntriesCount(p)));
                }

                cacheV3.backupPartsOffheapSwap = new ArrayList<>(c.backupPartitions().size());

                for (IgnitePair<Integer> part: c.backupPartitions()) {
                    int p = part.get1();

                    cacheV3.backupPartsOffheapSwap.add(new GridTuple3<>(p, swap.offheapEntriesCount(p), swap.swapEntriesCount(p)));
                }
            }
        }

        return c;
    }

    /** {@inheritDoc} */
    @Override protected VisorCache initHistory(VisorCache c) {
        super.initHistory(c);

        if (c instanceof VisorCacheV3) {
            ((VisorCacheV3)c).primaryPartsOffheapSwap = Collections.emptyList();
            ((VisorCacheV3)c).backupPartsOffheapSwap = Collections.emptyList();
        }

        return c;
    }

    /** {@inheritDoc} */
    @Override public VisorCache history() {
        return initHistory(new VisorCacheV3());
    }

    /**
     * @return Collection with primary partitions IDs and offheap and swap entries count.
     */
    public Collection<GridTuple3<Integer, Long, Long>> primaryPartitionsOffheapSwap() {
        return primaryPartsOffheapSwap;
    }

    /**
     * @return Collection with backup partitions IDs and offheap and swap entries count.
     */
    public Collection<GridTuple3<Integer, Long, Long>> backupPartitionsOffheapSwap() {
        return backupPartsOffheapSwap;
    }
}
