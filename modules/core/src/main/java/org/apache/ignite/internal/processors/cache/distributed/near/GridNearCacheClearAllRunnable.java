/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheClearAllRunnable;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Runnable for {@link IgniteInternalCache#clearLocally(boolean, boolean, boolean)} routine for near cache.
 */
public class GridNearCacheClearAllRunnable<K, V> extends GridCacheClearAllRunnable<K, V> {
    /** Runnable for DHT cache. */
    private final GridCacheClearAllRunnable<K, V> dhtJob;

    /**
     * Constructor.
     *
     * @param cache Cache to be cleared.
     * @param obsoleteVer Obsolete version.
     * @param dhtJob Linked DHT job.
     */
    public GridNearCacheClearAllRunnable(GridCacheAdapter<K, V> cache, GridCacheVersion obsoleteVer,
        GridCacheClearAllRunnable<K, V> dhtJob) {
        super(cache, obsoleteVer, dhtJob.id(), dhtJob.totalCount(), dhtJob.readers());

        this.dhtJob = dhtJob;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        try {
            // Delegate to DHT cache first.
            if (dhtJob != null)
                dhtJob.run();
        }
        finally {
            super.run();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearCacheClearAllRunnable.class, this);
    }
}
