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

import java.io.Serializable;
import java.util.Iterator;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.LessNamingBean;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap2;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Data transfer object for {@link IgniteCache}.
 */
public class VisorCache implements Serializable, LessNamingBean {
    /** */
    private static final CachePeekMode[] PEEK_NO_NEAR =
        new CachePeekMode[] {CachePeekMode.PRIMARY, CachePeekMode.BACKUP};

    /** */
    private static final long serialVersionUID = 0L;

    /** Default cache size sampling. */
    private static final int DFLT_CACHE_SIZE_SAMPLING = 10;

    /** Cache name. */
    private String name;

    /** Cache deployment ID. */
    private IgniteUuid dynamicDeploymentId;

    /** Cache mode. */
    private CacheMode mode;

    /** Cache size in bytes. */
    private long memorySize;

    /** Cache size in bytes. */
    private long indexesSize;

    /** Number of all entries in cache. */
    private long size;

    /** Number of all entries in near cache. */
    private int nearSize;

    /** Number of primary entries in cache. */
    private long primarySize;

    /** Number of backup entries in cache. */
    private long backupSize;

    /** Number of cache entries stored in heap memory. */
    private long onHeapEntriesCnt;

    /** Number of partitions. */
    private int partitions;

    /** Cache metrics. */
    private VisorCacheMetrics metrics;

    /** Cache partitions states. */
    private GridDhtPartitionMap2 partitionsMap;

    /** Flag indicating that cache has near cache. */
    private boolean near;

    /**
     * @param ignite Grid.
     * @param cacheName Cache name.
     * @param sample Sample size.
     * @return Data transfer object for given cache.
     * @throws IgniteCheckedException If failed to create data transfer object.
     */
    public VisorCache from(IgniteEx ignite, String cacheName, int sample) throws IgniteCheckedException {
        assert ignite != null;

        GridCacheAdapter ca = ignite.context().cache().internalCache(cacheName);

        // Cache was not started.
        if (ca == null || !ca.context().started())
            return null;

        GridCacheContext cctx = ca.context();

        name = cacheName;

        CacheConfiguration cfg = ca.configuration();

        mode = cfg.getCacheMode();

        boolean partitioned = (mode == CacheMode.PARTITIONED || mode == CacheMode.REPLICATED)
            && cctx.affinityNode();

        if (partitioned) {
            GridDhtCacheAdapter dca = null;

            if (ca instanceof GridNearCacheAdapter)
                dca = ((GridNearCacheAdapter)ca).dht();
            else if (ca instanceof GridDhtCacheAdapter)
                dca = (GridDhtCacheAdapter)ca;

            if (dca != null) {
                GridDhtPartitionTopology top = dca.topology();

                if (cfg.getCacheMode() != CacheMode.LOCAL && cfg.getBackups() > 0)
                    partitionsMap = top.localPartitionMap();
            }
        }

        dynamicDeploymentId = cctx.dynamicDeploymentId();
        size = ca.localSizeLong(PEEK_NO_NEAR);
        primarySize = ca.primarySizeLong();
        backupSize = size - primarySize; // This is backup size.
        nearSize = ca.nearSize();
        onHeapEntriesCnt = 0; // TODO GG-11148 Need to rename on ON-heap entries count, see
        partitions = ca.affinity().partitions();
        metrics = new VisorCacheMetrics().from(ignite, cacheName);
        near = cctx.isNear();

        estimateMemorySize(ignite, ca, sample);

        return this;
    }

    /**
     * Estimate memory size used by cache.
     *
     * @param ignite Ignite.
     * @param ca Cache adapter.
     * @param sample Sample size.
     * @throws IgniteCheckedException If estimation failed.
     */
    protected void estimateMemorySize(IgniteEx ignite, GridCacheAdapter ca, int sample) throws IgniteCheckedException {
        /* TODO Fix after GG-11739 implemented.
        int size = ca.size();

        Iterable<GridCacheEntryEx> set = ca.context().isNear()
            ? ((GridNearCacheAdapter)ca).dht().entries()
            : ca.entries();

        long memSz = 0;

        Iterator<GridCacheEntryEx> it = set.iterator();

        int sz = sample > 0 ? sample : DFLT_CACHE_SIZE_SAMPLING;

        int cnt = 0;

        while (it.hasNext() && cnt < sz) {
            memSz += it.next().memorySize();

            cnt++;
        }

        if (cnt > 0)
            memSz = (long)((double)memSz / cnt * size);

        memorySize = memSz;
        */
        memorySize = 0;
    }

    /**
     * @return New instance suitable to store in history.
     */
    public VisorCache history() {
        VisorCache c = new VisorCache();

        c.name = name;
        c.mode = mode;
        c.memorySize = memorySize;
        c.indexesSize = indexesSize;
        c.size = size;
        c.nearSize = nearSize;
        c.backupSize = backupSize;
        c.primarySize = primarySize;
        c.onHeapEntriesCnt = onHeapEntriesCnt;
        c.partitions = partitions;
        c.metrics = metrics;
        c.near = near;

        return c;
    }

    /**
     * @return Cache name.
     */
    public String name() {
        return name;
    }

    /**
     * Sets new value for cache name.
     *
     * @param name New cache name.
     */
    public void name(String name) {
        this.name = name;
    }

    /**
     * @return Dynamic deployment ID.
     */
    public IgniteUuid dynamicDeploymentId() {
        return dynamicDeploymentId;
    }

    /**
     * @return Cache mode.
     */
    public CacheMode mode() {
        return mode;
    }

    /**
     * @return Cache size in bytes.
     */
    public long memorySize() {
        return memorySize;
    }

    /**
     * @return Indexes size in bytes.
     */
    public long indexesSize() {
        return indexesSize;
    }

    /**
     * @return Number of all entries in cache.
     */
    public long size() {
        return size;
    }

    /**
     * @return Number of all entries in near cache.
     */
    public int nearSize() {
        return nearSize;
    }

    /**
     * @return Number of backup entries in cache.
     */
    public long backupSize() {
        return backupSize;
    }

    /**
     * @return Number of primary entries in cache.
     */
    public long primarySize() {
        return primarySize;
    }

    /**
     * @return Number of cache entries stored in heap memory.
     */
    public long onHeapEntriesCount() {
        return onHeapEntriesCnt;
    }

    /**
     * @return Number of partitions.
     */
    public int partitions() {
        return partitions;
    }

    /**
     * @return Cache metrics.
     */
    public VisorCacheMetrics metrics() {
        return metrics;
    }

    /**
     * @return Cache partitions states.
     */
    @Nullable public GridDhtPartitionMap2 partitionMap() {
        return partitionsMap;
    }

    /**
     * @return {@code true} if cache has near cache.
     */
    public boolean near() {
        return near;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCache.class, this);
    }
}
