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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Data transfer object for {@link IgniteCache}.
 */
public class VisorCache implements Serializable {
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
    private int size;

    /** Number of all entries in near cache. */
    private int nearSize;

    /** Number of all entries in DHT cache. */
    private int dhtSize;

    /** Number of primary entries in cache. */
    private int primarySize;

    /** Memory size allocated in off-heap. */
    private long offHeapAllocatedSize;

    /** Number of cache entries stored in off-heap memory. */
    private long offHeapEntriesCnt;

    /** Size in bytes for swap space. */
    private long swapSize;

    /** Number of cache entries stored in swap space. */
    private long swapKeys;

    /** Number of partitions. */
    private int partitions;

    /** Primary partitions IDs with sizes. */
    private Collection<IgnitePair<Integer>> primaryPartitions;

    /** Backup partitions IDs with sizes. */
    private Collection<IgnitePair<Integer>> backupPartitions;

    /** Cache metrics. */
    private VisorCacheMetrics metrics;

    /** Cache partitions states. */
    private GridDhtPartitionMap partitionsMap;

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

        name = cacheName;

        try {
            swapSize = ca.swapSize();
            swapKeys = ca.swapKeys();
        }
        catch (IgniteCheckedException ignored) {
            swapSize = -1;
            swapKeys = -1;
        }

        primaryPartitions = Collections.emptyList();
        backupPartitions = Collections.emptyList();

        CacheConfiguration cfg = ca.configuration();

        mode = cfg.getCacheMode();

        boolean partitioned = (mode == CacheMode.PARTITIONED || mode == CacheMode.REPLICATED)
            && ca.context().affinityNode();

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

                List<GridDhtLocalPartition> parts = top.localPartitions();

                primaryPartitions = new ArrayList<>(parts.size());
                backupPartitions = new ArrayList<>(parts.size());

                for (GridDhtLocalPartition part : parts) {
                    int p = part.id();

                    int sz = part.size();

                    // Pass -1 as topology version in order not to wait for topology version.
                    if (part.primary(AffinityTopologyVersion.NONE))
                        primaryPartitions.add(new IgnitePair<>(p, sz));
                    else
                        backupPartitions.add(new IgnitePair<>(p, sz));
                }
            }
            else {
                // Old way of collecting partitions info.
                ClusterNode node = ignite.cluster().localNode();

                int[] pp = ca.affinity().primaryPartitions(node);

                primaryPartitions= new ArrayList<>(pp.length);

                for (int p : pp) {
                    Set set = ca.entrySet(p);

                    primaryPartitions.add(new IgnitePair<>(p, set != null ? set.size() : 0));
                }

                int[] bp = ca.affinity().backupPartitions(node);

                backupPartitions = new ArrayList<>(bp.length);

                for (int p : bp) {
                    Set set = ca.entrySet(p);

                    backupPartitions.add(new IgnitePair<>(p, set != null ? set.size() : 0));
                }
            }
        }

        size = ca.size();
        nearSize = ca.nearSize();

        dynamicDeploymentId = ca.context().dynamicDeploymentId();
        dhtSize = size - nearSize;
        primarySize = ca.primarySize();
        offHeapAllocatedSize = ca.offHeapAllocatedSize();
        offHeapEntriesCnt = ca.offHeapEntriesCount();
        partitions = ca.affinity().partitions();
        metrics = VisorCacheMetrics.from(ignite, cacheName);

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
        int size = ca.size();

        Set<GridCacheEntryEx> set = ca.map().entries0();

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
        c.dhtSize = dhtSize;
        c.primarySize = primarySize;
        c.offHeapAllocatedSize = offHeapAllocatedSize;
        c.offHeapEntriesCnt = offHeapEntriesCnt;
        c.swapSize = swapSize;
        c.swapKeys = swapKeys;
        c.partitions = partitions;
        c.primaryPartitions = Collections.emptyList();
        c.backupPartitions = Collections.emptyList();
        c.metrics = metrics;

        return c;
    }

    /**
     * @return Cache name.
     */
    public String name() {
        return name;
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
    public int size() {
        return size;
    }

    /**
     * @return Number of all entries in near cache.
     */
    public int nearSize() {
        return nearSize;
    }

    /**
     * @return Number of all entries in DHT cache.
     */
    public int dhtSize() {
        return dhtSize;
    }

    /**
     * @return Number of primary entries in cache.
     */
    public int primarySize() {
        return primarySize;
    }

    /**
     * @return Memory size allocated in off-heap.
     */
    public long offHeapAllocatedSize() {
        return offHeapAllocatedSize;
    }

    /**
     * @return Number of cache entries stored in off-heap memory.
     */
    public long offHeapEntriesCount() {
        return offHeapEntriesCnt;
    }

    /**
     * @return Size in bytes for swap space.
     */
    public long swapSize() {
        return swapSize;
    }

    /**
     * @return Number of cache entries stored in swap space.
     */
    public long swapKeys() {
        return swapKeys;
    }

    /**
     * @return Number of partitions.
     */
    public int partitions() {
        return partitions;
    }

    /**
     * @return Primary partitions IDs with sizes.
     */
    public Collection<IgnitePair<Integer>> primaryPartitions() {
        return primaryPartitions;
    }

    /**
     * @return Backup partitions IDs with sizes.
     */
    public Collection<IgnitePair<Integer>> backupPartitions() {
        return backupPartitions;
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
    @Nullable public GridDhtPartitionMap partitionMap() {
        return partitionsMap;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCache.class, this);
    }
}