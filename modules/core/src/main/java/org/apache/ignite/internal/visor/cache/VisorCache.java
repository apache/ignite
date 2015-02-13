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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Data transfer object for {@link IgniteCache}.
 */
public class VisorCache implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default cache size sampling.  */
    private static final int DFLT_CACHE_SIZE_SAMPLING = 10;

    /** Cache name. */
    private String name;

    /** Cache mode. */
    private CacheMode mode;

    /** Cache size in bytes. */
    private long memorySize;

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
    private long offHeapEntriesCount;

    /** Size in bytes for swap space. */
    private long swapSize;

    /** Number of cache entries stored in swap space. */
    private long swapKeys;

    /** Number of partitions. */
    private int partsCnt;

    /** Primary partitions IDs with sizes. */
    private Collection<IgnitePair<Integer>> primaryParts;

    /** Backup partitions IDs with sizes. */
    private Collection<IgnitePair<Integer>> backupParts;

    /** Cache metrics. */
    private VisorCacheMetrics metrics;

    /** Cache partitions states. */
    private GridDhtPartitionMap partsMap;

    /**
     * @param ignite Grid.
     * @param c Actual cache.
     * @param sample Sample size.
     * @return Data transfer object for given cache.
     * @throws IgniteCheckedException
     */
    public static VisorCache from(Ignite ignite, GridCache c, int sample) throws IgniteCheckedException {
        assert ignite != null;
        assert c != null;

        String cacheName = c.name();

        GridCacheAdapter ca = ((IgniteKernal)ignite).internalCache(cacheName);

        long swapSize;
        long swapKeys;

        try {
            swapSize = ca.swapSize();
            swapKeys = ca.swapKeys();
        }
        catch (IgniteCheckedException ignored) {
            swapSize = -1;
            swapKeys = -1;
        }

        Collection<IgnitePair<Integer>> pps = Collections.emptyList();
        Collection<IgnitePair<Integer>> bps = Collections.emptyList();
        GridDhtPartitionMap partsMap = null;

        CacheConfiguration cfg = ca.configuration();

        CacheMode mode = cfg.getCacheMode();

        boolean partitioned = (mode == CacheMode.PARTITIONED || mode == CacheMode.REPLICATED)
            && cfg.getDistributionMode() != CacheDistributionMode.CLIENT_ONLY;

        if (partitioned) {
            GridDhtCacheAdapter dca = null;

            if (ca instanceof GridNearCacheAdapter)
                dca = ((GridNearCacheAdapter)ca).dht();
            else if (ca instanceof GridDhtCacheAdapter)
                dca = (GridDhtCacheAdapter)ca;

            if (dca != null) {
                GridDhtPartitionTopology top = dca.topology();

                if (cfg.getCacheMode() != CacheMode.LOCAL && cfg.getBackups() > 0)
                    partsMap = top.localPartitionMap();

                List<GridDhtLocalPartition> parts = top.localPartitions();

                pps = new ArrayList<>(parts.size());
                bps = new ArrayList<>(parts.size());

                for (GridDhtLocalPartition part : parts) {
                    int p = part.id();

                    int sz = part.size();

                    if (part.primary(-1)) // Pass -1 as topology version in order not to wait for topology version.
                        pps.add(new IgnitePair<>(p, sz));
                    else
                        bps.add(new IgnitePair<>(p, sz));
                }
            }
            else {
                // Old way of collecting partitions info.
                ClusterNode node = ignite.cluster().localNode();

                int[] pp = ca.affinity().primaryPartitions(node);

                pps = new ArrayList<>(pp.length);

                for (int p : pp) {
                    Set set = ca.entrySet(p);

                    pps.add(new IgnitePair<>(p, set != null ? set.size() : 0));
                }

                int[] bp = ca.affinity().backupPartitions(node);

                bps = new ArrayList<>(bp.length);

                for (int p : bp) {
                    Set set = ca.entrySet(p);

                    bps.add(new IgnitePair<>(p, set != null ? set.size() : 0));
                }
            }
        }

        int size = ca.size();
        int near = ca.nearSize();

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

        VisorCache cache = new VisorCache();

        cache.name(cacheName);
        cache.mode(mode);
        cache.memorySize(memSz);
        cache.size(size);
        cache.nearSize(near);
        cache.dhtSize(size - near);
        cache.primarySize(ca.primarySize());
        cache.offHeapAllocatedSize(ca.offHeapAllocatedSize());
        cache.offHeapEntriesCount(ca.offHeapEntriesCount());
        cache.swapSize(swapSize);
        cache.swapKeys(swapKeys);
        cache.partitions(ca.affinity().partitions());
        cache.primaryPartitions(pps);
        cache.backupPartitions(bps);
        cache.metrics(VisorCacheMetrics.from(ca));
        cache.partitionMap(partsMap);

        return cache;
    }

    /**
     * @return New instance suitable to store in history.
     */
    public VisorCache history() {
        VisorCache c = new VisorCache();

        c.name(name);
        c.mode(mode);
        c.memorySize(memorySize);
        c.size(size);
        c.nearSize(nearSize);
        c.dhtSize(dhtSize);
        c.primarySize(primarySize);
        c.offHeapAllocatedSize(offHeapAllocatedSize);
        c.offHeapEntriesCount(offHeapEntriesCount);
        c.swapSize(swapSize);
        c.swapKeys(swapKeys);
        c.partitions(partsCnt);
        c.primaryPartitions(Collections.<IgnitePair<Integer>>emptyList());
        c.backupPartitions(Collections.<IgnitePair<Integer>>emptyList());
        c.metrics(metrics);

        return c;
    }

    /**
     * @return Cache name.
     */
    public String name() {
        return name;
    }

    /**
     * @param name New cache name.
     */
    public void name(String name) {
        this.name = name;
    }

    /**
     * @return Cache mode.
     */
    public CacheMode mode() {
        return mode;
    }

    /**
     * @param mode New cache mode.
     */
    public void mode(CacheMode mode) {
        this.mode = mode;
    }

    /**
     * @return Cache size in bytes.
     */
    public long memorySize() {
        return memorySize;
    }

    /**
     * @param memorySize New cache size in bytes.
     */
    public void memorySize(long memorySize) {
        this.memorySize = memorySize;
    }

    /**
     * @return Number of all entries in cache.
     */
    public int size() {
        return size;
    }

    /**
     * @param size New number of all entries in cache.
     */
    public void size(int size) {
        this.size = size;
    }

    /**
     * @return Number of all entries in near cache.
     */
    public int nearSize() {
        return nearSize;
    }

    /**
     * @param nearSize New number of all entries in near cache.
     */
    public void nearSize(int nearSize) {
        this.nearSize = nearSize;
    }

    /**
     * @return Number of all entries in DHT cache.
     */
    public int dhtSize() {
        return dhtSize;
    }

    /**
     * @param dhtSize New number of all entries in DHT cache.
     */
    public void dhtSize(int dhtSize) {
        this.dhtSize = dhtSize;
    }

    /**
     * @return Number of primary entries in cache.
     */
    public int primarySize() {
        return primarySize;
    }

    /**
     * @param primarySize New number of primary entries in cache.
     */
    public void primarySize(int primarySize) {
        this.primarySize = primarySize;
    }

    /**
     * @return Memory size allocated in off-heap.
     */
    public long offHeapAllocatedSize() {
        return offHeapAllocatedSize;
    }

    /**
     * @param offHeapAllocatedSize New memory size allocated in off-heap.
     */
    public void offHeapAllocatedSize(long offHeapAllocatedSize) {
        this.offHeapAllocatedSize = offHeapAllocatedSize;
    }

    /**
     * @return Number of cache entries stored in off-heap memory.
     */
    public long offHeapEntriesCount() {
        return offHeapEntriesCount;
    }

    /**
     * @param offHeapEntriesCnt New number of cache entries stored in off-heap memory.
     */
    public void offHeapEntriesCount(long offHeapEntriesCnt) {
        offHeapEntriesCount = offHeapEntriesCnt;
    }

    /**
     * @return Size in bytes for swap space.
     */
    public long swapSize() {
        return swapSize;
    }

    /**
     * @param swapSize New size in bytes for swap space.
     */
    public void swapSize(long swapSize) {
        this.swapSize = swapSize;
    }

    /**
     * @return Number of cache entries stored in swap space.
     */
    public long swapKeys() {
        return swapKeys;
    }

    /**
     * @param swapKeys New number of cache entries stored in swap space.
     */
    public void swapKeys(long swapKeys) {
        this.swapKeys = swapKeys;
    }

    /**
     * @return Number of partitions.
     */
    public int partitions() {
        return partsCnt;
    }

    /**
     * @param partsCnt New number of partitions.
     */
    public void partitions(int partsCnt) {
        this.partsCnt = partsCnt;
    }

    /**
     * @return Primary partitions IDs with sizes.
     */
    public Collection<IgnitePair<Integer>> primaryPartitions() {
        return primaryParts;
    }

    /**
     * @param primaryParts New primary partitions IDs with sizes.
     */
    public void primaryPartitions(Collection<IgnitePair<Integer>> primaryParts) {
        this.primaryParts = primaryParts;
    }

    /**
     * @return Backup partitions IDs with sizes.
     */
    public Collection<IgnitePair<Integer>> backupPartitions() {
        return backupParts;
    }

    /**
     * @param backupParts New backup partitions IDs with sizes.
     */
    public void backupPartitions(Collection<IgnitePair<Integer>> backupParts) {
        this.backupParts = backupParts;
    }

    /**
     * @return Cache metrics.
     */
    public VisorCacheMetrics metrics() {
        return metrics;
    }

    /**
     * @param metrics New cache metrics.
     */
    public void metrics(VisorCacheMetrics metrics) {
        this.metrics = metrics;
    }

    /**
     * @return Cache partitions states.
     */
    @Nullable public GridDhtPartitionMap partitionMap() {
        return partsMap;
    }

    /**
     * @param partsMap New cache partitions states.
     */
    public void partitionMap(@Nullable GridDhtPartitionMap partsMap) {
        this.partsMap = partsMap;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCache.class, this);
    }
}
