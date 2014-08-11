/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.dto;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.dr.cache.sender.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Data transfer object for {@link GridCache}.
 */
public class VisorCache implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default cache size sampling.  */
    private static final int DFLT_CACHE_SIZE_SAMPLING = 10;

    /** Cache name. */
    private String name;

    /** Cache mode. */
    private GridCacheMode mode;

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
    private Collection<GridPair<Integer>> primaryParts;

    /** Backup partitions IDs with sizes. */
    private Collection<GridPair<Integer>> backupParts;

    /** Cache metrics. */
    private VisorCacheMetrics metrics;

    /** Metrics for data sent during data center replication. */
    private VisorDrSenderCacheMetrics drSndMetrics;

    /** Metrics for data received during data center replication. */
    private VisorDrReceiverCacheMetrics drRcvMetrics;

    /** Cache partitions states. */
    private GridDhtPartitionMap partsMap;

    /**
     * @param g Grid.
     * @param c Actual cache.
     * @param sample Sample size.
     * @return Data transfer object for given cache.
     * @throws GridException
     */
    public static VisorCache from(Grid g, GridCache c, int sample) throws GridException {
        assert g != null;
        assert c != null;

        GridCacheMode mode = c.configuration().getCacheMode();
        GridCacheDistributionMode distributionMode = c.configuration().getDistributionMode();
        boolean partitioned = (mode == GridCacheMode.PARTITIONED || mode == GridCacheMode.REPLICATED)
            && distributionMode != GridCacheDistributionMode.CLIENT_ONLY;

        long swapSize;
        long swapKeys;

        try {
            swapSize = c.swapSize();
            swapKeys = c.swapKeys();
        }
        catch (GridException ignored) {
            swapSize = -1;
            swapKeys = -1;
        }

        GridNode node = g.localNode();

        Collection<GridPair<Integer>> pps = new ArrayList<>();

        if (partitioned) {
            for (int p : c.affinity().primaryPartitions(node)) {
                Set set = c.entrySet(p);

                pps.add(new GridPair<>(p, set != null ? set.size() : 0));
            }
        }

        Collection<GridPair<Integer>> bps = new ArrayList<>();

        if (partitioned) {
            for (int p : c.affinity().backupPartitions(node)) {
                Set set = c.entrySet(p);

                bps.add(new GridPair<>(p, set != null ? set.size() : 0));
            }
        }

        int size = c.size();
        int near = c.nearSize();

        Set<GridCacheEntry> set = c.entrySet();

        long memSz = 0;

        Iterator<GridCacheEntry> it = set.iterator();

        int sz = sample > 0 ? sample : DFLT_CACHE_SIZE_SAMPLING;

        int cnt = 0;

        while (it.hasNext() && cnt < sz) {
            memSz += it.next().memorySize();

            cnt++;
        }

        if (cnt > 0)
            memSz = (long)((double)memSz / cnt * size);

        GridCacheConfiguration cfg = c.configuration();

        GridDhtPartitionMap partsMap = (cfg.getCacheMode() != GridCacheMode.LOCAL && cfg.getBackups() > 0)
            ? ((GridCacheAdapter)c.cache()).context().topology().localPartitionMap() : null;

        VisorCache cache = new VisorCache();

        cache.name(c.name());
        cache.mode(mode);
        cache.memorySize(memSz);
        cache.size(size);
        cache.nearSize(near);
        cache.dhtSize(size - near);
        cache.primarySize(c.primarySize());
        cache.offHeapAllocatedSize(c.offHeapAllocatedSize());
        cache.offHeapEntriesCount(c.offHeapEntriesCount());
        cache.swapSize(swapSize);
        cache.swapKeys(swapKeys);
        cache.partitions(c.affinity().partitions());
        cache.primaryPartitions(pps);
        cache.backupPartitions(bps);
        cache.metrics(VisorCacheMetrics.from(c.metrics()));
        cache.drSendMetrics(VisorDrSenderCacheMetrics.from(c));
        cache.drReceiveMetrics(VisorDrReceiverCacheMetrics.from(c));
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
        c.primaryPartitions(Collections.<GridPair<Integer>>emptyList());
        c.backupPartitions(Collections.<GridPair<Integer>>emptyList());
        c.metrics(metrics);
        c.drSendMetrics(drSndMetrics);
        c.drReceiveMetrics(drRcvMetrics);

        return c;
    }

    /**
     * @return {@code true} if replication paused.
     */
    public boolean drSendMetricsPaused() {
        if (drSndMetrics != null) {
            GridDrStatus status = drSndMetrics.status();

            if (status != null) {
                return status.paused();
            }
        }

        return false;
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
    public GridCacheMode mode() {
        return mode;
    }

    /**
     * @param mode New cache mode.
     */
    public void mode(GridCacheMode mode) {
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
    public Collection<GridPair<Integer>> primaryPartitions() {
        return primaryParts;
    }

    /**
     * @param primaryParts New primary partitions IDs with sizes.
     */
    public void primaryPartitions(Collection<GridPair<Integer>> primaryParts) {
        this.primaryParts = primaryParts;
    }

    /**
     * @return Backup partitions IDs with sizes.
     */
    public Collection<GridPair<Integer>> backupPartitions() {
        return backupParts;
    }

    /**
     * @param backupParts New backup partitions IDs with sizes.
     */
    public void backupPartitions(Collection<GridPair<Integer>> backupParts) {
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
     * @return Metrics for data sent during data center replication.
     */
    public VisorDrSenderCacheMetrics drSendMetrics() {
        return drSndMetrics;
    }

    /**
     * @param drSndMetrics New metrics for data sent during data center replication.
     */
    public void drSendMetrics(VisorDrSenderCacheMetrics drSndMetrics) {
        this.drSndMetrics = drSndMetrics;
    }

    /**
     * @return Metrics for data received during data center replication.
     */
    public VisorDrReceiverCacheMetrics drReceiveMetrics() {
        return drRcvMetrics;
    }

    /**
     * @param drRcvMetrics New metrics for data received during data center replication.
     */
    public void drReceiveMetrics(VisorDrReceiverCacheMetrics drRcvMetrics) {
        this.drRcvMetrics = drRcvMetrics;
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
