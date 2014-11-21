/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.dto;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Data transfer object for aggregated cache metrics.
 */
public class VisorCacheAggregatedMetrics implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private final String cacheName;

    /** Nodes with this cache. */
    private Collection<UUID> nodes = new ArrayList<>();

    /** Min number of elements in the cache. */
    private int minSize = Integer.MAX_VALUE;

    /** Avg number of elements in the cache. */
    private double avgSize;

    /** Max number of elements in the cache. */
    private int maxSize = Integer.MIN_VALUE;

    /** Gets last read time of the owning cache. */
    private long lastRead;

    /** Gets last read time of the owning cache. */
    private long lastWrite;

    /** Hits of the owning cache. */
    private int minHits = Integer.MAX_VALUE;

    /** Hits of the owning cache. */
    private double avgHits;

    /** Hits of the owning cache. */
    private int maxHits = Integer.MIN_VALUE;

    /** Minimum misses of the owning cache. */
    private int minMisses = Integer.MAX_VALUE;

    /** Average misses of the owning cache. */
    private double avgMisses;

    /** Maximum misses of the owning cache. */
    private int maxMisses = Integer.MIN_VALUE;

    /** Minimum total number of reads of the owning cache. */
    private int minReads = Integer.MAX_VALUE;

    /** Average total number of reads of the owning cache. */
    private double avgReads;

    /** Maximum total number of reads of the owning cache. */
    private int maxReads = Integer.MIN_VALUE;

    /** Minimum total number of writes of the owning cache. */
    private int minWrites = Integer.MAX_VALUE;

    /** Average total number of writes of the owning cache. */
    private double avgWrites;

    /** Maximum total number of writes of the owning cache. */
    private int maxWrites = Integer.MIN_VALUE;

    /**  */
    private VisorCacheQueryAggregatedMetrics qryMetrics = new VisorCacheQueryAggregatedMetrics();

    /**  */
    private Collection<VisorCacheMetrics2> metrics = new ArrayList<>();

    /**
     * Create data transfer object with given parameters.
     *
     * @param cacheName Cache name.
     */
    public VisorCacheAggregatedMetrics(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Nodes.
     */
    public Collection<UUID> nodes() {
        return nodes;
    }

    /**
     * @return Min size.
     */
    public int minSize() {
        return minSize;
    }

    /**
     * @param size New min size.
     */
    public void minSize(int size) {
        minSize = Math.min(minSize, size);
    }

    /**
     * @return Avg size.
     */
    public double avgSize() {
        return avgSize;
    }

    /**
     * @param avgSize New avg size.
     */
    public void avgSize(double avgSize) {
        this.avgSize = avgSize;
    }

    /**
     * @return Max size.
     */
    public int maxSize() {
        return maxSize;
    }

    /**
     * @param size New max size.
     */
    public void maxSize(int size) {
        maxSize = Math.max(maxSize, size);
    }

    /**
     * @return Last read.
     */
    public long lastRead() {
        return lastRead;
    }

    /**
     * @param lastRead New last read.
     */
    public void lastRead(long lastRead) {
        this.lastRead = Math.max(this.lastRead, lastRead);
    }

    /**
     * @return Last write.
     */
    public long lastWrite() {
        return lastWrite;
    }

    /**
     * @param lastWrite New last write.
     */
    public void lastWrite(long lastWrite) {
        this.lastWrite = Math.max(this.lastWrite, lastWrite);
    }

    /**
     * @return Min hits.
     */
    public int minHits() {
        return minHits;
    }

    /**
     * @param minHits New min hits.
     */
    public void minHits(int minHits) {
        this.minHits = Math.min(this.minHits, minHits);
    }

    /**
     * @return Avg hits.
     */
    public double avgHits() {
        return avgHits;
    }

    /**
     * @param avgHits New avg hits.
     */
    public void avgHits(double avgHits) {
        this.avgHits = avgHits;
    }

    /**
     * @return Max hits.
     */
    public int maxHits() {
        return maxHits;
    }

    /**
     * @param hits New max hits.
     */
    public void maxHits(int hits) {
        maxHits = Math.max(maxHits, hits);
    }

    /**
     * @return Min misses.
     */
    public int minMisses() {
        return minMisses;
    }

    /**
     * @param misses New min misses.
     */
    public void minMisses(int misses) {
        minMisses = Math.min(minMisses, misses);
    }

    /**
     * @return Avg misses.
     */
    public double avgMisses() {
        return avgMisses;
    }

    /**
     * @param avgMisses New avg misses.
     */
    public void avgMisses(double avgMisses) {
        this.avgMisses = avgMisses;
    }

    /**
     * @return Max misses.
     */
    public int maxMisses() {
        return maxMisses;
    }

    /**
     * @param maxMisses New max misses.
     */
    public void maxMisses(int maxMisses) {
        this.maxMisses = maxMisses;
    }

    /**
     * @return Min reads.
     */
    public int minReads() {
        return minReads;
    }

    /**
     * @param reads New min reads.
     */
    public void minReads(int reads) {
        minReads = Math.min(minReads, reads);
    }

    /**
     * @return Avg reads.
     */
    public double avgReads() {
        return avgReads;
    }

    /**
     * @param avgReads New avg reads.
     */
    public void avgReads(double avgReads) {
        this.avgReads = avgReads;
    }

    /**
     * @return Max reads.
     */
    public int maxReads() {
        return maxReads;
    }

    /**
     * @param reads New max reads.
     */
    public void maxReads(int reads) {
        maxReads = Math.max(maxReads, reads);
    }

    /**
     * @return Min writes.
     */
    public int minWrites() {
        return minWrites;
    }

    /**
     * @param writes New min writes.
     */
    public void minWrites(int writes) {
        minWrites = Math.min(minWrites, writes);
    }

    /**
     * @return Avg writes.
     */
    public double avgWrites() {
        return avgWrites;
    }

    /**
     * @param avgWrites New avg writes.
     */
    public void avgWrites(double avgWrites) {
        this.avgWrites = avgWrites;
    }

    /**
     * @return Max writes.
     */
    public int maxWrites() {
        return maxWrites;
    }

    /**
     * @param writes New max writes.
     */
    public void maxWrites(int writes) {
        maxWrites = Math.max(maxWrites, writes);
    }

    /**
     * @return Query metrics.
     */
    public VisorCacheQueryAggregatedMetrics queryMetrics() {
        return qryMetrics;
    }

    /**
     * @return Metrics.
     */
    public Collection<VisorCacheMetrics2> metrics() {
        return metrics;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheAggregatedMetrics.class, this);
    }
}
