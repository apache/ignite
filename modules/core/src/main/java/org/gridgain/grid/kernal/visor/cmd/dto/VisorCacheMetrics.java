/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Data transfer object for cache metrics.
 */
public class VisorCacheMetrics implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private final String cacheName;

    /** Node id. */
    private final UUID nodeId;

    /** Number of CPUs available to the JVM. */
    private final int cpus;

    /** Percent of heap memory used. */
    private final double heapUsed;

    /** Percent of estimated CPU usage. */
    private final double cpuLoad;

    /** Uptime of the JVM in milliseconds. */
    private final long upTime;

    /** Number of elements in the cache. */
    private final int size;

    /** Last read time of the owning cache. */
    private final long lastRead;

    /** Last write time of the owning cache. */
    private final long lastWrite;

    /** Hits of the owning cache. */
    private final int hits;

    /** Misses of the owning cache. */
    private final int misses;

    /** Total number of reads of the owning cache. */
    private final int reads;

    /** Total number of writes of the owning cache. */
    private final int writes;

    /** Gets query metrics for cache. */
    private final VisorCacheQueryMetrics qryMetrics;

    /**
     * Create data transfer object with given parameters.
     *
     * @param cacheName Cache name.
     * @param nodeId Node id.
     * @param cpus Number of CPUs available to the JVM
     * @param heapUsed Percent of heap memory used.
     * @param cpuLoad Percent of estimated CPU usage.
     * @param upTime Uptime of the JVM in milliseconds.
     * @param size Number of elements in the cache.
     * @param lastRead Last read time of the owning cache.
     * @param lastWrite Last write time of the owning cache.
     * @param hits Hits of the owning cache.
     * @param misses Misses of the owning cache.
     * @param reads Total number of reads of the owning cache.
     * @param writes Total number of writes of the owning cache.
     * @param qryMetrics Gets query metrics for cache.
     */
    public VisorCacheMetrics(
        String cacheName,
        UUID nodeId,
        int cpus,
        double heapUsed,
        double cpuLoad,
        long upTime,
        int size,
        long lastRead,
        long lastWrite,
        int hits,
        int misses,
        int reads,
        int writes,
        VisorCacheQueryMetrics qryMetrics
    ) {
        this.cacheName = cacheName;
        this.nodeId = nodeId;
        this.cpus = cpus;
        this.heapUsed = heapUsed;
        this.cpuLoad = cpuLoad;
        this.upTime = upTime;
        this.size = size;
        this.lastRead = lastRead;
        this.lastWrite = lastWrite;
        this.hits = hits;
        this.misses = misses;
        this.reads = reads;
        this.writes = writes;
        this.qryMetrics = qryMetrics;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Node id.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Number of CPUs available to the JVM.
     */
    public int cpus() {
        return cpus;
    }

    /**
     * @return Percent of heap memory used.
     */
    public double heapUsed() {
        return heapUsed;
    }

    /**
     * @return Percent of estimated CPU usage.
     */
    public double cpuLoad() {
        return cpuLoad;
    }

    /**
     * @return Uptime of the JVM in milliseconds.
     */
    public long upTime() {
        return upTime;
    }

    /**
     * @return Number of elements in the cache.
     */
    public int size() {
        return size;
    }

    /**
     * @return Gets last read time of the owning cache.
     */
    public long lastRead() {
        return lastRead;
    }

    /**
     * @return Gets last write time of the owning cache.
     */
    public long lastWrite() {
        return lastWrite;
    }

    /**
     * @return Gets hits of the owning cache.
     */
    public int hits() {
        return hits;
    }

    /**
     * @return Gets misses of the owning cache.
     */
    public int misses() {
        return misses;
    }

    /**
     * @return Gets total number of reads of the owning cache.
     */
    public int reads() {
        return reads;
    }

    /**
     * @return Gets total number of writes of the owning cache.
     */
    public int writes() {
        return writes;
    }

    /**
     * @return Gets query metrics for cache.
     */
    public VisorCacheQueryMetrics queryMetrics() {
        return qryMetrics;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheMetrics.class, this);
    }
}
