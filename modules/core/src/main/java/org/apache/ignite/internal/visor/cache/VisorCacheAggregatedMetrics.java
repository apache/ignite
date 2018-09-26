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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Data transfer object for aggregated cache metrics.
 */
public class VisorCacheAggregatedMetrics extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private String name;

    /** Cache mode. */
    private CacheMode mode;

    /** Cache system state. */
    private boolean sys;

    /** Node IDs with cache metrics. */
    private Map<UUID, VisorCacheMetrics> metrics = new HashMap<>();

    /** Total number of entries in heap. */
    private transient Long totalHeapSize;

    /** Minimum number of entries in heap. */
    private transient Long minHeapSize;

    /** Average number of entries in heap. */
    private transient Double avgHeapSize;

    /** Maximum number of entries in heap. */
    private transient Long maxHeapSize;

    /** Total number of entries in off heap. */
    private transient Long totalOffHeapSize;

    /** Minimum number of entries in off heap. */
    private transient Long minOffHeapSize;

    /** Average number of entries in off heap. */
    private transient Double avgOffHeapSize;

    /** Maximum number of entries in off heap. */
    private transient Long maxOffHeapSize;

    /** Minimum hits of the owning cache. */
    private transient Long minHits;

    /** Average hits of the owning cache. */
    private transient Double avgHits;

    /** Maximum hits of the owning cache. */
    private transient Long maxHits;

    /** Minimum misses of the owning cache. */
    private transient Long minMisses;

    /** Average misses of the owning cache. */
    private transient Double avgMisses;

    /** Maximum misses of the owning cache. */
    private transient Long maxMisses;

    /** Minimum total number of reads of the owning cache. */
    private transient Long minReads;

    /** Average total number of reads of the owning cache. */
    private transient Double avgReads;

    /** Maximum total number of reads of the owning cache. */
    private transient Long maxReads;

    /** Minimum total number of writes of the owning cache. */
    private transient Long minWrites;

    /** Average total number of writes of the owning cache. */
    private transient Double avgWrites;

    /** Maximum total number of writes of the owning cache. */
    private transient Long maxWrites;

    /** Minimum execution time of query. */
    private transient Long minQryTime;

    /** Average execution time of query. */
    private transient Double avgQryTime;

    /** Maximum execution time of query. */
    private transient Long maxQryTime;

    /** Total execution time of query. */
    private transient Long totalQryTime;

    /** Number of executions. */
    private transient Integer execsQry;

    /** Total number of times a query execution failed. */
    private transient Integer failsQry;

    /**
     * Default constructor.
     */
    public VisorCacheAggregatedMetrics() {
        // No-op.
    }

    /**
     * Create data transfer object for aggregated cache metrics.
     *
     * @param cm Source cache metrics.
     */
    public VisorCacheAggregatedMetrics(VisorCacheMetrics cm) {
        name = cm.getName();
        mode = cm.getMode();
        sys = cm.isSystem();
    }

    /**
     * @return Cache name.
     */
    public String getName() {
        return name;
    }

    /** @return Cache mode. */
    public CacheMode getMode() {
        return mode;
    }

    /** @return Cache system state. */
    public boolean isSystem() {
        return sys;
    }

    /**
     * @return Nodes.
     */
    public Collection<UUID> getNodes() {
        return metrics.keySet();
    }

    /**
     * @return Total number of entries in heap.
     */
    public long getTotalHeapSize() {
        if (totalHeapSize == null) {
            totalHeapSize = 0L;

            for (VisorCacheMetrics metric : metrics.values())
                totalHeapSize += metric.getHeapEntriesCount();
        }

        return totalHeapSize;
    }

    /**
     * @return Minimum number of entries in heap.
     */
    public long getMinimumHeapSize() {
        if (minHeapSize == null) {
            minHeapSize = Long.MAX_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                minHeapSize = Math.min(minHeapSize, metric.getHeapEntriesCount());
        }

        return minHeapSize;
    }

    /**
     * @return Average number of entries in heap.
     */
    public double getAverageHeapSize() {
        if (avgHeapSize == null) {
            avgHeapSize = 0.0d;

            for (VisorCacheMetrics metric : metrics.values())
                avgHeapSize += metric.getHeapEntriesCount();

            avgHeapSize /= metrics.size();
        }

        return avgHeapSize;
    }

    /**
     * @return Maximum number of entries in heap.
     */
    public long getMaximumHeapSize() {
        if (maxHeapSize == null) {
            maxHeapSize = Long.MIN_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                maxHeapSize = Math.max(maxHeapSize, metric.getHeapEntriesCount());
        }

        return maxHeapSize;
    }

    /**
     * @param metric Metrics to process.
     * @return Off heap primary entries count.
     */
    private long getOffHeapPrimaryEntriesCount(VisorCacheMetrics metric) {
        return metric.getOffHeapPrimaryEntriesCount();
    }

    /**
     * @return Total number of entries in off-heap.
     */
    public long getTotalOffHeapSize() {
        if (totalOffHeapSize == null) {
            totalOffHeapSize = 0L;

            for (VisorCacheMetrics metric : metrics.values())
                totalOffHeapSize += metric.getOffHeapPrimaryEntriesCount();
        }

        return totalOffHeapSize;
    }

    /**
     * @return Minimum number of primary entries in off heap.
     */
    public long getMinimumOffHeapPrimarySize() {
        if (minOffHeapSize == null) {
            minOffHeapSize = Long.MAX_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                minOffHeapSize = Math.min(minOffHeapSize, getOffHeapPrimaryEntriesCount(metric));
        }

        return minOffHeapSize;
    }

    /**
     * @return Average number of primary entries in off heap.
     */
    public double getAverageOffHeapPrimarySize() {
        if (avgOffHeapSize == null) {
            avgOffHeapSize = 0.0d;

            for (VisorCacheMetrics metric : metrics.values())
                avgOffHeapSize += getOffHeapPrimaryEntriesCount(metric);

            avgOffHeapSize /= metrics.size();
        }

        return avgOffHeapSize;
    }

    /**
     * @return Maximum number of primary entries in off heap.
     */
    public long getMaximumOffHeapPrimarySize() {
        if (maxOffHeapSize == null) {
            maxOffHeapSize = Long.MIN_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                maxOffHeapSize = Math.max(maxOffHeapSize, getOffHeapPrimaryEntriesCount(metric));
        }

        return maxOffHeapSize;
    }

    /**
     * @return Minimum hits of the owning cache.
     */
    public long getMinimumHits() {
        if (minHits == null) {
            minHits = Long.MAX_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                minHits = Math.min(minHits, metric.getHits());
        }

        return minHits;
    }

    /**
     * @return Average hits of the owning cache.
     */
    public double getAverageHits() {
        if (avgHits == null) {
            avgHits = 0.0d;

            for (VisorCacheMetrics metric : metrics.values())
                avgHits += metric.getHits();

            avgHits /= metrics.size();
        }

        return avgHits;
    }

    /**
     * @return Maximum hits of the owning cache.
     */
    public long getMaximumHits() {
        if (maxHits == null) {
            maxHits = Long.MIN_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                maxHits = Math.max(maxHits, metric.getHits());
        }

        return maxHits;
    }

    /**
     * @return Minimum misses of the owning cache.
     */
    public long getMinimumMisses() {
        if (minMisses == null) {
            minMisses = Long.MAX_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                minMisses = Math.min(minMisses, metric.getMisses());
        }

        return minMisses;
    }

    /**
     * @return Average misses of the owning cache.
     */
    public double getAverageMisses() {
        if (avgMisses == null) {
            avgMisses = 0.0d;

            for (VisorCacheMetrics metric : metrics.values())
                avgMisses += metric.getMisses();

            avgMisses /= metrics.size();
        }

        return avgMisses;
    }

    /**
     * @return Maximum misses of the owning cache.
     */
    public long getMaximumMisses() {
        if (maxMisses == null) {
            maxMisses = Long.MIN_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                maxMisses = Math.max(maxMisses, metric.getMisses());
        }

        return maxMisses;
    }

    /**
     * @return Minimum total number of reads of the owning cache.
     */
    public long getMinimumReads() {
        if (minReads == null) {
            minReads = Long.MAX_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                minReads = Math.min(minReads, metric.getReads());
        }

        return minReads;
    }

    /**
     * @return Average total number of reads of the owning cache.
     */
    public double getAverageReads() {
        if (avgReads == null) {
            avgReads = 0.0d;

            for (VisorCacheMetrics metric : metrics.values())
                avgReads += metric.getReads();

            avgReads /= metrics.size();
        }

        return avgReads;
    }

    /**
     * @return Maximum total number of reads of the owning cache.
     */
    public long getMaximumReads() {
        if (maxReads == null) {
            maxReads = Long.MIN_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                maxReads = Math.max(maxReads, metric.getReads());
        }

        return maxReads;
    }

    /**
     * @return Minimum total number of writes of the owning cache.
     */
    public long getMinimumWrites() {
        if (minWrites == null) {
            minWrites = Long.MAX_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                minWrites = Math.min(minWrites, metric.getWrites());
        }

        return minWrites;
    }

    /**
     * @return Average total number of writes of the owning cache.
     */
    public double getAverageWrites() {
        if (avgWrites == null) {
            avgWrites = 0.0d;

            for (VisorCacheMetrics metric : metrics.values())
                avgWrites += metric.getWrites();

            avgWrites /= metrics.size();
        }

        return avgWrites;
    }

    /**
     * @return Maximum total number of writes of the owning cache.
     */
    public long getMaximumWrites() {
        if (maxWrites == null) {
            maxWrites = Long.MIN_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                maxWrites = Math.max(maxWrites, metric.getWrites());
        }

        return maxWrites;
    }

    /**
     * @return Minimum execution time of query.
     */
    public long getMinimumQueryTime() {
        if (minQryTime == null) {
            minQryTime = Long.MAX_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                minQryTime = Math.min(minQryTime, metric.getQueryMetrics().getMinimumTime());
        }

        return minQryTime;
    }

    /**
     * @return Average execution time of query.
     */
    public double getAverageQueryTime() {
        if (avgQryTime == null) {
            avgQryTime = 0.0d;

            for (VisorCacheMetrics metric : metrics.values())
                avgQryTime += metric.getQueryMetrics().getAverageTime();

            avgQryTime /= metrics.size();
        }

        return avgQryTime;
    }

    /**
     * @return Maximum execution time of query.
     */
    public long getMaximumQueryTime() {
        if (maxQryTime == null) {
            maxQryTime = Long.MIN_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                maxQryTime = Math.max(maxQryTime, metric.getQueryMetrics().getMaximumTime());
        }

        return maxQryTime;
    }

    /**
     * @return Total execution time of query.
     */
    public long getTotalQueryTime() {
        if (totalQryTime == null)
            totalQryTime = (long)(getAverageQueryTime() * getQueryExecutions());

        return totalQryTime;
    }

    /**
     * @return Number of executions.
     */
    public int getQueryExecutions() {
        if (execsQry == null) {
            execsQry = 0;

            for (VisorCacheMetrics metric : metrics.values())
                execsQry += metric.getQueryMetrics().getExecutions();
        }

        return execsQry;
    }

    /**
     * @return Total number of times a query execution failed.
     */
    public int getQueryFailures() {
        if (failsQry == null) {
            failsQry = 0;

            for (VisorCacheMetrics metric : metrics.values())
                failsQry += metric.getQueryMetrics().getFailures();
        }

        return failsQry;
    }

    /**
     * @return Map of Node IDs to cache metrics.
     */
    public Map<UUID, VisorCacheMetrics> getMetrics() {
        return metrics;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, name);
        U.writeEnum(out, mode);
        out.writeBoolean(sys);
        U.writeMap(out, metrics);
        out.writeObject(minHeapSize);
        out.writeObject(avgHeapSize);
        out.writeObject(maxHeapSize);
        out.writeObject(minOffHeapSize);
        out.writeObject(avgOffHeapSize);
        out.writeObject(maxOffHeapSize);
        out.writeObject(minHits);
        out.writeObject(avgHits);
        out.writeObject(maxHits);
        out.writeObject(minMisses);
        out.writeObject(avgMisses);
        out.writeObject(maxMisses);
        out.writeObject(minReads);
        out.writeObject(avgReads);
        out.writeObject(maxReads);
        out.writeObject(minWrites);
        out.writeObject(avgWrites);
        out.writeObject(maxWrites);
        out.writeObject(minQryTime);
        out.writeObject(avgQryTime);
        out.writeObject(maxQryTime);
        out.writeObject(totalQryTime);
        out.writeObject(execsQry);
        out.writeObject(failsQry);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        name = U.readString(in);
        mode = CacheMode.fromOrdinal(in.readByte());
        sys = in.readBoolean();
        metrics = U.readMap(in);
        minHeapSize = (Long)in.readObject();
        avgHeapSize = (Double)in.readObject();
        maxHeapSize = (Long)in.readObject();
        minOffHeapSize = (Long)in.readObject();
        avgOffHeapSize = (Double)in.readObject();
        maxOffHeapSize = (Long)in.readObject();
        minHits = (Long)in.readObject();
        avgHits = (Double)in.readObject();
        maxHits = (Long)in.readObject();
        minMisses = (Long)in.readObject();
        avgMisses = (Double)in.readObject();
        maxMisses = (Long)in.readObject();
        minReads = (Long)in.readObject();
        avgReads = (Double)in.readObject();
        maxReads = (Long)in.readObject();
        minWrites = (Long)in.readObject();
        avgWrites = (Double)in.readObject();
        maxWrites = (Long)in.readObject();
        minQryTime = (Long)in.readObject();
        avgQryTime = (Double)in.readObject();
        maxQryTime = (Long)in.readObject();
        totalQryTime = (Long)in.readObject();
        execsQry = (Integer)in.readObject();
        failsQry = (Integer)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheAggregatedMetrics.class, this);
    }
}
