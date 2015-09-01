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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Data transfer object for aggregated cache metrics.
 */
public class VisorCacheAggregatedMetrics implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private String name;

    /** Cache mode. */
    private CacheMode mode;

    /** Cache system state. */
    private boolean sys;

    /** Node IDs with cache metrics. */
    private final Map<UUID, VisorCacheMetrics> metrics = new HashMap<>();

    /** Minimum number of elements in the cache. */
    private transient Integer minSize;

    /** Average number of elements in the cache. */
    private transient Double avgSize;

    /** Maximum number of elements in the cache. */
    private transient Integer maxSize;

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
     * Create data transfer object for aggregated cache metrics.
     *
     * @param cm Source cache metrics.
     * @return Data transfer object for aggregated cache metrics.
     */
    public static VisorCacheAggregatedMetrics from(VisorCacheMetrics cm) {
        VisorCacheAggregatedMetrics acm = new VisorCacheAggregatedMetrics();

        acm.name = cm.name();
        acm.mode = cm.mode();
        acm.sys = cm.system();

        return acm;
    }

    /**
     * @return Cache name.
     */
    public String name() {
        return name;
    }

    /** @return Cache mode. */
    public CacheMode mode() {
        return mode;
    }

    /** @return Cache system state. */
    public boolean system() {
        return sys;
    }

    /**
     * @return Nodes.
     */
    public Collection<UUID> nodes() {
        return metrics.keySet();
    }

    /**
     * @return Minimum number of elements in the cache.
     */
    public int minimumSize() {
        if (minSize == null) {
            minSize = Integer.MAX_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                minSize = Math.min(minSize, metric.keySize());
        }

        return minSize;
    }

    /**
     * @return Average number of elements in the cache.
     */
    public double averageSize() {
        if (avgSize == null) {
            avgSize = 0.0d;

            for (VisorCacheMetrics metric : metrics.values())
                avgSize += metric.keySize();

            avgSize /= metrics.size();
        }

        return avgSize;
    }

    /**
     * @return Maximum number of elements in the cache.
     */
    public int maximumSize() {
        if (maxSize == null) {
            maxSize = Integer.MIN_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                maxSize = Math.max(maxSize, metric.keySize());
        }

        return maxSize;
    }

    /**
     * @return Minimum hits of the owning cache.
     */
    public long minimumHits() {
        if (minHits == null) {
            minHits = Long.MAX_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                minHits = Math.min(minHits, metric.hits());
        }

        return minHits;
    }

    /**
     * @return Average hits of the owning cache.
     */
    public double averageHits() {
        if (avgHits == null) {
            avgHits = 0.0d;

            for (VisorCacheMetrics metric : metrics.values())
                avgHits += metric.hits();

            avgHits /= metrics.size();
        }

        return avgHits;
    }

    /**
     * @return Maximum hits of the owning cache.
     */
    public long maximumHits() {
        if (maxHits == null) {
            maxHits = Long.MIN_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                maxHits = Math.max(maxHits, metric.hits());
        }

        return maxHits;
    }

    /**
     * @return Minimum misses of the owning cache.
     */
    public long minimumMisses() {
        if (minMisses == null) {
            minMisses = Long.MAX_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                minMisses = Math.min(minMisses, metric.misses());
        }

        return minMisses;
    }

    /**
     * @return Average misses of the owning cache.
     */
    public double averageMisses() {
        if (avgMisses == null) {
            avgMisses = 0.0d;

            for (VisorCacheMetrics metric : metrics.values())
                avgMisses += metric.misses();

            avgMisses /= metrics.size();
        }

        return avgMisses;
    }

    /**
     * @return Maximum misses of the owning cache.
     */
    public long maximumMisses() {
        if (maxMisses == null) {
            maxMisses = Long.MIN_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                maxMisses = Math.max(maxMisses, metric.misses());
        }

        return maxMisses;
    }

    /**
     * @return Minimum total number of reads of the owning cache.
     */
    public long minimumReads() {
        if (minReads == null) {
            minReads = Long.MAX_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                minReads = Math.min(minReads, metric.reads());
        }

        return minReads;
    }

    /**
     * @return Average total number of reads of the owning cache.
     */
    public double averageReads() {
        if (avgReads == null) {
            avgReads = 0.0d;

            for (VisorCacheMetrics metric : metrics.values())
                avgReads += metric.reads();

            avgReads /= metrics.size();
        }

        return avgReads;
    }

    /**
     * @return Maximum total number of reads of the owning cache.
     */
    public long maximumReads() {
        if (maxReads == null) {
            maxReads = Long.MIN_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                maxReads = Math.max(maxReads, metric.reads());
        }

        return maxReads;
    }

    /**
     * @return Minimum total number of writes of the owning cache.
     */
    public long minimumWrites() {
        if (minWrites == null) {
            minWrites = Long.MAX_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                minWrites = Math.min(minWrites, metric.writes());
        }

        return minWrites;
    }

    /**
     * @return Average total number of writes of the owning cache.
     */
    public double averageWrites() {
        if (avgWrites == null) {
            avgWrites = 0.0d;

            for (VisorCacheMetrics metric : metrics.values())
                avgWrites += metric.writes();

            avgWrites /= metrics.size();
        }

        return avgWrites;
    }

    /**
     * @return Maximum total number of writes of the owning cache.
     */
    public long maximumWrites() {
        if (maxWrites == null) {
            maxWrites = Long.MIN_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                maxWrites = Math.max(maxWrites, metric.writes());
        }

        return maxWrites;
    }

    /**
     * @return Minimum execution time of query.
     */
    public long minimumQueryTime() {
        if (minQryTime == null) {
            minQryTime = Long.MAX_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                minQryTime = Math.min(minQryTime, metric.queryMetrics().minimumTime());
        }

        return minQryTime;
    }

    /**
     * @return Average execution time of query.
     */
    public double averageQueryTime() {
        if (avgQryTime == null) {
            avgQryTime = 0.0d;

            for (VisorCacheMetrics metric : metrics.values())
                avgQryTime += metric.queryMetrics().averageTime();

            avgQryTime /= metrics.size();
        }

        return avgQryTime;
    }

    /**
     * @return Maximum execution time of query.
     */
    public long maximumQueryTime() {
        if (maxQryTime == null) {
            maxQryTime = Long.MIN_VALUE;

            for (VisorCacheMetrics metric : metrics.values())
                maxQryTime = Math.max(maxQryTime, metric.queryMetrics().maximumTime());
        }

        return maxQryTime;
    }

    /**
     * @return Total execution time of query.
     */
    public long totalQueryTime() {
        if (totalQryTime == null)
            totalQryTime = (long)(averageQueryTime() * execsQuery());

        return totalQryTime;
    }

    /**
     * @return Number of executions.
     */
    public int execsQuery() {
        if (execsQry == null) {
            execsQry = 0;

            for (VisorCacheMetrics metric : metrics.values())
                execsQry += metric.queryMetrics().executions();
        }

        return execsQry;
    }

    /**
     * @return Total number of times a query execution failed.
     */
    public int failsQuery() {
        if (failsQry == null) {
            failsQry = 0;

            for (VisorCacheMetrics metric : metrics.values())
                failsQry += metric.queryMetrics().fails();
        }

        return failsQry;
    }

    /**
     * @return Node IDs with cache metrics.
     */
    public Map<UUID, VisorCacheMetrics> metrics() {
        return metrics;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheAggregatedMetrics.class, this);
    }
}