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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.cluster.ClusterNode;

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Statistics of rebalance.
 */
public class RebalanceStatistics {
    /** Start time of rebalance in milliseconds. */
    private final AtomicLong start = new AtomicLong();

    /** End time of rebalance in milliseconds. */
    private final AtomicLong end = new AtomicLong();

    /** Rebalance attempt. */
    private final AtomicInteger attempt = new AtomicInteger();

    /** Rebalance statistics for suppliers. */
    private final Map<ClusterNode, SupplierRebalanceStatistics> supStat = new ConcurrentHashMap<>();

    /**
     * Default constructor.
     */
    public RebalanceStatistics() {
    }

    /**
     * Constructor.
     *
     * @param attempt Rebalance attempt, must be greater than {@code 0}.
     */
    public RebalanceStatistics(int attempt) {
        assert attempt > 0;

        this.attempt.set(attempt);
    }

    /**
     * Copying constructor.
     *
     * @param other Other instance.
     */
    public RebalanceStatistics(RebalanceStatistics other) {
        attempt.set(other.attempt.get());
        start.set(other.start.get());
        end.set(other.end.get());
        supStat.putAll(other.supStat);
    }

    /**
     * Set start time of rebalance for supplier in milliseconds.
     *
     * @param supplierNode Supplier node.
     * @param start        Start time of rebalance in milliseconds.
     */
    public void start(ClusterNode supplierNode, long start) {
        supplierRebalanceStatistics(supplierNode).start(start);
    }

    /**
     * Set end time of rebalance for supplier in milliseconds.
     *
     * @param supplierNode Supplier node.
     * @param end          End time of rebalance in milliseconds.
     */
    public void end(ClusterNode supplierNode, long end) {
        supplierRebalanceStatistics(supplierNode).end(end);
    }

    /**
     * Set end time of rebalance for supplier in milliseconds.
     *
     * @param supplierNodeId Supplier node id.
     * @param end            End time of rebalance in milliseconds.
     */
    public void end(UUID supplierNodeId, long end) {
        for (Entry<ClusterNode, SupplierRebalanceStatistics> supStatEntry : supStat.entrySet()) {
            if (supStatEntry.getKey().id().equals(supplierNodeId)) {
                supStatEntry.getValue().end(end);
                return;
            }
        }
    }

    /**
     * Updating statistics for supplier.
     *
     * @param supplierNode Supplier node.
     * @param p            Partition id.
     * @param hist         Historical or full rebalance.
     * @param e            Count of entries.
     * @param b            Count of bytes.
     */
    public void update(ClusterNode supplierNode, int p, boolean hist, long e, long b) {
        supplierRebalanceStatistics(supplierNode).update(hist, p, e, b);
    }

    /**
     * Merging statistics, without {@link SupplierRebalanceStatistics#partitions}.
     *
     * @param other Other rebalance statistics.
     */
    public void merge(RebalanceStatistics other) {
        start.getAndUpdate(s -> s == 0 ? other.start() : min(other.start(), s));
        end.getAndUpdate(e -> max(other.end(), e));
        attempt.getAndUpdate(a -> max(other.attempt(), a));

        for (Entry<ClusterNode, SupplierRebalanceStatistics> e : other.supplierStatistics().entrySet())
            supStat.computeIfAbsent(e.getKey(), n -> new SupplierRebalanceStatistics()).merge(e.getValue());
    }

    /**
     * Set start time of rebalance in milliseconds.
     *
     * @param start Start time of rebalance in milliseconds.
     */
    public void start(long start) {
        this.start.set(start);
        end(start);
    }

    /**
     * Return start time of rebalance in milliseconds.
     *
     * @return Start time of rebalance in milliseconds.
     */
    public long start() {
        return start.get();
    }

    /**
     * Set end time of rebalance in milliseconds.
     *
     * @param end End time of rebalance in milliseconds.
     */
    public void end(long end) {
        this.end.set(end);
    }

    /**
     * Return end time of rebalance in milliseconds.
     *
     * @return End time of rebalance in milliseconds.
     */
    public long end() {
        return end.get();
    }

    /**
     * Return rebalance statistics for suppliers.
     *
     * @return Rebalance statistics for suppliers.
     */
    public Map<ClusterNode, SupplierRebalanceStatistics> supplierStatistics() {
        return supStat;
    }

    /**
     * Return rebalance attempt.
     *
     * @return Rebalance attempt.
     */
    public int attempt() {
        return attempt.get();
    }

    /**
     * Reset statistics.
     */
    public void reset() {
        start.set(0);
        end.set(0);

        supStat.clear();
    }

    /**
     * Reset attempt.
     */
    public void resetAttempt() {
        attempt.set(0);
    }

    /**
     * Return rebalance statistics for supplier node.
     *
     * @param supplierNode Supplier node.
     * @return Rebalance statistics for supplier node.
     */
    private SupplierRebalanceStatistics supplierRebalanceStatistics(ClusterNode supplierNode) {
        return supStat.computeIfAbsent(supplierNode, n -> new SupplierRebalanceStatistics());
    }
}
