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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.isNull;

/**
 * Statistics of rebalance by supplier.
 */
public class SupplierRebalanceStatistics {
    /** Start time of rebalance in milliseconds. */
    private final AtomicLong start = new AtomicLong();

    /** End time of rebalance in milliseconds. */
    private final AtomicLong end = new AtomicLong();

    /**
     * Rebalanced partitions.
     * Key - partition id, value - fully({@code true}) or
     * historically({@code false}) rebalanced.
     */
    private final Map<Integer, Boolean> parts = new ConcurrentHashMap<>();

    /** Counter of partitions received by full rebalance. */
    private final LongAdder fullParts = new LongAdder();

    /** Counter of partitions received by historical rebalance. */
    private final LongAdder histParts = new LongAdder();

    /** Counter of entries received by full rebalance. */
    private final LongAdder fullEntries = new LongAdder();

    /** Counter of entries received by historical rebalance. */
    private final LongAdder histEntries = new LongAdder();

    /** Counter of bytes received by full rebalance. */
    private final LongAdder fullBytes = new LongAdder();

    /** Counter of bytes received by historical rebalance. */
    private final LongAdder histBytes = new LongAdder();

    /**
     * Merging statistics of rebalance by supplier, without {@link #partitions}.
     *
     * @param other Other statistics of rebalance by supplier.
     */
    public void merge(SupplierRebalanceStatistics other) {
        start.getAndUpdate(prev -> prev == 0 ? other.start() : min(other.start(), prev));
        end.getAndUpdate(prev -> max(other.end(), prev));

        fullParts.add(other.fullParts());
        histParts.add(other.histParts());
        fullEntries.add(other.fullEntries());
        histEntries.add(other.histEntries());
        fullBytes.add(other.fullBytes());
        histBytes.add(other.histBytes());
    }

    /**
     * Updating statistics.
     *
     * @param p    Partition id.
     * @param hist Historical or full rebalance.
     * @param e    Count of entries.
     * @param b    Count of bytes.
     */
    public void update(boolean hist, int p, long e, long b) {
        Boolean prev = parts.put(p, !hist);

        if (isNull(prev))
            (hist ? histParts : fullParts).add(1);

        (hist ? histEntries : fullEntries).add(e);
        (hist ? histBytes : fullBytes).add(b);

        end(U.currentTimeMillis());
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
     * Set end time of rebalance in milliseconds.
     *
     * @param end End time of rebalance in milliseconds.
     */
    public void end(long end) {
        this.end.set(end);
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
     * Return end time of rebalance in milliseconds.
     *
     * @return End time of rebalance in milliseconds.
     */
    public long end() {
        return end.get();
    }

    /**
     * Returns rebalanced partitions.
     * Key - partition id, value - fully({@code true}) or
     * historically({@code false}) rebalanced.
     *
     * @return Rebalanced partitions.
     */
    public Map<Integer, Boolean> partitions() {
        return parts;
    }

    /**
     * Return count of partitions received by full rebalance.
     *
     * @return Count of partitions received by full rebalance.
     */
    public long fullParts() {
        return fullParts.sum();
    }

    /**
     * Return count of partitions received by historical rebalance.
     *
     * @return Count of partitions received by historical rebalance.
     */
    public long histParts() {
        return histParts.sum();
    }

    /**
     * Return count of entries received by full rebalance.
     *
     * @return Count of entries received by full rebalance.
     */
    public long fullEntries() {
        return fullEntries.sum();
    }

    /**
     * Return count of entries received by historical rebalance.
     *
     * @return Count of entries received by historical rebalance.
     */
    public long histEntries() {
        return histEntries.sum();
    }

    /**
     * Return count of bytes received by full rebalance.
     *
     * @return Count of bytes received by full rebalance.
     */
    public long fullBytes() {
        return fullBytes.sum();
    }

    /**
     * Return count of bytes received by historical rebalance.
     *
     * @return Count of bytes received by historical rebalance.
     */
    public long histBytes() {
        return histBytes.sum();
    }
}
