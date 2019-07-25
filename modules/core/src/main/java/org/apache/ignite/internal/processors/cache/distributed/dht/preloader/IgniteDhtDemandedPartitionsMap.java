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
 *
 */

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Map of partitions demanded during rebalancing.
 */
public class IgniteDhtDemandedPartitionsMap implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Map of partitions that will be preloaded from history. (partId -> (fromCntr, toCntr)). */
    private CachePartitionPartialCountersMap historical;

    /** Set of partitions that will be preloaded from all it's current data. */
    @GridToStringInclude
    private Set<Integer> full;

    public IgniteDhtDemandedPartitionsMap() {
        // No-op.
    }

    /**
     * Adds partition for preloading from history.
     *
     * @param partId Partition ID.
     * @param from First demanded counter.
     * @param to Last demanded counter.
     * @param partCnt Maximum possible partition count.
     */
    public void addHistorical(int partId, long from, long to, int partCnt) {
        assert !hasFull(partId);

        if (historical == null)
            historical = new CachePartitionPartialCountersMap(partCnt);

        historical.add(partId, from, to);
    }

    /**
     * Adds partition for preloading from all current data.
     *
     * @param partId Partition ID.
     */
    public void addFull(int partId) {
        assert !hasHistorical(partId);

        if (full == null)
            full = new HashSet<>();

        full.add(partId);
    }

    /**
     * Removes partition.
     *
     * @param partId Partition ID.
     * @return {@code True} if changed.
     */
    public boolean remove(int partId) {
        assert !(hasFull(partId) && hasHistorical(partId));

        if (full != null && full.remove(partId))
            return true;

        if (historical != null && historical.remove(partId))
            return true;

        return false;
    }

    /** */
    public boolean hasPartition(int partId) {
        return hasHistorical(partId) || hasFull(partId);
    }

    /** */
    public boolean hasHistorical() {
        return historical != null && !historical.isEmpty();
    }

    /** */
    public boolean hasHistorical(int partId) {
        return historical != null && historical.contains(partId);
    }

    /** */
    public boolean hasFull() {
        return full != null && !full.isEmpty();
    }

    /** */
    public boolean hasFull(int partId) {
        return full != null && full.contains(partId);
    }

    /** */
    public boolean isEmpty() {
        return !hasFull() && !hasHistorical();
    }

    /** */
    public int size() {
        int histSize = historical != null ? historical.size() : 0;
        int fullSize = full != null ? full.size() : 0;

        return histSize + fullSize;
    }

    /** */
    public CachePartitionPartialCountersMap historicalMap() {
        if (historical == null)
            return CachePartitionPartialCountersMap.EMPTY;

        return historical;
    }

    /** */
    public Set<Integer> fullSet() {
        if (full == null)
            return Collections.emptySet();

        return Collections.unmodifiableSet(full);
    }

    /** */
    public Set<Integer> historicalSet() {
        if (historical == null)
            return Collections.emptySet();

        Set<Integer> historical = new HashSet<>(historicalMap().size());

        for (int i = 0; i < historicalMap().size(); i++) {
            int p = historicalMap().partitionAt(i);

            historical.add(p);
        }

        return historical;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteDhtDemandedPartitionsMap.class, this);
    }
}
