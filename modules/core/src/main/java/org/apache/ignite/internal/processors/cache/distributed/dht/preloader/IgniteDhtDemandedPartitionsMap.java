/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 *
 */

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Map of partitions demanded during rebalancing.
 */
public class IgniteDhtDemandedPartitionsMap implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Map of partitions that will be preloaded from history. (partId -> (fromCntr, toCntr)). */
    private CachePartitionPartialCountersMap historical;

    /** Set of partitions that will be preloaded from all it's current data. */
    private Set<Integer> full;

    public IgniteDhtDemandedPartitionsMap(
        @Nullable CachePartitionPartialCountersMap historical,
        @Nullable Set<Integer> full)
    {
        this.historical = historical;
        this.full = full;
    }

    public IgniteDhtDemandedPartitionsMap() {
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
