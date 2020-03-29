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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;

/**
 *
 */
public class CachePartitionPartialCountersMap implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final IgniteProductVersion PARTIAL_COUNTERS_MAP_SINCE = IgniteProductVersion.fromString("2.1.4");

    /** */
    public static final CachePartitionPartialCountersMap EMPTY = new CachePartitionPartialCountersMap();

    /** */
    private int[] partIds;

    /** */
    private long[] initialUpdCntrs;

    /** */
    private long[] updCntrs;

    /** */
    private int curIdx;

    /** */
    private CachePartitionPartialCountersMap() {
        this(0);
    }

    /**
     * @param partsCnt Total number of partitions will be stored in the partial map.
     */
    public CachePartitionPartialCountersMap(int partsCnt) {
        partIds = new int[partsCnt];
        initialUpdCntrs = new long[partsCnt];
        updCntrs = new long[partsCnt];
    }

    /**
     * @return Total number of partitions added to the map.
     */
    public int size() {
        return curIdx;
    }

    /**
     * @return {@code True} if map is empty.
     */
    public boolean isEmpty() {
        return curIdx == 0;
    }

    /**
     * Adds partition counters for a partition with the given ID.
     *
     * @param partId Partition ID to add.
     * @param initialUpdCntr Partition initial update counter.
     * @param updCntr Partition update counter.
     */
    public void add(int partId, long initialUpdCntr, long updCntr) {
        if (curIdx > 0) {
            if (partIds[curIdx - 1] >= partId)
                throw new IllegalArgumentException("Adding a partition in the wrong order " +
                    "[prevPart=" + partIds[curIdx - 1] + ", partId=" + partId + ']');
        }

        if (curIdx == partIds.length)
            throw new IllegalStateException("Adding more partitions than reserved: " + partIds.length);

        partIds[curIdx] = partId;
        initialUpdCntrs[curIdx] = initialUpdCntr;
        updCntrs[curIdx] = updCntr;

        curIdx++;
    }

    /**
     * Removes element.
     *
     * @param partId Partition ID.
     * @return {@code True} if element was actually removed.
     */
    public boolean remove(int partId) {
        int removedIdx = partitionIndex(partId);

        if (removedIdx < 0)
            return false;

        int lastIdx = --curIdx;

        for (int i = removedIdx; i < lastIdx; i++) {
            partIds[i] = partIds[i + 1];
            initialUpdCntrs[i] = initialUpdCntrs[i + 1];
            updCntrs[i] = updCntrs[i + 1];
        }

        partIds[lastIdx] = 0;
        initialUpdCntrs[lastIdx] = 0;
        updCntrs[lastIdx] = 0;

        return true;
    }

    /**
     * Cuts the array sizes according to curIdx. No more entries can be added to this map after this method is called.
     */
    public void trim() {
        if (partIds != null && curIdx < partIds.length) {
            partIds = Arrays.copyOf(partIds, curIdx);
            initialUpdCntrs = Arrays.copyOf(initialUpdCntrs, curIdx);
            updCntrs = Arrays.copyOf(updCntrs, curIdx);
        }
    }

    /**
     * @param partId Partition ID to search.
     * @return Partition index in the array.
     */
    public int partitionIndex(int partId) {
        return Arrays.binarySearch(partIds, 0, curIdx, partId);
    }

    /**
     * @param partId Partition ID.
     * @return {@code True} if partition is present in map.
     */
    public boolean contains(int partId) {
        return partitionIndex(partId) >= 0;
    }

    /**
     * Gets partition ID saved at the given index.
     *
     * @param idx Index to get value from.
     * @return Partition ID.
     */
    public int partitionAt(int idx) {
        return partIds[idx];
    }

    /**
     * Gets initial update counter saved at the given index.
     *
     * @param idx Index to get value from.
     * @return Initial update counter.
     */
    public long initialUpdateCounterAt(int idx) {
        return initialUpdCntrs[idx];
    }

    /**
     * Gets update counter saved at the given index.
     *
     * @param idx Index to get value from.
     * @return Update counter.
     */
    public long updateCounterAt(int idx) {
        return updCntrs[idx];
    }

    /**
     * @param cntrsMap Partial local counters map.
     * @return Partition ID to partition counters map.
     */
    public static Map<Integer, T2<Long, Long>> toCountersMap(CachePartitionPartialCountersMap cntrsMap) {
        if (cntrsMap.size() == 0)
            return Collections.emptyMap();

        Map<Integer, T2<Long, Long>> res = U.newHashMap(cntrsMap.size());

        for (int idx = 0; idx < cntrsMap.size(); idx++)
            res.put(cntrsMap.partitionAt(idx),
                new T2<>(cntrsMap.initialUpdateCounterAt(idx), cntrsMap.updateCounterAt(idx)));

        return res;
    }

    /**
     * @param map Partition ID to partition counters map.
     * @param partsCnt Total cache partitions.
     * @return Partial local counters map.
     */
    static CachePartitionPartialCountersMap fromCountersMap(Map<Integer, T2<Long, Long>> map, int partsCnt) {
        CachePartitionPartialCountersMap map0 = new CachePartitionPartialCountersMap(partsCnt);

        TreeMap<Integer, T2<Long, Long>> sorted = new TreeMap<>(map);

        for (Map.Entry<Integer, T2<Long, Long>> e : sorted.entrySet())
            map0.add(e.getKey(), e.getValue().get1(), e.getValue().get2());

        map0.trim();

        return map0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder sb = new StringBuilder("CachePartitionPartialCountersMap {");

        for (int i = 0; i < partIds.length; i++) {
            sb.append(partIds[i]).append("=(").append(initialUpdCntrs[i]).append(",")
                .append(updCntrs[i]).append(")");

            if (i != partIds.length - 1)
                sb.append(", ");
        }

        sb.append("}");

        return sb.toString();
    }
}
