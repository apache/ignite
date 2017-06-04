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

package org.apache.ignite.internal.pagemem.wal.record;

import java.util.Arrays;

/**
 *
 */
public class CacheState {
    /** */
    private int[] parts;

    /** */
    private long[] vals;

    /** */
    private int idx;

    /** */
    private boolean needsSort;

    /**
     * @param partsCnt Partitions count.
     */
    public CacheState(int partsCnt) {
        parts = new int[partsCnt];
        vals = new long[partsCnt * 2];
    }

    /**
     * @param partId Partition ID to add.
     * @param size Partition size.
     * @param cntr Partition counter.
     */
    public void addPartitionState(int partId, long size, long cntr) {
        if (idx == parts.length)
            throw new IllegalStateException("Failed to add new partition to the partitions state " +
                "(no enough space reserved) [partId=" + partId + ", reserved=" + parts.length + ']');

        if (idx > 0) {
            if (parts[idx - 1] >= partId)
                needsSort = true;
        }

        parts[idx] = partId;
        vals[2 * idx] = size;
        vals[2 * idx + 1] = cntr;

        idx++;
    }

    /**
     * Gets partition size by partition ID.
     *
     * @param partId Partition ID.
     * @return Partition size (will return {@code -1} if partition is not present in the record).
     */
    public long sizeByPartition(int partId) {
        int idx = indexByPartition(partId);

        return idx >= 0 ? vals[2 * idx] : -1;
    }

    /**
     * Gets partition counter by partition ID.
     *
     * @param partId Partition ID.
     * @return Partition update counter (will return {@code -1} if partition is not present in the record).
     */
    public long counterByPartition(int partId) {
        int idx = indexByPartition(partId);

        return idx >= 0 ? vals[2 * idx + 1] : 0;
    }

    /**
     * @param idx Index to get.
     * @return Partition ID.
     */
    public int partitionByIndex(int idx) {
        return parts[idx];
    }

    /**
     * @param idx Index to get.
     * @return Partition size by index.
     */
    public long partitionSizeByIndex(int idx) {
        return vals[idx * 2];
    }

    /**
     * @param idx Index to get.
     * @return Partition size by index.
     */
    public long partitionCounterByIndex(int idx) {
        return vals[idx * 2 + 1];
    }

    /**
     * @return State size.
     */
    public int size() {
        return idx;
    }

    /**
     * @param partId Partition ID to search.
     * @return Non-negative index of partition if found or negative value if not found.
     */
    private int indexByPartition(int partId) {
        return Arrays.binarySearch(parts, 0, idx, partId);
    }

    /**
     * This method is added for compatibility reasons because in earlier versions
     * this class has been written as a hash map, which does not preserve sorting.
     */
    public void checkSorted() {
        if (needsSort) {
            StateElement[] toSort = new StateElement[parts.length];

            for (int i = 0; i < parts.length; i++)
                toSort[i] = new StateElement(parts[i], vals[2 * i], vals[2 * i + 1]);

            Arrays.sort(toSort);

            for (int i = 0; i < parts.length; i++) {
                parts[i] = toSort[i].partId;
                vals[2 * i] = toSort[i].size;
                vals[2 * i + 1] = toSort[i].cntr;
            }

            needsSort = false;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "CacheState [cap=" + parts.length + ", size=" + idx + ']';
    }

    /**
     *
     */
    @SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
    private static class StateElement implements Comparable<StateElement> {
        /** */
        private int partId;

        /** */
        private long size;

        /** */
        private long cntr;

        /**
         * @param partId Partition ID.
         * @param size Size.
         * @param cntr Counter.
         */
        private StateElement(int partId, long size, long cntr) {
            this.partId = partId;
            this.size = size;
            this.cntr = cntr;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(StateElement o) {
            return Integer.compare(partId, o.partId);
        }
    }
}
