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
    private byte[] states;

    /** */
    private int idx;

    /**
     * @param partsCnt Partitions count.
     */
    public CacheState(int partsCnt) {
        parts = new int[partsCnt];
        vals = new long[partsCnt * 2];
        states = new byte[partsCnt];
    }

    /**
     * @param partId Partition ID to add.
     * @param size Partition size.
     * @param cntr Partition counter.
     */
    public void addPartitionState(int partId, long size, long cntr) {
        addPartitionState(partId, size, cntr, (byte)-1);
    }

    /**
     * @param partId Partition ID to add.
     * @param size Partition size.
     * @param cntr Partition counter.
     * @param state Partition state.
     */
    public void addPartitionState(int partId, long size, long cntr, byte state) {
        if (idx == parts.length)
            throw new IllegalStateException("Failed to add new partition to the partitions state " +
                "(no enough space reserved) [partId=" + partId + ", reserved=" + parts.length + ']');

        if (idx > 0) {
            if (parts[idx - 1] >= partId)
                throw new IllegalStateException("Adding partition in a wrong order [prev=" + parts[idx - 1] +
                    ", cur=" + partId + ']');
        }

        parts[idx] = partId;
        states[idx] = state;

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
     * @return State partition.
     */
    public byte stateByIndex(int idx) {
        return states[idx];
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return "CacheState [cap=" + parts.length + ", size=" + idx + ']';
    }
}
