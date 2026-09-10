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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Map;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;

/**
 * Partition update counters message.
 *
 * @see #finishUpdating()
 */
public class PartitionUpdateCountersMessage implements Message {
    /** */
    private static final int ITEM_SIZE = 4 /* partition */ + 8 /* initial counter */ + 8 /* updates count */;

    /**
     * Views over {@link #data}. The byte order is pinned instead of following the host, so that the bytes a node puts
     * on the wire do not depend on the architecture it runs on. Item fields are not naturally aligned, which the plain
     * {@code get}/{@code set} access modes used here allow.
     */
    private static final VarHandle INT_VIEW = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    /** */
    private static final VarHandle LONG_VIEW = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    /** */
    @Order(0)
    int cacheId;

    /** Byte representation of partition counters. */
    @Order(1)
    byte[] data;

    /** */
    @Order(2)
    int size;

    /** Used for assigning counters to cache entries during tx finish. */
    private Map<Integer, Long> counters;

    /** Empty constructor for a {@link MessageFactory}. */
    public PartitionUpdateCountersMessage() {
        // No-op.
    }

    /**
     * @param cacheId Cache id.
     * @param initSize Initial size.
     */
    public PartitionUpdateCountersMessage(int cacheId, int initSize) {
        assert initSize >= 1;

        this.cacheId = cacheId;
        data = new byte[initSize * ITEM_SIZE];
    }

    /**
     * @return Cache id.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * @return Size.
     */
    public int size() {
        return size;
    }

    /**
     * @param idx Item number.
     * @return Partition number.
     */
    public int partition(int idx) {
        if (idx >= size)
            throw new ArrayIndexOutOfBoundsException();

        return (int)INT_VIEW.get(data, idx * ITEM_SIZE);
    }

    /**
     * @param idx Item number.
     * @return Partition number.
     */
    public long initialCounter(int idx) {
        if (idx >= size)
            throw new ArrayIndexOutOfBoundsException();

        return (long)LONG_VIEW.get(data, idx * ITEM_SIZE + 4);
    }

    /**
     * @param idx Item number.
     * @return Update counter delta.
     */
    public long updatesCount(int idx) {
        if (idx >= size)
            throw new ArrayIndexOutOfBoundsException();

        return (long)LONG_VIEW.get(data, idx * ITEM_SIZE + 12);
    }

    /**
     * @param part Partition number.
     * @param init Init partition counter.
     * @param updatesCnt Update counter delta.
     *
     * @see #finishUpdating()
     */
    public void add(int part, long init, long updatesCnt) {
        ensureSpace(size + 1);

        int off = size++ * ITEM_SIZE;

        INT_VIEW.set(data, off, part);
        LONG_VIEW.set(data, off + 4, init);
        LONG_VIEW.set(data, off + 12, updatesCnt);
    }

    /** Optimizes the memory used after adding counters with {@link #add(int, long, long)}. */
    public void finishUpdating() {
        if (data != null && data.length != size * ITEM_SIZE) {
            assert data.length > size * ITEM_SIZE;

            data = Arrays.copyOf(data, size * ITEM_SIZE);
        }
    }

    /**
     * Calculate next counter for partition.
     *
     * @param partId Partition id.
     *
     * @return Next counter for partition.
     */
    public Long nextCounter(int partId) {
        if (counters == null) {
            counters = U.newHashMap(size);

            for (int i = 0; i < size; i++)
                counters.put(partition(i), initialCounter(i));
        }

        return counters.computeIfPresent(partId, (key, cntr) -> cntr + 1);
    }

    /**
     * Check if there is enough space is allocated.
     *
     * @param newSize Size to ensure.
     */
    private void ensureSpace(int newSize) {
        int req = newSize * ITEM_SIZE;

        // Growth alone may fall short of the request: 1.33 of a one-item array is still less than two items.
        if (data.length < req)
            data = Arrays.copyOf(data, Math.max(req, (int)(data.length * 1.33f)));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < size; i++) {
            sb.append("[part=")
                .append(partition(i))
                .append(", initCntr=")
                .append(initialCounter(i))
                .append(", cntr=")
                .append(updatesCount(i))
                .append(']');
        }

        return "PartitionUpdateCountersMessage{" +
            "cacheId=" + cacheId +
            ", size=" + size +
            ", cntrs=" + sb +
            '}';
    }
}
