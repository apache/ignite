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

package org.apache.ignite.internal.binary.streams;

import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_MARSHAL_BUFFERS_RECHECK;

/**
 * Memory allocator chunk.
 */
public class BinaryMemoryAllocatorChunk {
    /** Buffer size re-check frequency. */
    private static final Long CHECK_FREQ = Long.getLong(IGNITE_MARSHAL_BUFFERS_RECHECK, 10000);

    /** Data array */
    private byte[] data;

    /** Max message size detected between checks. */
    private int maxMsgSize;

    /** Last time array size is checked. */
    private long lastCheckNanos = System.nanoTime();

    /** Whether the holder is acquired or not. */
    private boolean acquired;

    /**
     * Allocate.
     *
     * @param size Desired size.
     * @return Data.
     */
    public byte[] allocate(int size) {
        if (acquired)
            return new byte[size];

        acquired = true;

        if (data == null || size > data.length)
            data = new byte[size];

        return data;
    }

    /**
     * Reallocate.
     *
     * @param data Old data.
     * @param size Size.
     * @return New data.
     */
    public byte[] reallocate(byte[] data, int size) {
        byte[] newData = new byte[size];

        if (this.data == data)
            this.data = newData;

        System.arraycopy(data, 0, newData, 0, data.length);

        return newData;
    }

    /**
     * Shrinks array size if needed.
     */
    public void release(byte[] data, int maxMsgSize) {
        if (this.data != data)
            return;

        if (maxMsgSize > this.maxMsgSize)
            this.maxMsgSize = maxMsgSize;

        this.acquired = false;

        long nowNanos = System.nanoTime();

        if (U.nanosToMillis(nowNanos - lastCheckNanos) >= CHECK_FREQ) {
            int halfSize = data.length >> 1;

            if (this.maxMsgSize < halfSize)
                this.data = new byte[halfSize];

            lastCheckNanos = nowNanos;
        }
    }

    /**
     * @return {@code True} if acquired.
     */
    public boolean isAcquired() {
        return acquired;
    }
}
