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
    private long lastCheck = U.currentTimeMillis();

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

        long now = U.currentTimeMillis();

        if (now - this.lastCheck >= CHECK_FREQ) {
            int halfSize = data.length >> 1;

            if (this.maxMsgSize < halfSize)
                this.data = new byte[halfSize];

            this.lastCheck = now;
        }
    }

    /**
     * @return {@code True} if acquired.
     */
    public boolean isAcquired() {
        return acquired;
    }
}
