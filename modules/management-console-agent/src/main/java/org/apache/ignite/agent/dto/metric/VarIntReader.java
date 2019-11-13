/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.dto.metric;

/**
 * <p>Provides varInt get operations for {@code int} and {@code long} types from backing byte array.</p>
 * <p>Each get operation moves position forward accordingly to the read type size.</p>
 */
public class VarIntReader {
    /** Backing byte array. */
    private final byte[] arr;

    /** Current position. */
    private int pos;

    /**
     * Constructor.
     *
     * @param arr Backing byte array.
     */
    public VarIntReader(byte[] arr) {
        this.arr = arr;
    }

    /**
     * Sets position for reading.
     *
     * @param pos Position.
     */
    public void position(int pos) {
        this.pos = pos;
    }

    /**
     * Reads boolean value.
     *
     * @return Boolean value.
     */
    public boolean getBoolean() {
        return arr[pos++] != 0;
    }

    /**
     * Reads double value.
     *
     * @return Double value.
     */
    public double getDouble() {
        long val = (((long)arr[pos++]) << 56) |
                (((long)arr[pos++]) & 0xFF) << 48 |
                (((long)arr[pos++]) & 0xFF) << 40 |
                (((long)arr[pos++]) & 0xFF) << 32 |
                (((long)arr[pos++]) & 0xFF) << 24 |
                (((long)arr[pos++]) & 0xFF) << 16 |
                (((long)arr[pos++]) & 0xFF) << 8 |
                (((long)arr[pos++]) & 0xFF);

        return Double.longBitsToDouble(val);
    }

    /**
     * Reads integer value in varInt format.
     *
     * @return Integer value.
     */
    public int getVarInt() {
        int b, i = 0, res = 0;

        do {
            b = arr[pos++] & 0xFF;

            res |= ((b & 0x7F) << (7 * i++));
        }
        while ((b & 0x80) != 0);

        return res;
    }

    /**
     * Reads long value in varInt format.
     *
     * @return Long value.
     */
    public long getVarLong() {
        int i = 0;

        long b, res = 0;

        do {
            b = arr[pos++] & 0xFF;

            res |= ((b & 0x7F) << (7 * i++));
        }
        while ((b & 0x80) != 0);

        return res;
    }
}
