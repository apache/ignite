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
 * <p>Supports expandable byte buffer with varInt put operations for {@code int} and {@code long} types.</p> VarInt
 * encoding allows to write integer values with large number of leading zeros in less than 4 bytes.
 * See https://en.wikipedia.org/wiki/Variable-length_quantity
 * <p>Each put operation moves position forward accordingly to the written type size.</p>
 */
public class VarIntWriter {
    /** Backing byte array. */
    private byte[] arr;

    /** Current position. */
    private int pos;

    /**
     * Constructor.
     *
     * @param initCap Initial backing byte array capacity.
     */
    public VarIntWriter(int initCap) {
        arr = new byte[initCap];
    }

    /**
     * Returns current position in backing byte array.
     */
    public int position() {
        return pos;
    }

    /**
     * Resets current position in backing byte array to {@code 0}.
     */
    public void reset() {
        pos = 0;
    }

    /**
     * Puts boolean value.
     *
     * @param val Value.
     */
    public void putBoolean(boolean val) {
        ensureCapacity(1);

        arr[pos++] = (byte)(val ? 1 : 0);
    }

    /**
     * Puts double value.
     *
     * @param val Value.
     */
    public void putDouble(double val) {
        ensureCapacity(Double.BYTES);

        long v = Double.doubleToLongBits(val);

        arr[pos++] = (byte)(v >> 56);
        arr[pos++] = (byte)(v >> 48);
        arr[pos++] = (byte)(v >> 40);
        arr[pos++] = (byte)(v >> 32);
        arr[pos++] = (byte)(v >> 24);
        arr[pos++] = (byte)(v >> 16);
        arr[pos++] = (byte)(v >> 8);
        arr[pos++] = (byte)(v);
    }

    /**
     * Puts integer value in varInt format. It can consume from 1 to 5 bytes.
     *
     * @param val Value.
     */
    public void putVarInt(int val) {
        if ((val & (~0 << 7)) == 0) {
            ensureCapacity(1);

            arr[pos++] = (byte)val;
        }
        else if ((val & (~0 << 14)) == 0) {
            ensureCapacity(2);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 7);
        }
        else if ((val & (~0 << 21)) == 0) {
            ensureCapacity(3);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 7) & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 14);
        }
        else if ((val & (~0 << 28)) == 0) {
            ensureCapacity(4);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 7) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 14) & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 21);
        }
        else {
            ensureCapacity(5);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 7) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 14) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 21) & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 28);
        }
    }

    /**
     * Puts long value in varInt format. It can consume form 1 to 10 bytes.
     *
     * @param val Value.
     */
    public void putVarLong(long val) {
        if ((val & (~0L << 7)) == 0) {
            ensureCapacity(1);

            arr[pos++] = (byte)val;
        }
        else if ((val & (~0L << 14)) == 0) {
            ensureCapacity(2);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 7);
        }
        else if ((val & (~0L << 21)) == 0) {
            ensureCapacity(3);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 7) & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 14);
        }
        else if ((val & (~0L << 28)) == 0) {
            ensureCapacity(4);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 7) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 14) & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 21);
        }
        else if ((val & (~0L << 35)) == 0) {
            ensureCapacity(5);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 7) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 14) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 21) & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 28);
        }
        else if ((val & (~0L << 42)) == 0) {
            ensureCapacity(6);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 7) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 14) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 21) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 28) & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 35);
        }
        else if ((val & (~0L << 49)) == 0) {
            ensureCapacity(7);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 7) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 14) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 21) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 28) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 35) & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 42);
        }
        else if ((val & (~0L << 56)) == 0) {
            ensureCapacity(8);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 7) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 14) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 21) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 28) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 35) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 42) & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 49);
        }
        else if ((val & (~0L << 63)) == 0) {
            ensureCapacity(9);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 7) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 14) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 21) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 28) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 35) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 42) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 49) & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 56);
        }
        else {
            ensureCapacity(10);

            arr[pos++] = (byte)((val & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 7) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 14) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 21) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 28) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 35) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 42) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 49) & 0x7F) | 0x80);

            arr[pos++] = (byte)(((val >>> 56) & 0x7F) | 0x80);

            arr[pos++] = (byte)(val >>> 63);
        }
    }

    /**
     * Copies data from backing byte array to target array from beaning of backing array to the current position.
     *
     * @param arr Target byte array.
     * @param off Target byte array offset.
     */
    public void toBytes(byte[] arr, int off) {
        System.arraycopy(this.arr, 0, arr, off, pos);
    }

    /**
     * Doubles size of backing byte array in case of array capacity isn't enough.
     *
     * @param len Length.
     */
    private void ensureCapacity(int len) {
        if (arr.length - pos < len) {
            byte[] tmp = new byte[Math.max(arr.length * 2, arr.length + len)];

            System.arraycopy(arr, 0, tmp, 0, pos);

            arr = tmp;
        }
    }
}
