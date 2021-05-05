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

package org.apache.ignite.internal.schema;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.util.Arrays;

/**
 * A simple byte array wrapper to allow dynamic byte array expansion during the row construction. Grows exponentially
 * up to 1 MB, then expands by 1 MB each time an expansion is required. Values are always written in LITTLE_ENDIAN
 * format.
 * <p>
 * Additionally, it tracks the high watermark of the values ever written to the buffer so that only written bytes are
 * returned from the {@link #toArray()} method. If the current (expanded) buffer size does not match the high watermark,
 * the {@link #toArray()} method will return a smaller copy of the array to exactly match the watermark.
 * <p>
 * All write methods have an absolute position. The buffer will automatically expand to fit the value being written. If
 * there is a gap between previously written values and the current value, it will be filled with zero bytes:
 * <pre>
 * ExpandableByteBuf b = new ExpandableByteBuf(1);
 * b.put(0, (byte)1); // Does not expand.
 * b.put(5, (byte)1); // Expands, meaningful bytes are [0..5]
 *
 * byte[] data = b.toArray(); // data.length == 6
 * </pre>
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class ExpandableByteBuf {
    /** */
    private static final int MB = 1024 * 1024;

    /** */
    private byte[] arr;

    /** */
    private ByteBuffer buf;

    /** */
    private int len;

    /**
     * @param size Start buffer size.
     */
    public ExpandableByteBuf(int size) {
        if (size <= 0)
            size = 32;

        arr = new byte[size];
        buf = ByteBuffer.wrap(arr);
        buf.order(ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Writes {@code byte} value to the buffer.
     *
     * @param off Buffer offset.
     * @param val Value.
     */
    public void put(int off, byte val) {
        ensureCapacity(off + 1);

        buf.put(off, val);
    }

    /**
     * Writes {@code short} value to the buffer.
     *
     * @param off Buffer offset.
     * @param val Value.
     */
    public void putShort(int off, short val) {
        ensureCapacity(off + 2);

        buf.putShort(off, val);
    }

    /**
     * Writes {@code int} value to the buffer.
     *
     * @param off Buffer offset.
     * @param val Value.
     */
    public void putInt(int off, int val) {
        ensureCapacity(off + 4);

        buf.putInt(off, val);
    }

    /**
     * Writes {@code float} value to the buffer.
     *
     * @param off Buffer offset.
     * @param val Value.
     */
    public void putFloat(int off, float val) {
        ensureCapacity(off + 4);

        buf.putFloat(off, val);
    }

    /**
     * Writes {@code long} value to the buffer.
     *
     * @param off Buffer offset.
     * @param val Value.
     */
    public void putLong(int off, long val) {
        ensureCapacity(off + 8);

        buf.putLong(off, val);
    }

    /**
     * Writes {@code double} value to the buffer.
     *
     * @param off Buffer offset.
     * @param val Value.
     */
    public void putDouble(int off, double val) {
        ensureCapacity(off + 8);

        buf.putDouble(off, val);
    }

    /**
     * Writes {@code byte[]} value to the buffer.
     *
     * @param off Buffer offset.
     * @param val Value.
     */
    public void putBytes(int off, byte[] val) {
        ensureCapacity(off + val.length);

        buf.position(off);

        try {
            buf.put(val);
        }
        finally {
            buf.position(0);
        }
    }

    /**
     * Writes {@code String} value to the buffer.
     *
     * @param off Buffer offset.
     * @param val Value.
     * @param encoder Charset encoder.
     * @return Bytes written.
     * @throws CharacterCodingException If encoding failed.
     */
    public int putString(int off, String val, CharsetEncoder encoder) throws CharacterCodingException {
        ensureCapacity(off);

        encoder.reset();

        buf.position(off);

        try {
            CharBuffer valBuf = CharBuffer.wrap(val);

            while (true) {
                CoderResult cr = encoder.encode(valBuf, buf, true);

                len = buf.position();

                if (cr.isUnderflow())
                    break;

                if (cr.isOverflow()) {
                    expand(len + 1);

                    continue;
                }

                if (cr.isError())
                    cr.throwException();

            }

            while (true) {
                CoderResult cr = encoder.flush(buf);

                len = buf.position();

                if (cr.isOverflow()) {
                    expand(len + 1);

                    continue;
                }

                if (cr.isUnderflow())
                    break;

                if (cr.isError())
                    cr.throwException();
            }

            return len - off;
        }
        finally {
            buf.position(0);
        }
    }

    /**
     * Reads {@code byte} value from buffer.
     *
     * @param off Buffer offset.
     * @return Value.
     */
    public byte get(int off) {
        return buf.get(off);
    }

    /**
     * @return The byte array of all bytes written to this array, including gaps.
     */
    public byte[] toArray() {
        if (arr.length == len)
            return arr;
        else
            return Arrays.copyOf(arr, len);
    }

    /**
     * If the current capacity is smaller than {@code cap}, will expand the buffer size.
     *
     * @param cap Target capacity.
     */
    void ensureCapacity(int cap) {
        if (arr.length < cap)
            expand(cap);

        if (cap > len)
            len = cap;
    }

    /**
     * @param cap Capacity to expand.
     */
    private void expand(int cap) {
        int l = arr.length;

        while (l < cap) {
            if (l < MB)
                l *= 2;
            else
                l += MB;
        }

        byte[] tmp = new byte[cap];

        System.arraycopy(arr, 0, tmp, 0, arr.length);

        arr = tmp;
        int oldPos = buf.position();
        buf = ByteBuffer.wrap(arr);
        buf.position(oldPos);
        buf.order(ByteOrder.LITTLE_ENDIAN);
    }
}
