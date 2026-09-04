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

package org.apache.ignite.internal.binary;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import org.apache.ignite.IgniteCommonsSystemProperties;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.binary.BinaryWriterExImpl.ZERO_COPY;

/**
 * Writes {@link String} values to a {@link BinaryOutputStream} in UTF-8 without allocation of temporary byte arrays.
 *
 * @see IgniteCommonsSystemProperties#IGNITE_BINARY_STRING_ZERO_COPY
 */
public final class StringWriter {
    /** Latin-1 value of the {@code java.lang.String#coder} field. */
    private static final byte LATIN1 = 0;

    /** Offset of the {@code java.lang.String#value} field, or {@code -1} if the compact string fast path is unavailable. */
    private static final long STR_VALUE_OFF;

    /** Offset of the {@code java.lang.String#coder} field, or {@code -1} if the compact string fast path is unavailable. */
    private static final long STR_CODER_OFF;

    static {
        IgniteBiTuple<Long, Long> result = fieldsOffsets();

        STR_VALUE_OFF = result.get1();
        STR_CODER_OFF = result.get2();
    }

    /**
     * Handle of the intrinsified {@code java.lang.StringCoding#hasNegatives}, or {@code null} if unavailable.
     * The intrinsic scans the array with SIMD instructions, far faster than any scalar loop.
     */
    private static final MethodHandle HAS_NEGATIVES = hasNegatives();

    /** */
    private StringWriter() {
        // No-op.
    }

    /**
     * Writes a string to the output stream.
     *
     * @param val Value.
     * @param out Output stream.
     */
    public static void write(@NotNull String val, BinaryOutputStream out) {
        // 1 byte for `GridBinaryMarshaller.STRING` and integer (4 bytes) for length.
        out.unsafeEnsure(1 + 4);
        out.unsafeWriteByte(GridBinaryMarshaller.STRING);

        int lenPos = out.position();

        out.unsafePosition(out.position() + 4);

        int written;

        byte[] latin1 = latin1Value(val);

        if (latin1 != null) {
            if (out.hasArray()) {
                if (!hasNegatives(latin1)) {
                    out.unsafeEnsure(latin1.length);
                    // Pure ASCII: UTF-8 representation matches the internal array, copy it as-is.
                    System.arraycopy(latin1, 0, out.array(), out.position(), latin1.length);

                    written = latin1.length;
                }
                else
                    written = encodeLatin1(latin1, out);

                out.unsafePosition(out.position() + written);
            }
            else
                written = writeLatin1(latin1, out);
        }
        else {
            // Allocating memory for worst case - 3 bytes per char.
            out.unsafeEnsure(Math.multiplyExact(3, val.length()));

            if (out.hasArray()) {
                written = encodeChars(val, out);

                out.unsafePosition(out.position() + written);
            }
            else
                written = writeChars(val, out);
        }

        out.unsafeWriteInt(lenPos, written);
    }

    /**
     * Writes a Latin-1 encoded string value to the stream.
     *
     * @param val Internal Latin-1 array of the string.
     * @param out Output stream.
     * @return Number of bytes written.
     */
    private static int writeLatin1(byte[] val, BinaryOutputStream out) {
        out.unsafeEnsure(Math.addExact(val.length, val.length));

        int utfLen = 0;

        for (int i = 0; i < val.length; i++) {
            byte b = val[i];

            if (b >= 0) {
                out.unsafeWriteByte(b);

                utfLen++;
            }
            else {
                int c = b & 0b1111_1111;

                out.unsafeWriteByte((byte)(0b1100_0000 | (c >> 6)));
                out.unsafeWriteByte((byte)(0b1000_0000 | (c & 0b0011_1111)));

                utfLen += 2;
            }
        }

        return utfLen;
    }

    /**
     * Encodes a Latin-1 string value to the buffer as UTF-8.
     *
     * @param val Internal Latin-1 array of the string.
     * @param out Output stream.
     * @return Count of written bytes.
     */
    private static int encodeLatin1(byte[] val, BinaryOutputStream out) {
        out.unsafeEnsure(Math.addExact(val.length, val.length));

        byte[] buf = out.array();

        long off = out.position() + GridUnsafe.BYTE_ARR_OFF;

        for (int i = 0; i < val.length; i++) {
            byte b = val[i];

            if (b >= 0)
                GridUnsafe.putByte(buf, off++, b);
            else {
                int c = b & 0xFF;

                GridUnsafe.putByte(buf, off++, (byte)(0b1100_0000 | (c >> 6)));
                GridUnsafe.putByte(buf, off++, (byte)(0b1000_0000 | (c & 0b0011_1111)));
            }
        }

        return (int)(off - GridUnsafe.BYTE_ARR_OFF - out.position());
    }

    /**
     * Writes string chars UTF-8 encoded to the stream. Replicates {@code String#getBytes(UTF_8)} behavior exactly,
     * including replacement of malformed surrogates with {@code '?'}. Stream capacity must be ensured by the caller.
     *
     * @param val Value.
     * @param out Output stream.
     * @return Number of bytes written.
     */
    private static int writeChars(String val, BinaryOutputStream out) {
        int len = val.length();
        int utfLen = 0;

        for (int i = 0; i < len; i++) {
            char c = val.charAt(i);

            if (c < 0x80) {
                out.unsafeWriteByte((byte)c);

                utfLen++;
            }
            else if (c < 0x800) {
                out.unsafeWriteByte((byte)(0b11_000000 | (c >> 6)));
                out.unsafeWriteByte((byte)(0b10_000000 | (c & 0b00_111111)));

                utfLen += 2;
            }
            else if (!Character.isSurrogate(c)) {
                out.unsafeWriteByte((byte)(0b1110_0000 | (c >> 12)));
                out.unsafeWriteByte((byte)(0b1000_0000 | ((c >> 6) & 0b0011_1111)));
                out.unsafeWriteByte((byte)(0b1000_0000 | (c & 0b0011_1111)));

                utfLen += 3;
            }
            else {
                char c2;

                if (Character.isHighSurrogate(c) && i + 1 < len && Character.isLowSurrogate(c2 = val.charAt(i + 1))) {
                    int cp = Character.toCodePoint(c, c2);

                    out.unsafeWriteByte((byte)(0b1111_0000 | (cp >> 18)));
                    out.unsafeWriteByte((byte)(0b1000_0000 | ((cp >> 12) & 0b0011_1111)));
                    out.unsafeWriteByte((byte)(0b1000_0000 | ((cp >> 6) & 0b0011_1111)));
                    out.unsafeWriteByte((byte)(0b1000_0000 | (cp & 0b0011_1111)));

                    utfLen += 4;
                    i++;
                }
                else {
                    out.unsafeWriteByte((byte)'?');

                    utfLen++;
                }
            }
        }

        return utfLen;
    }

    /**
     * Encodes string chars to the buffer as UTF-8. Replicates {@code String#getBytes(UTF_8)} behavior exactly,
     * including replacement of malformed surrogates with {@code '?'}. Buffer capacity must be ensured by the caller.
     *
     * @param val Value.
     * @param out Output stream.
     * @return Count of written bytes.
     */
    private static int encodeChars(String val, BinaryOutputStream out) {
        byte[] buf = out.array();
        int len = val.length();

        // Unsafe writes skip the array bounds checks: capacity is ensured by the caller.
        long off = GridUnsafe.BYTE_ARR_OFF + out.position();

        for (int i = 0; i < len; i++) {
            char c = val.charAt(i);

            if (c < 0x80)
                GridUnsafe.putByte(buf, off++, (byte)c);
            else if (c < 0x800) {
                GridUnsafe.putByte(buf, off++, (byte)(0b1100_0000 | (c >> 6)));
                GridUnsafe.putByte(buf, off++, (byte)(0b1000_0000 | (c & 0b0011_1111)));
            }
            else if (!Character.isSurrogate(c)) {
                GridUnsafe.putByte(buf, off++, (byte)(0b1110_0000 | (c >> 12)));
                GridUnsafe.putByte(buf, off++, (byte)(0b1000_0000 | ((c >> 6) & 0b0011_1111)));
                GridUnsafe.putByte(buf, off++, (byte)(0b1000_0000 | (c & 0b0011_1111)));
            }
            else {
                char c2;

                if (Character.isHighSurrogate(c) && i + 1 < len && Character.isLowSurrogate(c2 = val.charAt(i + 1))) {
                    int cp = Character.toCodePoint(c, c2);

                    GridUnsafe.putByte(buf, off++, (byte)(0b1111_0000 | (cp >> 18)));
                    GridUnsafe.putByte(buf, off++, (byte)(0b1000_0000 | ((cp >> 12) & 0b0011_1111)));
                    GridUnsafe.putByte(buf, off++, (byte)(0b1000_0000 | ((cp >> 6) & 0b0011_1111)));
                    GridUnsafe.putByte(buf, off++, (byte)(0b1000_0000 | (cp & 0b0011_1111)));

                    i++;
                }
                else
                    GridUnsafe.putByte(buf, off++, (byte)'?');
            }
        }

        return (int)(off - GridUnsafe.BYTE_ARR_OFF - out.position());
    }

    /**
     * @param val String.
     * @return Internal Latin-1 array of the string,
     *      or {@code null} if the string is UTF-16 encoded or the internal layout of {@link String} is unknown.
     */
    public static byte[] latin1Value(String val) {
        if (STR_VALUE_OFF < 0 || GridUnsafe.getByteField(val, STR_CODER_OFF) != LATIN1)
            return null;

        return (byte[])GridUnsafe.getObjectField(val, STR_VALUE_OFF);
    }

    /**
     * @param arr Array.
     * @return {@code True} if the array contains a byte with the sign bit set.
     */
    public static boolean hasNegatives(byte[] arr) {
        if (HAS_NEGATIVES != null) {
            try {
                return (boolean)HAS_NEGATIVES.invokeExact(arr, 0, arr.length);
            }
            catch (Throwable ignored) {
                // Fall through to the generic implementation.
            }
        }

        // 8-byte strides with an early exit.
        int i = 0;

        for (int lim = arr.length - Long.BYTES; i <= lim; i += Long.BYTES) {
            long hasNegatives = GridUnsafe.getLong(arr, GridUnsafe.BYTE_ARR_OFF + i)
                & 0b10000000_10000000_10000000_10000000_10000000_10000000_10000000_10000000L;

            if (hasNegatives != 0)
                return true;
        }

        for (; i < arr.length; i++) {
            if (arr[i] < 0)
                return true;
        }

        return false;
    }

    /** */
    private static IgniteBiTuple<Long, Long> fieldsOffsets() {
        if (ZERO_COPY) {
            try {
                Field valField = String.class.getDeclaredField("value");
                Field coderField = String.class.getDeclaredField("coder");

                // On JDK 8 the value field is a char[], only the generic encoder can be used.
                if (valField.getType() == byte[].class && coderField.getType() == byte.class) {
                    return new IgniteBiTuple<>(
                        GridUnsafe.objectFieldOffset(valField),
                        GridUnsafe.objectFieldOffset(coderField)
                    );
                }
            }
            catch (Throwable ignored) {
                // No-op.
            }
        }

        return new IgniteBiTuple<>(-1L, -1L);
    }

    /** */
    private static @Nullable MethodHandle hasNegatives() {
        if (ZERO_COPY) {
            try {
                Method mtd = Class.forName("java.lang.StringCoding").getDeclaredMethod("hasNegatives", byte[].class, int.class, int.class);

                mtd.setAccessible(true);

                return MethodHandles.lookup().unreflect(mtd);
            }
            catch (Throwable ignored) {
                // No-op.
            }
        }

        return null;
    }
}
