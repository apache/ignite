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
import org.jetbrains.annotations.NotNull;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.binary.BinaryWriterExImpl.ZERO_COPY;

/**
 * Writes {@link String} values to a {@link BinaryOutputStream} in UTF-8 without allocation of temporary byte arrays:
 * UTF-8 bytes are encoded directly into the stream buffer after a reserved length slot which is patched afterwards.
 * <p>
 * On JDKs with compact strings (9+) an additional fast path is used: for Latin-1 strings the internal {@code byte[]}
 * value of the string is encoded without per-char conversion, and for pure ASCII strings it is copied into the stream
 * as-is, since the UTF-8 representation is identical to the internal one.
 * <p>
 * The produced bytes are identical to serialization of {@code val.getBytes(UTF_8)}, including replacement of
 * malformed surrogates with {@code '?'}. Zero-copy serialization can be disabled with the
 * {@link IgniteCommonsSystemProperties#IGNITE_BINARY_STRING_ZERO_COPY} system property.
 */
public final class StringWriter {
    /** Latin-1 value of the {@code java.lang.String#coder} field. */
    private static final byte LATIN1 = 0;

    /** Mask to test 8 bytes for a set sign bit at once. */
    private static final long NEGATIVE_BYTES_MSK = 0b10000000_10000000_10000000_10000000_10000000_10000000_10000000_10000000L;

    /** Offset of the {@code java.lang.String#value} field, or {@code -1} if the compact string fast path is unavailable. */
    private static final long STR_VALUE_OFF;

    /** Offset of the {@code java.lang.String#coder} field, or {@code -1} if the compact string fast path is unavailable. */
    private static final long STR_CODER_OFF;

    /**
     * Handle of the intrinsified {@code java.lang.StringCoding#hasNegatives}, or {@code null} if unavailable.
     * The intrinsic scans the array with SIMD instructions, far faster than any scalar loop.
     */
    private static final MethodHandle HAS_NEGATIVES;

    static {
        MethodHandle hasNegatives = null;

        if (ZERO_COPY) {
            try {
                Method mtd = Class.forName("java.lang.StringCoding").getDeclaredMethod("hasNegatives", byte[].class, int.class, int.class);

                // Requires '--add-opens=java.base/java.lang=ALL-UNNAMED' which Ignite scripts pass by default.
                mtd.setAccessible(true);

                MethodHandle hasNegatives0 = MethodHandles.lookup().unreflect(mtd);

                if (!(boolean)hasNegatives0.invokeExact(new byte[] {1, 2, 3}, 0, 3)
                    && (boolean)hasNegatives0.invokeExact(new byte[] {1, -2, 3}, 0, 3))
                    hasNegatives = hasNegatives0;
            }
            catch (Throwable ignored) {
                // The scalar implementation will be used.
            }
        }

        HAS_NEGATIVES = hasNegatives;

        long valOff = -1;
        long coderOff = -1;

        if (ZERO_COPY) {
            try {
                Field valField = String.class.getDeclaredField("value");
                Field coderField = String.class.getDeclaredField("coder");

                // On JDK 8 the value field is a char[], only the generic encoder can be used.
                if (valField.getType() == byte[].class && coderField.getType() == byte.class) {
                    valOff = GridUnsafe.objectFieldOffset(valField);
                    coderOff = GridUnsafe.objectFieldOffset(coderField);

                    if (!probe(valOff, coderOff)) {
                        valOff = -1;
                        coderOff = -1;
                    }
                }
            }
            catch (Throwable ignored) {
                valOff = -1;
                coderOff = -1;
            }
        }

        STR_VALUE_OFF = valOff;
        STR_CODER_OFF = coderOff;
    }

    /** */
    private StringWriter() {
        // No-op.
    }

    /**
     * Writes a string to the output stream as a {@link GridBinaryMarshaller#STRING} flag followed by UTF-8 length
     * (int) and UTF-8 bytes.
     *
     * @param val Value.
     * @param out Output stream.
     */
    public static void write(@NotNull String val, BinaryOutputStream out) {
        int lenPos = writeHeader(out);

        int writtenBytes;

        byte[] latin1 = latin1Value(val);

        if (latin1 != null) {
            if (out.hasArray()) {
                // Encode into the backing array directly: plain indexed writes are much faster than
                // per-byte virtual calls through the stream interface.
                int start = lenPos + 4;
                int end = encodeLatin1(latin1, out, start);

                writtenBytes = end - start;

                out.unsafePosition(end);
            }
            else
                writtenBytes = writeLatin1(latin1, out);
        }
        else {
            // Worst case is 3 bytes per char: a surrogate pair (2 chars) produces 4 bytes, a lone surrogate 1 byte.
            out.unsafeEnsure(Math.multiplyExact(3, val.length()));

            if (out.hasArray()) {
                // Encode into the backing array directly: plain indexed writes are much faster than
                // per-byte virtual calls through the stream interface.
                int start = lenPos + 4;
                int end = encodeChars(val, out.array(), start);

                writtenBytes = end - start;

                out.unsafePosition(end);
            } else
                writtenBytes = writeChars(val, out);
        }

        out.unsafeWriteInt(lenPos, writtenBytes);
    }

    /**
     * Checks that the internal layout of {@link String} behaves as the compact string fast path expects.
     *
     * @param valOff Offset of the {@code value} field.
     * @param coderOff Offset of the {@code coder} field.
     * @return {@code True} if the fast path can be used.
     */
    private static boolean probe(long valOff, long coderOff) {
        String probe = "Ignite\u00e9";

        // Compact strings can be disabled with -XX:-CompactStrings, then all strings are UTF-16 encoded.
        if (GridUnsafe.getByteField(probe, coderOff) != LATIN1)
            return false;

        Object val = GridUnsafe.getObjectField(probe, valOff);

        if (!(val instanceof byte[]))
            return false;

        byte[] arr = (byte[])val;

        if (arr.length != probe.length())
            return false;

        for (int i = 0; i < arr.length; i++) {
            if ((arr[i] & 0xFF) != probe.charAt(i))
                return false;
        }

        return true;
    }

    /**
     * @param val String.
     * @return Internal Latin-1 array of the string, or {@code null} if the string is UTF-16 encoded or the internal
     *      layout of {@link String} is unknown.
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
    private static boolean hasNegatives(byte[] arr) {
        if (HAS_NEGATIVES != null) {
            try {
                return (boolean)HAS_NEGATIVES.invokeExact(arr, 0, arr.length);
            }
            catch (Throwable ignored) {
                // TODO LT log here.
                // Fall through to the generic implementation.
            }
        }

        // 8-byte strides with an early exit: measured on par with a branch-free loop for clean arrays
        // (the exit branch is never taken there, and neither variant is vectorized by the JIT)
        // and far faster when a negative byte occurs early.
        int i = 0;

        for (int lim = arr.length - Long.BYTES; i <= lim; i += Long.BYTES) {
            if ((GridUnsafe.getLong(arr, GridUnsafe.BYTE_ARR_OFF + i) & NEGATIVE_BYTES_MSK) != 0)
                return true;
        }

        for (; i < arr.length; i++) {
            if (arr[i] < 0)
                return true;
        }

        return false;
    }

    /**
     * Writes a Latin-1 encoded string value to the stream.
     *
     * @param val Internal Latin-1 array of the string.
     * @param out Output stream.
     * @return Number of bytes written.
     */
    private static int writeLatin1(byte[] val, BinaryOutputStream out) {
        if (!hasNegatives(val)) {
            // Pure ASCII: UTF-8 representation matches the internal array, copy it as-is.
            out.writeByteArray(val);

            return val.length;
        }

        out.unsafeEnsure(Math.addExact(val.length, val.length));

        int utfLen = 0;

        for (int i = 0; i < val.length; i++) {
            byte b = val[i];

            if (b >= 0) {
                out.unsafeWriteByte(b);

                utfLen++;
            }
            else {
                int c = b & 0xFF;

                out.unsafeWriteByte((byte)(0xC0 | (c >> 6)));
                out.unsafeWriteByte((byte)(0x80 | (c & 0x3F)));

                utfLen += 2;
            }
        }

        return utfLen;
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
                out.unsafeWriteByte((byte)(0xC0 | (c >> 6)));
                out.unsafeWriteByte((byte)(0x80 | (c & 0x3F)));

                utfLen += 2;
            }
            else if (Character.isSurrogate(c)) {
                char c2;

                if (Character.isHighSurrogate(c) && i + 1 < len && Character.isLowSurrogate(c2 = val.charAt(i + 1))) {
                    int cp = Character.toCodePoint(c, c2);

                    out.unsafeWriteByte((byte)(0xF0 | (cp >> 18)));
                    out.unsafeWriteByte((byte)(0x80 | ((cp >> 12) & 0x3F)));
                    out.unsafeWriteByte((byte)(0x80 | ((cp >> 6) & 0x3F)));
                    out.unsafeWriteByte((byte)(0x80 | (cp & 0x3F)));

                    utfLen += 4;
                    i++;
                }
                else {
                    out.unsafeWriteByte((byte)'?');

                    utfLen++;
                }
            }
            else {
                out.unsafeWriteByte((byte)(0xE0 | (c >> 12)));
                out.unsafeWriteByte((byte)(0x80 | ((c >> 6) & 0x3F)));
                out.unsafeWriteByte((byte)(0x80 | (c & 0x3F)));

                utfLen += 3;
            }
        }

        return utfLen;
    }

    /**
     * Encodes a Latin-1 string value to the buffer as UTF-8.
     *
     * @param val Internal Latin-1 array of the string.
     * @param out Output stream.
     * @param pos Buffer position to encode to.
     * @return Buffer position after the last encoded byte.
     */
    private static int encodeLatin1(byte[] val, BinaryOutputStream out, int pos) {
        if (!hasNegatives(val)) {
            out.unsafeEnsure(val.length);

            // Pure ASCII: UTF-8 representation matches the internal array, copy it as-is.
            System.arraycopy(val, 0, out.array(), pos, val.length);

            return pos + val.length;
        }

        out.unsafeEnsure(Math.addExact(val.length, val.length));

        byte[] buf = out.array();

        long off = GridUnsafe.BYTE_ARR_OFF + pos;

        for (int i = 0; i < val.length; i++) {
            byte b = val[i];

            if (b >= 0)
                GridUnsafe.putByte(buf, off++, b);
            else {
                int c = b & 0xFF;

                GridUnsafe.putByte(buf, off++, (byte)(0xC0 | (c >> 6)));
                GridUnsafe.putByte(buf, off++, (byte)(0x80 | (c & 0x3F)));
            }
        }

        return (int)(off - GridUnsafe.BYTE_ARR_OFF);
    }

    /**
     * Encodes string chars to the buffer as UTF-8. Replicates {@code String#getBytes(UTF_8)} behavior exactly,
     * including replacement of malformed surrogates with {@code '?'}. Buffer capacity must be ensured by the caller.
     *
     * @param val Value.
     * @param buf Buffer.
     * @param pos Buffer position to encode to.
     * @return Buffer position after the last encoded byte.
     */
    private static int encodeChars(String val, byte[] buf, int pos) {
        int len = val.length();

        // Unsafe writes skip the array bounds checks: capacity is ensured by the caller.
        long off = GridUnsafe.BYTE_ARR_OFF + pos;

        for (int i = 0; i < len; i++) {
            char c = val.charAt(i);

            if (c < 0x80)
                GridUnsafe.putByte(buf, off++, (byte)c);
            else if (c < 0x800) {
                GridUnsafe.putByte(buf, off++, (byte)(0xC0 | (c >> 6)));
                GridUnsafe.putByte(buf, off++, (byte)(0x80 | (c & 0x3F)));
            }
            else if (Character.isSurrogate(c)) {
                char c2;

                if (Character.isHighSurrogate(c) && i + 1 < len && Character.isLowSurrogate(c2 = val.charAt(i + 1))) {
                    int cp = Character.toCodePoint(c, c2);

                    GridUnsafe.putByte(buf, off++, (byte)(0xF0 | (cp >> 18)));
                    GridUnsafe.putByte(buf, off++, (byte)(0x80 | ((cp >> 12) & 0x3F)));
                    GridUnsafe.putByte(buf, off++, (byte)(0x80 | ((cp >> 6) & 0x3F)));
                    GridUnsafe.putByte(buf, off++, (byte)(0x80 | (cp & 0x3F)));

                    i++;
                }
                else
                    GridUnsafe.putByte(buf, off++, (byte)'?');
            }
            else {
                GridUnsafe.putByte(buf, off++, (byte)(0xE0 | (c >> 12)));
                GridUnsafe.putByte(buf, off++, (byte)(0x80 | ((c >> 6) & 0x3F)));
                GridUnsafe.putByte(buf, off++, (byte)(0x80 | (c & 0x3F)));
            }
        }

        return (int)(off - GridUnsafe.BYTE_ARR_OFF);
    }

    /**
     * Writes a string through a temporary UTF-8 byte array.
     *
     * @param val Value.
     * @param out Output stream.
     */
    static int writeWithTemporaryArray(String val, BinaryOutputStream out) {
        byte[] strArr;

        if (BinaryUtils.USE_STR_SERIALIZATION_VER_2)
            strArr = BinaryUtils.strToUtf8Bytes(val);
        else
            strArr = val.getBytes(UTF_8);

        out.writeByteArray(strArr);

        return strArr.length;
    }

    /** */
    private static int writeHeader(BinaryOutputStream out) {
        // 1 byte for `GridBinaryMarshaller.STRING` and integer (4 bytes) for length.
        out.unsafeEnsure(1 + 4);
        out.unsafeWriteByte(GridBinaryMarshaller.STRING);

        int pos = out.position();

        out.unsafePosition(out.position() + 4);

        return pos;
    }
}
