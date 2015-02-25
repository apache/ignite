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

package org.apache.ignite.internal.util;


import org.apache.ignite.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.zip.*;

/**
 * Collection of utility methods used throughout the system.
 */
public class IgniteByteUtils {
    /**
     * Writes collection of byte arrays to data output.
     *
     * @param out Output to write to.
     * @param bytes Collection with byte arrays.
     * @throws java.io.IOException If write failed.
     */
    public static void writeBytesCollection(DataOutput out, Collection<byte[]> bytes) throws IOException {
        if (bytes != null) {
            out.writeInt(bytes.size());

            for (byte[] b : bytes)
                writeByteArray(out, b);
        }
        else
            out.writeInt(-1);
    }

    /**
     * Reads collection of byte arrays from data input.
     *
     * @param in Data input to read from.
     * @return List of byte arrays.
     * @throws java.io.IOException If read failed.
     */
    public static List<byte[]> readBytesList(DataInput in) throws IOException {
        int size = in.readInt();

        if (size < 0)
            return null;

        List<byte[]> res = new ArrayList<>(size);

        for (int i = 0; i < size; i++)
            res.add(readByteArray(in));

        return res;
    }

    /**
     * Writes byte array to output stream accounting for <tt>null</tt> values.
     *
     * @param out Output stream to write to.
     * @param arr Array to write, possibly <tt>null</tt>.
     * @throws java.io.IOException If write failed.
     */
    public static void writeByteArray(DataOutput out, @Nullable byte[] arr) throws IOException {
        if (arr == null)
            out.writeInt(-1);
        else {
            out.writeInt(arr.length);

            out.write(arr);
        }
    }

    /**
     * Writes byte array to output stream accounting for <tt>null</tt> values.
     *
     * @param out Output stream to write to.
     * @param arr Array to write, possibly <tt>null</tt>.
     * @throws java.io.IOException If write failed.
     */
    public static void writeByteArray(DataOutput out, @Nullable byte[] arr, int maxLen) throws IOException {
        if (arr == null)
            out.writeInt(-1);
        else {
            int len = Math.min(arr.length, maxLen);

            out.writeInt(len);

            out.write(arr, 0, len);
        }
    }

    /**
     * Reads byte array from input stream accounting for <tt>null</tt> values.
     *
     * @param in Stream to read from.
     * @return Read byte array, possibly <tt>null</tt>.
     * @throws java.io.IOException If read failed.
     */
    @Nullable public static byte[] readByteArray(DataInput in) throws IOException {
        int len = in.readInt();

        if (len == -1)
            return null; // Value "-1" indicates null.

        byte[] res = new byte[len];

        in.readFully(res);

        return res;
    }

    /**
     * Reads byte array from given buffers (changing buffer positions).
     *
     * @param bufs Byte buffers.
     * @return Byte array.
     */
    public static byte[] readByteArray(ByteBuffer... bufs) {
        assert !F.isEmpty(bufs);

        int size = 0;

        for (ByteBuffer buf : bufs)
            size += buf.remaining();

        byte[] res = new byte[size];

        int off = 0;

        for (ByteBuffer buf : bufs) {
            int len = buf.remaining();

            if (len != 0) {
                buf.get(res, off, len);

                off += len;
            }
        }

        assert off == res.length;

        return res;
    }

    /**
     * // FIXME: added for DR dataCenterIds, review if it is needed after GG-6879.
     *
     * @param out Output.
     * @param col Set to write.
     * @throws java.io.IOException If write failed.
     */
    public static void writeByteCollection(DataOutput out, Collection<Byte> col) throws IOException {
        if (col != null) {
            out.writeInt(col.size());

            for (Byte i : col)
                out.writeByte(i);
        }
        else
            out.writeInt(-1);
    }

    /**
     * // FIXME: added for DR dataCenterIds, review if it is needed after GG-6879.
     *
     * @param in Input.
     * @return Deserialized list.
     * @throws java.io.IOException If deserialization failed.
     */
    @Nullable public static List<Byte> readByteList(DataInput in) throws IOException {
        int size = in.readInt();

        // Check null flag.
        if (size == -1)
            return null;

        List<Byte> col = new ArrayList<>(size);

        for (int i = 0; i < size; i++)
            col.add(in.readByte());

        return col;
    }

    /**
     * Join byte arrays into single one.
     *
     * @param bufs list of byte arrays to concatenate.
     * @return Concatenated byte's array.
     */
    public static byte[] join(byte[]... bufs) {
        int size = 0;
        for (byte[] buf : bufs) {
            size += buf.length;
        }

        byte[] res = new byte[size];
        int position = 0;
        for (byte[] buf : bufs) {
            IgniteUtils.arrayCopy(buf, 0, res, position, buf.length);
            position += buf.length;
        }

        return res;
    }

    /**
     * Converts byte array to formatted string. If calling:
     * <pre name="code" class="java">
     * ...
     * byte[] data = {10, 20, 30, 40, 50, 60, 70, 80, 90};
     *
     * U.byteArray2String(data, "0x%02X", ",0x%02X")
     * ...
     * </pre>
     * the result will be:
     * <pre name="code" class="java">
     * ...
     * 0x0A, 0x14, 0x1E, 0x28, 0x32, 0x3C, 0x46, 0x50, 0x5A
     * ...
     * </pre>
     *
     * @param arr Array of byte.
     * @param hdrFmt C-style string format for the first element.
     * @param bodyFmt C-style string format for second and following elements, if any.
     * @return String with converted bytes.
     */
    public static String byteArray2String(byte[] arr, String hdrFmt, String bodyFmt) {
        assert arr != null;
        assert hdrFmt != null;
        assert bodyFmt != null;

        SB sb = new SB();

        sb.a('{');

        boolean first = true;

        for (byte b : arr)
            if (first) {
                sb.a(String.format(hdrFmt, b));

                first = false;
            }
            else
                sb.a(String.format(bodyFmt, b));

        sb.a('}');

        return sb.toString();
    }

    /**
     * Convert string with hex values to byte array.
     *
     * @param hex Hexadecimal string to convert.
     * @return array of bytes defined as hex in string.
     * @throws IllegalArgumentException If input character differs from certain hex characters.
     */
    public static byte[] hexString2ByteArray(String hex) throws IllegalArgumentException {
        // If Hex string has odd character length.
        if (hex.length() % 2 != 0)
            hex = '0' + hex;

        char[] chars = hex.toCharArray();

        byte[] bytes = new byte[chars.length / 2];

        int byteCnt = 0;

        for (int i = 0; i < chars.length; i += 2) {
            int newByte = 0;

            newByte |= hexCharToByte(chars[i]);

            newByte <<= 4;

            newByte |= hexCharToByte(chars[i + 1]);

            bytes[byteCnt] = (byte)newByte;

            byteCnt++;
        }

        return bytes;
    }

    /**
     * Return byte value for certain character.
     *
     * @param ch Character
     * @return Byte value.
     * @throws IllegalArgumentException If input character differ from certain hex characters.
     */
    @SuppressWarnings({"UnnecessaryFullyQualifiedName", "fallthrough"})
    private static byte hexCharToByte(char ch) throws IllegalArgumentException {
        switch (ch) {
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                return (byte)(ch - '0');

            case 'a':
            case 'A':
                return 0xa;

            case 'b':
            case 'B':
                return 0xb;

            case 'c':
            case 'C':
                return 0xc;

            case 'd':
            case 'D':
                return 0xd;

            case 'e':
            case 'E':
                return 0xe;

            case 'f':
            case 'F':
                return 0xf;

            default:
                throw new IllegalArgumentException("Hex decoding wrong input character [character=" + ch + ']');
        }
    }

    /**
     * Converts primitive double to byte array.
     *
     * @param d Double to convert.
     * @return Byte array.
     */
    public static byte[] doubleToBytes(double d) {
        return longToBytes(Double.doubleToLongBits(d));
    }

    /**
     * Converts primitive {@code double} type to byte array and stores
     * it in the specified byte array.
     *
     * @param d Double to convert.
     * @param bytes Array of bytes.
     * @param off Offset.
     * @return New offset.
     */
    public static int doubleToBytes(double d, byte[] bytes, int off) {
        return longToBytes(Double.doubleToLongBits(d), bytes, off);
    }

    /**
     * Converts primitive float to byte array.
     *
     * @param f Float to convert.
     * @return Array of bytes.
     */
    public static byte[] floatToBytes(float f) {
        return intToBytes(Float.floatToIntBits(f));
    }

    /**
     * Converts primitive float to byte array.
     *
     * @param f Float to convert.
     * @param bytes Array of bytes.
     * @param off Offset.
     * @return New offset.
     */
    public static int floatToBytes(float f, byte[] bytes, int off) {
        return intToBytes(Float.floatToIntBits(f), bytes, off);
    }

    /**
     * Converts primitive {@code long} type to byte array.
     *
     * @param l Long value.
     * @return Array of bytes.
     */
    public static byte[] longToBytes(long l) {
        return GridClientByteUtils.longToBytes(l);
    }

    /**
     * Converts primitive {@code long} type to byte array and stores it in specified
     * byte array.
     *
     * @param l Long value.
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int longToBytes(long l, byte[] bytes, int off) {
        return off + GridClientByteUtils.longToBytes(l, bytes, off);
    }

    /**
     * Converts primitive {@code int} type to byte array.
     *
     * @param i Integer value.
     * @return Array of bytes.
     */
    public static byte[] intToBytes(int i) {
        return GridClientByteUtils.intToBytes(i);
    }

    /**
     * Converts primitive {@code int} type to byte array and stores it in specified
     * byte array.
     *
     * @param i Integer value.
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int intToBytes(int i, byte[] bytes, int off) {
        return off + GridClientByteUtils.intToBytes(i, bytes, off);
    }

    /**
     * Converts primitive {@code short} type to byte array.
     *
     * @param s Short value.
     * @return Array of bytes.
     */
    public static byte[] shortToBytes(short s) {
        return GridClientByteUtils.shortToBytes(s);
    }

    /**
     * Converts primitive {@code short} type to byte array and stores it in specified
     * byte array.
     *
     * @param s Short value.
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int shortToBytes(short s, byte[] bytes, int off) {
        return off + GridClientByteUtils.shortToBytes(s, bytes, off);
    }

    /**
     * Encodes {@link java.util.UUID} into a sequence of bytes using the {@link java.nio.ByteBuffer},
     * storing the result into a new byte array.
     *
     * @param uuid Unique identifier.
     * @param arr Byte array to fill with result.
     * @param off Offset in {@code arr}.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int uuidToBytes(@Nullable UUID uuid, byte[] arr, int off) {
        return off + GridClientByteUtils.uuidToBytes(uuid, arr, off);
    }

    /**
     * Converts an UUID to byte array.
     *
     * @param uuid UUID value.
     * @return Encoded into byte array {@link java.util.UUID}.
     */
    public static byte[] uuidToBytes(@Nullable UUID uuid) {
        return GridClientByteUtils.uuidToBytes(uuid);
    }

    /**
     * Constructs {@code short} from byte array.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Short value.
     */
    public static short bytesToShort(byte[] bytes, int off) {
        assert bytes != null;

        int bytesCnt = Short.SIZE >> 3;

        if (off + bytesCnt > bytes.length)
            // Just use the remainder.
            bytesCnt = bytes.length - off;

        short res = 0;

        for (int i = 0; i < bytesCnt; i++) {
            int shift = bytesCnt - i - 1 << 3;

            res |= (0xffL & bytes[off++]) << shift;
        }

        return res;
    }

    /**
     * Constructs {@code int} from byte array.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Integer value.
     */
    public static int bytesToInt(byte[] bytes, int off) {
        assert bytes != null;

        int bytesCnt = Integer.SIZE >> 3;

        if (off + bytesCnt > bytes.length)
            // Just use the remainder.
            bytesCnt = bytes.length - off;

        int res = 0;

        for (int i = 0; i < bytesCnt; i++) {
            int shift = bytesCnt - i - 1 << 3;

            res |= (0xffL & bytes[off++]) << shift;
        }

        return res;
    }

    /**
     * Constructs {@code long} from byte array.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Long value.
     */
    public static long bytesToLong(byte[] bytes, int off) {
        assert bytes != null;

        int bytesCnt = Long.SIZE >> 3;

        if (off + bytesCnt > bytes.length)
            bytesCnt = bytes.length - off;

        long res = 0;

        for (int i = 0; i < bytesCnt; i++) {
            int shift = bytesCnt - i - 1 << 3;

            res |= (0xffL & bytes[off++]) << shift;
        }

        return res;
    }

    /**
     * Reads an {@link java.util.UUID} form byte array.
     * If given array contains all 0s then {@code null} will be returned.
     *
     * @param bytes array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return UUID value or {@code null}.
     */
    public static UUID bytesToUuid(byte[] bytes, int off) {
        return GridClientByteUtils.bytesToUuid(bytes, off);
    }

    /**
     * Constructs double from byte array.
     *
     * @param bytes Byte array.
     * @param off Offset in {@code bytes} array.
     * @return Double value.
     */
    public static double bytesToDouble(byte[] bytes, int off) {
        return Double.longBitsToDouble(bytesToLong(bytes, off));
    }

    /**
     * Constructs float from byte array.
     *
     * @param bytes Byte array.
     * @param off Offset in {@code bytes} array.
     * @return Float value.
     */
    public static float bytesToFloat(byte[] bytes, int off) {
        return Float.intBitsToFloat(bytesToInt(bytes, off));
    }

    /**
     * Compares fragments of byte arrays.
     *
     * @param a First array.
     * @param aOff First array offset.
     * @param b Second array.
     * @param bOff Second array offset.
     * @param len Length of fragments.
     * @return {@code true} if fragments are equal, {@code false} otherwise.
     */
    public static boolean bytesEqual(byte[] a, int aOff, byte[] b, int bOff, int len) {
        if (aOff + len > a.length || bOff + len > b.length)
            return false;
        else {
            for (int i = 0; i < len; i++)
                if (a[aOff + i] != b[bOff + i])
                    return false;

            return true;
        }
    }

    /**
     * Converts an array of characters representing hexidecimal values into an
     * array of bytes of those same values. The returned array will be half the
     * length of the passed array, as it takes two characters to represent any
     * given byte. An exception is thrown if the passed char array has an odd
     * number of elements.
     *
     * @param data An array of characters containing hexidecimal digits
     * @return A byte array containing binary data decoded from
     *         the supplied char array.
     * @throws org.apache.ignite.IgniteCheckedException Thrown if an odd number or illegal of characters is supplied.
     */
    public static byte[] decodeHex(char[] data) throws IgniteCheckedException {

        int len = data.length;

        if ((len & 0x01) != 0)
            throw new IgniteCheckedException("Odd number of characters.");

        byte[] out = new byte[len >> 1];

        // Two characters form the hex value.
        for (int i = 0, j = 0; j < len; i++) {
            int f = IgniteUtils.toDigit(data[j], j) << 4;

            j++;

            f |= IgniteUtils.toDigit(data[j], j);

            j++;

            out[i] = (byte)(f & 0xFF);
        }

        return out;
    }

    /**
     * Zips byte array.
     *
     * @param input Input bytes.
     * @return Zipped byte array.
     * @throws java.io.IOException If failed.
     */
    public static byte[] zipBytes(byte[] input) throws IOException {
        return zipBytes(input, 4096);
    }

    /**
     * Zips byte array.
     *
     * @param input Input bytes.
     * @param initBufSize Initial buffer size.
     * @return Zipped byte array.
     * @throws java.io.IOException If failed.
     */
    public static byte[] zipBytes(byte[] input, int initBufSize) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(initBufSize);

        try (ZipOutputStream zos = new ZipOutputStream(bos)) {
            ZipEntry entry = new ZipEntry("");

            try {
                entry.setSize(input.length);

                zos.putNextEntry(entry);

                zos.write(input);
            } finally {
                zos.closeEntry();
            }
        }

        return bos.toByteArray();
    }
}
