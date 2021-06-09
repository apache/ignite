/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util;

import java.io.Serializable;
import java.util.Comparator;

/**
 *
 */
public final class BytesUtil {

    public static final byte[] EMPTY_BYTES = new byte[0];

    // A byte array comparator based on lexicograpic ordering.
    private static final ByteArrayComparator BYTES_LEXICO_COMPARATOR = new LexicographicByteArrayComparator();

    public static byte[] nullToEmpty(final byte[] bytes) {
        return bytes == null ? EMPTY_BYTES : bytes;
    }

    public static boolean isEmpty(final byte[] bytes) {
        return bytes == null || bytes.length == 0;
    }

//    /**
//     * This method has better performance than String#getBytes(Charset),
//     * See the benchmark class: Utf8Benchmark for details.
//     */
//    public static byte[] writeUtf8(final String in) {
//        if (in == null) {
//            return null;
//        }
//        if (UnsafeUtil.hasUnsafe()) {
//            // Calculate the encoded length.
//            final int len = UnsafeUtf8Util.encodedLength(in);
//            final byte[] outBytes = new byte[len];
//            UnsafeUtf8Util.encodeUtf8(in, outBytes, 0, len);
//            return outBytes;
//        } else {
//            return in.getBytes(StandardCharsets.UTF_8);
//        }
//    }
//
//    /**
//     * This method has better performance than String#String(byte[], Charset),
//     * See the benchmark class: Utf8Benchmark for details.
//     */
//    public static String readUtf8(final byte[] in) {
//        if (in == null) {
//            return null;
//        }
//        if (UnsafeUtil.hasUnsafe()) {
//            return UnsafeUtf8Util.decodeUtf8(in, 0, in.length);
//        } else {
//            return new String(in, StandardCharsets.UTF_8);
//        }
//    }

    public static byte[] nextBytes(final byte[] bytes) {
        Requires.requireNonNull(bytes, "bytes");
        final int len = bytes.length;
        if (len == 0) { // fast path
            return new byte[] {0};
        }
        final byte[] nextBytes = new byte[len + 1];
        System.arraycopy(bytes, 0, nextBytes, 0, len);
        nextBytes[len] = 0;
        return nextBytes;
    }

    public static ByteArrayComparator getDefaultByteArrayComparator() {
        return BYTES_LEXICO_COMPARATOR;
    }

    public static int compare(final byte[] a, final byte[] b) {
        return getDefaultByteArrayComparator().compare(a, b);
    }

    public static byte[] max(final byte[] a, final byte[] b) {
        return getDefaultByteArrayComparator().compare(a, b) > 0 ? a : b;
    }

    public static byte[] min(final byte[] a, final byte[] b) {
        return getDefaultByteArrayComparator().compare(a, b) < 0 ? a : b;
    }

    public interface ByteArrayComparator extends Comparator<byte[]>, Serializable {

        int compare(final byte[] buffer1, final int offset1, final int length1, final byte[] buffer2,
            final int offset2, final int length2);
    }

    private static class LexicographicByteArrayComparator implements ByteArrayComparator {

        private static final long serialVersionUID = -8623342242397267864L;

        @Override
        public int compare(final byte[] buffer1, final byte[] buffer2) {
            return compare(buffer1, 0, buffer1.length, buffer2, 0, buffer2.length);
        }

        @Override
        public int compare(final byte[] buffer1, final int offset1, final int length1, final byte[] buffer2,
            final int offset2, final int length2) {
            // short circuit equal case
            if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2) {
                return 0;
            }
            // similar to Arrays.compare() but considers offset and length
            final int end1 = offset1 + length1;
            final int end2 = offset2 + length2;
            for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
                int a = buffer1[i] & 0xff;
                int b = buffer2[j] & 0xff;
                if (a != b) {
                    return a - b;
                }
            }
            return length1 - length2;
        }
    }

    private final static char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    /**
     * Dump byte array into a hex string. See https://stackoverflow.com/questions/9655181/how-to-convert-a-byte-array-to-a-hex-string-in-java
     *
     * @param bytes bytes
     * @return hex string
     */
    public static String toHex(final byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        final char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    /**
     * Convert a string representation of a hex dump to a byte array. See https://stackoverflow.com/questions/140131/convert-a-string-representation-of-a-hex-dump-to-a-byte-array-using-java
     *
     * @param s hex string
     * @return bytes
     */
    public static byte[] hexStringToByteArray(final String s) {
        if (s == null) {
            return null;
        }
        final int len = s.length();
        final byte[] bytes = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            bytes[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
        }
        return bytes;
    }

    private BytesUtil() {
    }
}
