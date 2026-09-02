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

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryStreams;
import org.apache.ignite.internal.binary.streams.BinaryStreamsTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Tests that {@link StringWriter} output is byte-identical to serialization of the {@link String#getBytes()} result,
 * which was used before zero-copy string serialization was introduced.
 */
public class StringWriterSelfTest extends GridCommonAbstractTest {
    /** */
    public static final int ASCII_MAX = 0x80;

    /** */
    public static final int LATIN1_MAX = 0x100;

    /** */
    public static final int TWO_BYTES_MAX = 0x800;

    /** */
    public static final int THREE_BYTES_MAX = 0xD800;

    /** */
    public static final int FOUR_BYTES_MAX = 0xE000;

    /** */
    public static final int FOUR_BYTES_HIGH_BOUND = 0x10000;

    /** Tests for all encoder paths: ASCII bulk copy, Latin-1, generic UTF-16 and malformed surrogates. */
    @Test
    public void testCorpus() {
        String[] cases = {
            "",
            "a",
            "?",
            "abcdefghijklmnopqrstuvwxyz0123456789", // Long ASCII: exercises the 8-byte stride scan and bulk copy.
            "caf\u00e9",                            // Latin-1 with a negative byte.
            "\u00ff\u0080\u00a0",                   // Latin-1, negative bytes only.
            "\u041f\u0440\u0438\u0432\u0435\u0442", // Cyrillic: 2-byte UTF-8 sequences.
            "\u0800\u1234\uffff",                   // 3-byte UTF-8 sequences.
            "\ud83d\ude00",                         // Emoji: valid surrogate pair.
            "a\ud83d\ude00b\u00e9\u0416\u0001",     // Mixed content.
            "\ud800",                               // Lone high surrogate.
            "\udc00",                               // Lone low surrogate.
            "a\ud800",                              // High surrogate at the end.
            "\ud800a",                              // High surrogate followed by a regular char.
            "\ud800\ud800",                         // Two high surrogates.
            "\udc00\ud800",                         // Low surrogate before a high one.
            "\u0000",                               // NUL char.
            "nul\u0000nul"
        };

        for (String str : cases)
            check(str);
    }

    /** Randomized differential test against {@link String#getBytes()}. */
    @Test
    public void testRandomStrings() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int iter = 0; iter < 100; iter++) {
            StringBuilder sb = new StringBuilder(1 + rnd.nextInt(42));

            for (int i = 0; i < sb.capacity(); i++) {
                int bucket = rnd.nextInt(100);

                char c;

                if (bucket < 40)
                    // ASCII.
                    c = (char)rnd.nextInt(ASCII_MAX);
                else if (bucket < 55)
                    // Latin-1.
                    c = (char)(ASCII_MAX + rnd.nextInt(LATIN1_MAX - ASCII_MAX));
                else if (bucket < 65)
                    // Other 2-byte chars.
                    c = (char)(LATIN1_MAX + rnd.nextInt(TWO_BYTES_MAX - LATIN1_MAX));
                else if (bucket < 75)
                    // 3-byte chars.
                    c = (char)(TWO_BYTES_MAX + rnd.nextInt(THREE_BYTES_MAX - TWO_BYTES_MAX));
                else if (bucket < 90)
                    // Surrogates, mostly malformed.
                    c = (char)(THREE_BYTES_MAX + rnd.nextInt(FOUR_BYTES_MAX - THREE_BYTES_MAX));
                else
                    // 3-byte chars above the surrogate range.
                    c = (char)(FOUR_BYTES_MAX + rnd.nextInt(FOUR_BYTES_HIGH_BOUND - FOUR_BYTES_MAX));

                sb.append(c);
            }

            assertFalse(sb.isEmpty());

            check(sb.toString());
        }
    }

    /**
     * Tests strings whose UTF-8 form is larger than the stream's minimal capacity, so that the encoder's own capacity
     * reservation (rather than the buffer's initial slack) is what keeps the unchecked writes in bounds. Covers every
     * encoder path on both heap and offheap streams.
     */
    @Test
    public void testLargeStrings() {
        int len = 100_000;

        StringBuilder ascii = new StringBuilder(len);
        StringBuilder latin1 = new StringBuilder(len);
        StringBuilder cyrillic = new StringBuilder(len);
        StringBuilder mixed = new StringBuilder(len);

        for (int i = 0; i < len; i++) {
            ascii.append((char)('a' + i % 26));
            // Every char is a Latin-1 char with the sign bit set: worst case for the 2-bytes-per-char reservation.
            latin1.append((char)(ASCII_MAX + i % (LATIN1_MAX - ASCII_MAX)));
            cyrillic.append((char)('\u0410' + i % 32));
            mixed.append((char)('a' + i % 26)).append('\u00e9').append('\u0416').append('\u20ac').append("\ud83d\ude00");
        }

        check(ascii.toString());
        check(latin1.toString());
        check(cyrillic.toString());
        check(mixed.toString());
    }

    /** Tests that the stream position is correct after a string write, so surrounding values are not corrupted. */
    @Test
    public void testStreamPosition() {
        int int1 = 0xDEADBEEF;
        String str1 = "caf\u00e9";
        String str2 = "\ud83d\ude00";
        int int2 = 0xCAFEBABE;

        // Small initial capacity to check buffer reallocation.
        try (BinaryOutputStream out = BinaryStreams.outputStream(2)) {
            out.writeInt(int1);
            StringWriter.write(str1, out);
            StringWriter.write(str2, out);
            out.writeInt(int2);

            byte[] strBytes1 = strBytes(str1);
            byte[] strBytes2 = strBytes(str2);

            byte[] exp = new byte[Integer.BYTES + strBytes1.length + strBytes2.length + Integer.BYTES];

            System.arraycopy(intBytes(int1), 0, exp, 0, Integer.BYTES);
            System.arraycopy(strBytes1, 0, exp, Integer.BYTES, strBytes1.length);
            System.arraycopy(strBytes2, 0, exp, Integer.BYTES + strBytes1.length, strBytes2.length);
            System.arraycopy(intBytes(int2), 0, exp, Integer.BYTES + strBytes1.length + strBytes2.length, Integer.BYTES);

            assertTrue(Arrays.equals(exp, out.arrayCopy()));
        }
    }

    /**
     * Checks that serialized form of the given string is byte-identical to serialization of the {@link String#getBytes()} result.
     * @param str String to check.
     */
    private void check(String str) {
        for (boolean heapStream : new boolean[] {true, false}) {
            try (BinaryOutputStream out = heapStream ? BinaryStreams.outputStream(1) : BinaryStreamsTestUtils.offheapOutputStream(1)) {
                StringWriter.write(str, out);

                assertTrue("String serialization mismatch: " + str, Arrays.equals(strBytes(str), out.arrayCopy()));
            }
        }
    }

    /**
     * @param str String.
     * @return Expected serialized form of the string: flag, UTF-8 length and UTF-8 bytes.
     */
    private static byte[] strBytes(String str) {
        byte[] bytes = str.getBytes(UTF_8);
        byte[] res = new byte[Byte.BYTES + Integer.BYTES + bytes.length];

        res[0] = GridBinaryMarshaller.STRING;

        System.arraycopy(intBytes(bytes.length), 0, res, 1, 4);
        System.arraycopy(bytes, 0, res, 5, bytes.length);

        return res;
    }

    /**
     * @param val Value.
     * @return Little-endian representation of the value.
     */
    private static byte[] intBytes(int val) {
        return new byte[] {(byte)val, (byte)(val >> 8), (byte)(val >> 16), (byte)(val >> 24)};
    }
}
