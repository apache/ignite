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
import java.util.Random;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryStreams;
import org.apache.ignite.internal.binary.streams.BinaryStreamsTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Tests that {@link StringWriter} output is byte-identical to serialization of the {@link String#getBytes} result,
 * which was used before zero-copy string serialization was introduced.
 */
public class StringWriterSelfTest extends GridCommonAbstractTest {
    /** Edge cases for all encoder paths: ASCII bulk copy, Latin-1, generic UTF-16 and malformed surrogates. */
    private static final String[] CORPUS = {
        "",
        "a",
        "?",
        "abcdefghijklmnopqrstuvwxyz0123456789",  // Long ASCII: exercises the 8-byte stride scan and bulk copy.
        "caf\u00e9",                             // Latin-1 with a negative byte.
        "\u00ff\u0080\u00a0",                    // Latin-1, negative bytes only.
        "\u041f\u0440\u0438\u0432\u0435\u0442", // Cyrillic: 2-byte UTF-8 sequences.
        "\u0800\u1234\uffff",                   // 3-byte UTF-8 sequences.
        "\ud83d\ude00",                          // Emoji: valid surrogate pair.
        "a\ud83d\ude00b\u00e9\u0416\u0001",     // Mixed content.
        "\ud800",                                // Lone high surrogate.
        "\udc00",                                // Lone low surrogate.
        "a\ud800",                               // High surrogate at the end.
        "\ud800a",                               // High surrogate followed by a regular char.
        "\ud800\ud800",                          // Two high surrogates.
        "\udc00\ud800",                          // Low surrogate before a high one.
        "\u0000",                                // NUL char.
        "nul\u0000nul"
    };

    /**
     * Tests corpus of edge case strings.
     */
    @Test
    public void testCorpus() {
        for (String str : CORPUS)
            check(str);
    }

    /**
     * Randomized differential test against {@link String#getBytes}.
     */
    @Test
    public void testRandomStrings() {
        Random rnd = new Random(4242);

        for (int i = 0; i < 5_000; i++) {
            int len = rnd.nextInt(65);

            StringBuilder sb = new StringBuilder(len);

            for (int j = 0; j < len; j++) {
                int bucket = rnd.nextInt(100);

                char c;

                if (bucket < 40)
                    c = (char)rnd.nextInt(0x80);                        // ASCII.
                else if (bucket < 55)
                    c = (char)(0x80 + rnd.nextInt(0x100 - 0x80));       // Latin-1.
                else if (bucket < 65)
                    c = (char)(0x100 + rnd.nextInt(0x800 - 0x100));     // Other 2-byte chars.
                else if (bucket < 75)
                    c = (char)(0x800 + rnd.nextInt(0xD800 - 0x800));    // 3-byte chars.
                else if (bucket < 90)
                    c = (char)(0xD800 + rnd.nextInt(0xE000 - 0xD800));  // Surrogates, mostly malformed.
                else
                    c = (char)(0xE000 + rnd.nextInt(0x10000 - 0xE000)); // 3-byte chars above the surrogate range.

                sb.append(c);
            }

            check(sb.toString());
        }
    }

    /**
     * Tests that the stream position is correct after a string write, so surrounding values are not corrupted.
     */
    @Test
    public void testStreamPosition() {
        // Small initial capacity to exercise buffer reallocation.
        try (BinaryOutputStream out = BinaryStreams.outputStream(2)) {
            out.writeInt(0xDEADBEEF);

            StringWriter.write("caf\u00e9", out);
            StringWriter.write("\ud83d\ude00", out);

            out.writeInt(0xCAFEBABE);

            byte[] exp = concat(
                intLE(0xDEADBEEF),
                strBytes("caf\u00e9"),
                strBytes("\ud83d\ude00"),
                intLE(0xCAFEBABE));

            assertTrue(Arrays.equals(exp, out.arrayCopy()));
        }
    }

    /**
     * Checks that serialized form of the given string is byte-identical to serialization of
     * the {@link String#getBytes} result.
     *
     * @param str String to check.
     */
    private void check(String str) {
        byte[] exp = strBytes(str);

        try (BinaryOutputStream out = BinaryStreams.outputStream(1)) {
            StringWriter.write(str, out);

            assertSerialized(str, exp, out.arrayCopy());
        }

        try (BinaryOutputStream out = BinaryStreamsTestUtils.offheapOutputStream(1)) {
            StringWriter.write(str, out);

            assertSerialized(str, exp, out.arrayCopy());
        }
    }

    /**
     * @param str Source string.
     * @param exp Expected serialized form.
     * @param act Actual serialized form.
     */
    private void assertSerialized(String str, byte[] exp, byte[] act) {
        if (!Arrays.equals(exp, act)) {
            fail("String serialization mismatch [str=" + Arrays.toString(str.toCharArray()) +
                ", exp=" + Arrays.toString(exp) + ", act=" + Arrays.toString(act) + ']');
        }
    }

    /**
     * @param str String.
     * @return Expected serialized form of the string: flag, UTF-8 length and UTF-8 bytes.
     */
    private static byte[] strBytes(String str) {
        byte[] utf8 = str.getBytes(UTF_8);

        byte[] res = new byte[5 + utf8.length];

        res[0] = GridBinaryMarshaller.STRING;

        System.arraycopy(intLE(utf8.length), 0, res, 1, 4);
        System.arraycopy(utf8, 0, res, 5, utf8.length);

        return res;
    }

    /**
     * @param val Value.
     * @return Little-endian representation of the value.
     */
    private static byte[] intLE(int val) {
        return new byte[] {(byte)val, (byte)(val >> 8), (byte)(val >> 16), (byte)(val >> 24)};
    }

    /**
     * @param arrs Arrays.
     * @return Concatenated arrays.
     */
    private static byte[] concat(byte[]... arrs) {
        int len = 0;

        for (byte[] arr : arrs)
            len += arr.length;

        byte[] res = new byte[len];

        int pos = 0;

        for (byte[] arr : arrs) {
            System.arraycopy(arr, 0, res, pos, arr.length);

            pos += arr.length;
        }

        return res;
    }
}
