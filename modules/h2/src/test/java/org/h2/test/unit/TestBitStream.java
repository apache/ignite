/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Random;

import org.h2.dev.util.BitStream;
import org.h2.dev.util.BitStream.In;
import org.h2.dev.util.BitStream.Out;
import org.h2.test.TestBase;

/**
 * Test the bit stream (Golomb code and Huffman code) utility.
 */
public class TestBitStream extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        testHuffmanRandomized();
        testHuffman();
        testBitStream();
        testGolomb("11110010", 10, 42);
        testGolomb("00", 3, 0);
        testGolomb("010", 3, 1);
        testGolomb("011", 3, 2);
        testGolomb("100", 3, 3);
        testGolomb("1010", 3, 4);
        testGolombRandomized();
    }

    private void testHuffmanRandomized() {
        Random r = new Random(1);
        int[] freq = new int[r.nextInt(200) + 1];
        for (int i = 0; i < freq.length; i++) {
            freq[i] = r.nextInt(1000) + 1;
        }
        int seed = r.nextInt();
        r.setSeed(seed);
        BitStream.Huffman huff = new BitStream.Huffman(freq);
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        BitStream.Out out = new BitStream.Out(byteOut);
        for (int i = 0; i < 10000; i++) {
            huff.write(out, r.nextInt(freq.length));
        }
        out.close();
        BitStream.In in = new BitStream.In(new ByteArrayInputStream(byteOut.toByteArray()));
        r.setSeed(seed);
        for (int i = 0; i < 10000; i++) {
            int expected = r.nextInt(freq.length);
            assertEquals(expected, huff.read(in));
        }
    }

    private void testHuffman() {
        int[] freq = { 36, 18, 12, 9, 7, 6, 5, 4 };
        BitStream.Huffman huff = new BitStream.Huffman(freq);
        final StringBuilder buff = new StringBuilder();
        Out o = new Out(null) {
            @Override
            public void writeBit(int bit) {
                buff.append(bit == 0 ? '0' : '1');
            }
        };
        for (int i = 0; i < freq.length; i++) {
            buff.append(i + ": ");
            huff.write(o, i);
            buff.append("\n");
        }
        assertEquals(
                "0: 0\n" +
                "1: 110\n" +
                "2: 100\n" +
                "3: 1110\n" +
                "4: 1011\n" +
                "5: 1010\n" +
                "6: 11111\n" +
                "7: 11110\n", buff.toString());
    }

    private void testGolomb(String expected, int div, int value) {
        final StringBuilder buff = new StringBuilder();
        Out o = new Out(null) {
            @Override
            public void writeBit(int bit) {
                buff.append(bit == 0 ? '0' : '1');
            }
        };
        o.writeGolomb(div, value);
        int size = Out.getGolombSize(div, value);
        String got = buff.toString();
        assertEquals(size, got.length());
        assertEquals(expected, got);
    }

    private void testGolombRandomized() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Out bitOut = new Out(out);
        Random r = new Random(1);
        int len = 1000;
        for (int i = 0; i < len; i++) {
            int div = r.nextInt(100) + 1;
            int value = r.nextInt(1000000);
            bitOut.writeGolomb(div, value);
        }
        bitOut.flush();
        bitOut.close();
        byte[] data = out.toByteArray();
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        In bitIn = new In(in);
        r.setSeed(1);
        for (int i = 0; i < len; i++) {
            int div = r.nextInt(100) + 1;
            int value = r.nextInt(1000000);
            int v = bitIn.readGolomb(div);
            assertEquals("i=" + i + " div=" + div, value, v);
        }
    }

    private void testBitStream() {
        Random r = new Random();
        for (int test = 0; test < 10000; test++) {
            ByteArrayOutputStream buff = new ByteArrayOutputStream();
            int len = r.nextInt(40);
            Out out = new Out(buff);
            long seed = r.nextLong();
            Random r2 = new Random(seed);
            for (int i = 0; i < len; i++) {
                out.writeBit(r2.nextBoolean() ? 1 : 0);
            }
            out.close();
            In in = new In(new ByteArrayInputStream(
                    buff.toByteArray()));
            r2 = new Random(seed);
            int i = 0;
            for (; i < len; i++) {
                int expected = r2.nextBoolean() ? 1 : 0;
                assertEquals(expected, in.readBit());
            }
            for (; i % 8 != 0; i++) {
                assertEquals(0, in.readBit());
            }
            assertEquals(-1, in.readBit());
        }
    }

}
