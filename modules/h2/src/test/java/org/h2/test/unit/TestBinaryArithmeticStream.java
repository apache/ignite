/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import org.h2.dev.util.BinaryArithmeticStream;
import org.h2.dev.util.BinaryArithmeticStream.Huffman;
import org.h2.dev.util.BinaryArithmeticStream.In;
import org.h2.dev.util.BinaryArithmeticStream.Out;
import org.h2.dev.util.BitStream;
import org.h2.test.TestBase;

/**
 * Test the binary arithmetic stream utility.
 */
public class TestBinaryArithmeticStream extends TestBase {

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
        testCompareWithHuffman();
        testHuffmanRandomized();
        testCompressionRatio();
        testRandomized();
        testPerformance();
    }

    private void testCompareWithHuffman() throws IOException {
        Random r = new Random(1);
        for (int test = 0; test < 10; test++) {
            int[] freq = new int[4];
            for (int i = 0; i < freq.length; i++) {
                freq[i] = 0 + r.nextInt(1000);
            }
            BinaryArithmeticStream.Huffman ah = new BinaryArithmeticStream.Huffman(
                    freq);
            BitStream.Huffman hh = new BitStream.Huffman(freq);
            ByteArrayOutputStream hbOut = new ByteArrayOutputStream();
            ByteArrayOutputStream abOut = new ByteArrayOutputStream();
            BitStream.Out bOut = new BitStream.Out(hbOut);
            BinaryArithmeticStream.Out aOut = new BinaryArithmeticStream.Out(abOut);
            for (int i = 0; i < freq.length; i++) {
                for (int j = 0; j < freq[i]; j++) {
                    int x = i;
                    hh.write(bOut, x);
                    ah.write(aOut, x);
                }
            }
            assertTrue(hbOut.toByteArray().length >= abOut.toByteArray().length);
        }
    }

    private void testHuffmanRandomized() throws IOException {
        Random r = new Random(1);
        int[] freq = new int[r.nextInt(200) + 1];
        for (int i = 0; i < freq.length; i++) {
            freq[i] = r.nextInt(1000) + 1;
        }
        int seed = r.nextInt();
        r.setSeed(seed);
        Huffman huff = new Huffman(freq);
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        Out out = new Out(byteOut);
        for (int i = 0; i < 10000; i++) {
            huff.write(out, r.nextInt(freq.length));
        }
        out.flush();
        In in = new In(new ByteArrayInputStream(byteOut.toByteArray()));
        r.setSeed(seed);
        for (int i = 0; i < 10000; i++) {
            int expected = r.nextInt(freq.length);
            int got = huff.read(in);
            assertEquals(expected, got);
        }
    }

    private void testPerformance() throws IOException {
        Random r = new Random();
        // long time = System.nanoTime();
        // Profiler prof = new Profiler().startCollecting();
        for (int seed = 0; seed < 10000; seed++) {
            r.setSeed(seed);
            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            Out out = new Out(byteOut);
            int len = 100;
            for (int i = 0; i < len; i++) {
                boolean v = r.nextBoolean();
                int prob = r.nextInt(BinaryArithmeticStream.MAX_PROBABILITY);
                out.writeBit(v, prob);
            }
            out.flush();
            r.setSeed(seed);
            ByteArrayInputStream byteIn = new ByteArrayInputStream(
                    byteOut.toByteArray());
            In in = new In(byteIn);
            for (int i = 0; i < len; i++) {
                boolean expected = r.nextBoolean();
                int prob = r.nextInt(BinaryArithmeticStream.MAX_PROBABILITY);
                assertEquals(expected, in.readBit(prob));
            }
        }
        // time = System.nanoTime() - time;
        // System.out.println("time: " + TimeUnit.NANOSECONDS.toMillis(time));
        // System.out.println(prof.getTop(5));
    }

    private void testCompressionRatio() throws IOException {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        Out out = new Out(byteOut);
        int prob = 1000;
        int len = 1024;
        for (int i = 0; i < len; i++) {
            out.writeBit(true, prob);
        }
        out.flush();
        ByteArrayInputStream byteIn = new ByteArrayInputStream(
                byteOut.toByteArray());
        In in = new In(byteIn);
        for (int i = 0; i < len; i++) {
            assertTrue(in.readBit(prob));
        }
        // System.out.println(len / 8 + " comp: " +
        // byteOut.toByteArray().length);
    }

    private void testRandomized() throws IOException {
        for (int i = 0; i < 10000; i = (int) ((i + 10) * 1.1)) {
            testRandomized(i);
        }
    }

    private void testRandomized(int len) throws IOException {
        Random r = new Random();
        int seed = r.nextInt();
        r.setSeed(seed);
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        Out out = new Out(byteOut);
        for (int i = 0; i < len; i++) {
            int prob = r.nextInt(BinaryArithmeticStream.MAX_PROBABILITY);
            out.writeBit(r.nextBoolean(), prob);
        }
        out.flush();
        byteOut.write(r.nextInt(255));
        ByteArrayInputStream byteIn = new ByteArrayInputStream(
                byteOut.toByteArray());
        In in = new In(byteIn);
        r.setSeed(seed);
        for (int i = 0; i < len; i++) {
            int prob = r.nextInt(BinaryArithmeticStream.MAX_PROBABILITY);
            boolean expected = r.nextBoolean();
            boolean got = in.readBit(prob);
            assertEquals(expected, got);
        }
        assertEquals(r.nextInt(255), byteIn.read());
    }

}
