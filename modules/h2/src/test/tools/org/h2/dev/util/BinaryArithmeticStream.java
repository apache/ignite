/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.PriorityQueue;

/**
 * A binary arithmetic stream.
 */
public class BinaryArithmeticStream {

    /**
     * The maximum probability.
     */
    public static final int MAX_PROBABILITY = (1 << 12) - 1;

    /**
     * The low marker.
     */
    protected int low;

    /**
     * The high marker.
     */
    protected int high = 0xffffffff;

    /**
     * A binary arithmetic input stream.
     */
    public static class In extends BinaryArithmeticStream {

        private final InputStream in;
        private int data;

        public In(InputStream in) throws IOException {
            this.in = in;
            data = ((in.read() & 0xff) << 24) |
                    ((in.read() & 0xff) << 16) |
                    ((in.read() & 0xff) << 8) |
                    (in.read() & 0xff);
        }

        /**
         * Read a bit.
         *
         * @param probability the probability that the value is true
         * @return the value
         */
        public boolean readBit(int probability) throws IOException {
            int split = low + probability * ((high - low) >>> 12);
            boolean value;
            // compare unsigned
            if (data + Integer.MIN_VALUE > split + Integer.MIN_VALUE) {
                low = split + 1;
                value = false;
            } else {
                high = split;
                value = true;
            }
            while (low >>> 24 == high >>> 24) {
                data = (data << 8) | (in.read() & 0xff);
                low <<= 8;
                high = (high << 8) | 0xff;
            }
            return value;
        }

        /**
         * Read a value that is stored as a Golomb code.
         *
         * @param divisor the divisor
         * @return the value
         */
        public int readGolomb(int divisor) throws IOException {
            int q = 0;
            while (readBit(MAX_PROBABILITY / 2)) {
                q++;
            }
            int bit = 31 - Integer.numberOfLeadingZeros(divisor - 1);
            int r = 0;
            if (bit >= 0) {
                int cutOff = (2 << bit) - divisor;
                for (; bit > 0; bit--) {
                    r = (r << 1) + (readBit(MAX_PROBABILITY / 2) ? 1 : 0);
                }
                if (r >= cutOff) {
                    r = (r << 1) + (readBit(MAX_PROBABILITY / 2) ? 1 : 0) - cutOff;
                }
            }
            return q * divisor + r;
        }

    }

    /**
     * A binary arithmetic output stream.
     */
    public static class Out extends BinaryArithmeticStream {

        private final OutputStream out;

        public Out(OutputStream out) {
            this.out = out;
        }

        /**
         * Write a bit.
         *
         * @param value the value
         * @param probability the probability that the value is true
         */
        public void writeBit(boolean value, int probability) throws IOException {
            int split = low + probability * ((high - low) >>> 12);
            if (value) {
                high = split;
            } else {
                low = split + 1;
            }
            while (low >>> 24 == high >>> 24) {
                out.write(high >> 24);
                low <<= 8;
                high = (high << 8) | 0xff;
            }
        }

        /**
         * Flush the stream.
         */
        public void flush() throws IOException {
            out.write(high >> 24);
            out.write(high >> 16);
            out.write(high >> 8);
            out.write(high);
        }

        /**
         * Write the Golomb code of a value.
         *
         * @param divisor the divisor
         * @param value the value
         */
        public void writeGolomb(int divisor, int value) throws IOException {
            int q = value / divisor;
            for (int i = 0; i < q; i++) {
                writeBit(true, MAX_PROBABILITY / 2);
            }
            writeBit(false, MAX_PROBABILITY / 2);
            int r = value - q * divisor;
            int bit = 31 - Integer.numberOfLeadingZeros(divisor - 1);
            if (r < ((2 << bit) - divisor)) {
                bit--;
            } else {
                r += (2 << bit) - divisor;
            }
            for (; bit >= 0; bit--) {
                writeBit(((r >>> bit) & 1) == 1, MAX_PROBABILITY / 2);
            }
        }

    }

    /**
     * A Huffman code table / tree.
     */
    public static class Huffman {

        private final int[] codes;
        private final Node tree;

        public Huffman(int[] frequencies) {
            PriorityQueue<Node> queue = new PriorityQueue<>();
            for (int i = 0; i < frequencies.length; i++) {
                int f = frequencies[i];
                if (f > 0) {
                    queue.offer(new Node(i, f));
                }
            }
            while (queue.size() > 1) {
                queue.offer(new Node(queue.poll(), queue.poll()));
            }
            codes = new int[frequencies.length];
            tree = queue.poll();
            if (tree != null) {
                tree.initCodes(codes, 1);
            }
        }

        /**
         * Write a value.
         *
         * @param out the output stream
         * @param value the value to write
         */
        public void write(Out out, int value) throws IOException {
            int code = codes[value];
            int bitCount = 30 - Integer.numberOfLeadingZeros(code);
            Node n = tree;
            for (int i = bitCount; i >= 0; i--) {
                boolean goRight = ((code >> i) & 1) == 1;
                int prob = (int) ((long) MAX_PROBABILITY *
                        n.right.frequency / n.frequency);
                out.writeBit(goRight, prob);
                n = goRight ? n.right : n.left;
            }
        }

        /**
         * Read a value.
         *
         * @param in the input stream
         * @return the value
         */
        public int read(In in) throws IOException {
            Node n = tree;
            while (n.left != null) {
                int prob = (int) ((long) MAX_PROBABILITY *
                        n.right.frequency / n.frequency);
                boolean goRight = in.readBit(prob);
                n = goRight ? n.right : n.left;
            }
            return n.value;
        }

    }

    /**
     * A Huffman code node.
     */
    private static class Node implements Comparable<Node> {

        int value;
        Node left;
        Node right;
        final int frequency;

        Node(int value, int frequency) {
            this.frequency = frequency;
            this.value = value;
        }

        Node(Node left, Node right) {
            this.left = left;
            this.right = right;
            this.frequency = left.frequency + right.frequency;
        }

        @Override
        public int compareTo(Node o) {
            return frequency - o.frequency;
        }

        void initCodes(int[] codes, int bits) {
            if (left == null) {
                codes[value] = bits;
            } else {
                left.initCodes(codes, bits << 1);
                right.initCodes(codes, (bits << 1) + 1);
            }
        }

    }

}
