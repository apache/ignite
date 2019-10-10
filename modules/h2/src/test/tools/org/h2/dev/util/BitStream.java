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
 * A stream that supports Golomb and Huffman coding.
 */
public class BitStream {

    private BitStream() {
        // a utility class
    }

    /**
     * A bit input stream.
     */
    public static class In {

        private final InputStream in;
        private int current = 0x10000;

        public In(InputStream in) {
            this.in = in;
        }

        /**
         * Read a value that is stored as a Golomb code.
         *
         * @param divisor the divisor
         * @return the value
         */
        public int readGolomb(int divisor) {
            int q = 0;
            while (readBit() == 1) {
                q++;
            }
            int bit = 31 - Integer.numberOfLeadingZeros(divisor - 1);
            int r = 0;
            if (bit >= 0) {
                int cutOff = (2 << bit) - divisor;
                for (; bit > 0; bit--) {
                    r = (r << 1) + readBit();
                }
                if (r >= cutOff) {
                    r = (r << 1) + readBit() - cutOff;
                }
            }
            return q * divisor + r;
        }

        /**
         * Read a bit.
         *
         * @return the bit (0 or 1)
         */
        public int readBit() {
            if (current >= 0x10000) {
                try {
                    current = 0x100 | in.read();
                    if (current < 0) {
                        return -1;
                    }
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
            int bit = (current >>> 7) & 1;
            current <<= 1;
            return bit;
        }

        /**
         * Close the stream. This will also close the underlying stream.
         */
        public void close() {
            try {
                in.close();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

    }

    /**
     * A bit output stream.
     */
    public static class Out {

        private final OutputStream out;
        private int current = 1;

        public Out(OutputStream out) {
            this.out = out;
        }

        /**
         * Write the Golomb code of a value.
         *
         * @param divisor the divisor
         * @param value the value
         */
        public void writeGolomb(int divisor, int value) {
            int q = value / divisor;
            for (int i = 0; i < q; i++) {
                writeBit(1);
            }
            writeBit(0);
            int r = value - q * divisor;
            int bit = 31 - Integer.numberOfLeadingZeros(divisor - 1);
            if (r < ((2 << bit) - divisor)) {
                bit--;
            } else {
                r += (2 << bit) - divisor;
            }
            for (; bit >= 0; bit--) {
                writeBit((r >>> bit) & 1);
            }
        }

        /**
         * Get the size of the Golomb code for this value.
         *
         * @param divisor the divisor
         * @param value the value
         * @return the number of bits
         */
        public static int getGolombSize(int divisor, int value) {
            int q = value / divisor;
            int r = value - q * divisor;
            int bit = 31 - Integer.numberOfLeadingZeros(divisor - 1);
            if (r < ((2 << bit) - divisor)) {
                bit--;
            }
            return bit + q + 2;
        }

        /**
         * Write a bit.
         *
         * @param bit the bit (0 or 1)
         */
        public void writeBit(int bit) {
            current = (current << 1) + bit;
            if (current > 0xff) {
                try {
                    out.write(current & 0xff);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
                current = 1;
            }
        }

        /**
         * Flush the stream. This will at write at most 7 '0' bits.
         * This will also flush the underlying stream.
         */
        public void flush() {
            while (current > 1) {
                writeBit(0);
            }
            try {
                out.flush();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        /**
         * Flush and close the stream.
         * This will also close the underlying stream.
         */
        public void close() {
            flush();
            try {
                out.close();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }

        }

    }

    /**
     * A Huffman code.
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
        public void write(BitStream.Out out, int value) {
            int code = codes[value];
            int bitCount = 30 - Integer.numberOfLeadingZeros(code);
            for (int i = bitCount; i >= 0; i--) {
                out.writeBit((code >> i) & 1);
            }
        }

        /**
         * Read a value.
         *
         * @param in the input stream
         * @return the value
         */
        public int read(BitStream.In in) {
            Node n = tree;
            while (n.left != null) {
                n = in.readBit() == 1 ? n.right : n.left;
            }
            return n.value;
        }

        /**
         * Get the number of bits of the Huffman code for this value.
         *
         * @param value the value
         * @return the number of bits
         */
        public int getBitCount(int value) {
            int code = codes[value];
            return 30 - Integer.numberOfLeadingZeros(code);
        }

    }

    /**
     * A Huffman code node.
     */
    private static class Node implements Comparable<Node> {

        int value;
        Node left;
        Node right;
        private final int frequency;

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
