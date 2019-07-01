/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.hash;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * A perfect hash function tool. It needs about 1.4 bits per key, and the
 * resulting hash table is about 79% full. The minimal perfect hash function
 * needs about 2.3 bits per key.
 * <p>
 * Generating the hash function takes about 1 second per million keys
 * for both perfect hash and minimal perfect hash.
 * <p>
 * The algorithm is recursive: sets that contain no or only one entry are not
 * processed as no conflicts are possible. Sets that contain between 2 and 16
 * entries, up to 16 hash functions are tested to check if they can store the
 * data without conflict. If no function was found, the same is tested on a
 * larger bucket (except for the minimal perfect hash). If no hash function was
 * found, and for larger buckets, the bucket is split into a number of smaller
 * buckets (up to 32).
 * <p>
 * At the end of the generation process, the data is compressed using a general
 * purpose compression tool (Deflate / Huffman coding). The uncompressed data is
 * around 1.52 bits per key (perfect hash) and 3.72 (minimal perfect hash).
 * <p>
 * Please also note the MinimalPerfectHash class, which uses less space per key.
 */
public class PerfectHash {

    /**
     * The maximum size of a bucket.
     */
    private static final int MAX_SIZE = 16;

    /**
     * The maximum number of hash functions to test.
     */
    private static final int OFFSETS = 16;

    /**
     * The maximum number of buckets to split the set into.
     */
    private static final int MAX_SPLIT = 32;

    /**
     * The description of the hash function. Used for calculating the hash of a
     * key.
     */
    private final byte[] data;

    /**
     * The offset of the result of the hash function at the given offset within
     * the data array. Used for calculating the hash of a key.
     */
    private final int[] plus;

    /**
     * The position of the next bucket in the data array (in case this bucket
     * needs to be skipped). Used for calculating the hash of a key.
     */
    private final int[] next;

    /**
     * Create a hash object to convert keys to hashes.
     *
     * @param data the data returned by the generate method
     */
    public PerfectHash(byte[] data) {
        this.data = data = expand(data);
        plus = new int[data.length];
        next = new int[data.length];
        for (int i = 0, p = 0; i < data.length; i++) {
            plus[i] = p;
            int n = data[i] & 255;
            p += n < 2 ? n : n >= MAX_SPLIT ? (n / OFFSETS) : 0;
        }
    }

    /**
     * Calculate the hash from the key.
     *
     * @param x the key
     * @return the hash
     */
    public int get(int x) {
        return get(0, x, 0);
    }

    private int get(int pos, int x, int level) {
        int n = data[pos] & 255;
        if (n < 2) {
            return plus[pos];
        } else if (n >= MAX_SPLIT) {
            return plus[pos] + hash(x, level, n % OFFSETS, n / OFFSETS);
        }
        pos++;
        int h = hash(x, level, 0, n);
        for (int i = 0; i < h; i++) {
            pos = read(pos);
        }
        return get(pos, x, level + 1);
    }

    private int read(int pos) {
        int p = next[pos];
        if (p == 0) {
            int n = data[pos] & 255;
            if (n < 2 || n >= MAX_SPLIT) {
                return pos + 1;
            }
            int start = pos++;
            for (int i = 0; i < n; i++) {
                pos = read(pos);
            }
            next[start] = p = pos;
        }
        return p;
    }

    /**
     * Generate the perfect hash function data from the given set of integers.
     *
     * @param list the set
     * @param minimal whether the perfect hash function needs to be minimal
     * @return the data
     */
    public static byte[] generate(Set<Integer> list, boolean minimal) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        generate(list, 0, minimal, out);
        return compress(out.toByteArray());
    }

    private static void generate(Collection<Integer> set, int level,
            boolean minimal, ByteArrayOutputStream out) {
        int size = set.size();
        if (size <= 1) {
            out.write(size);
            return;
        }
        if (size < MAX_SIZE) {
            int max = minimal ? size : Math.min(MAX_SIZE - 1, size * 2);
            for (int s = size; s <= max; s++) {
                // Try a few hash functions ("offset" is basically the hash
                // function index). We could try less hash functions, and
                // instead use a larger size and remember the position of the
                // hole (specially for the minimal perfect case), but that's
                // more complicated.
                nextOffset:
                for (int offset = 0; offset < OFFSETS; offset++) {
                    int bits = 0;
                    for (int x : set) {
                        int h = hash(x, level, offset, s);
                        if ((bits & (1 << h)) != 0) {
                            continue nextOffset;
                        }
                        bits |= 1 << h;
                    }
                    out.write(s * OFFSETS + offset);
                    return;
                }
            }
        }
        // Split the set into multiple smaller sets. We could try to split more
        // evenly by trying out multiple hash functions, but that's more
        // complicated.
        int split;
        if (minimal) {
            split = size > 150 ? size / 83 : (size + 3) / 4;
        } else {
            split = size > 265 ? size / 142 : (size + 5) / 7;
        }
        split = Math.min(MAX_SPLIT - 1, Math.max(2, split));
        out.write(split);
        List<List<Integer>> lists = new ArrayList<>(split);
        for (int i = 0; i < split; i++) {
            lists.add(new ArrayList<Integer>(size / split));
        }
        for (int x : set) {
            lists.get(hash(x, level, 0, split)).add(x);
        }
        for (List<Integer> s2 : lists) {
            generate(s2, level + 1, minimal, out);
        }
    }

    /**
     * Calculate the hash of a key. The result depends on the key, the recursion
     * level, and the offset.
     *
     * @param x the key
     * @param level the recursion level
     * @param offset the index of the hash function
     * @param size the size of the bucket
     * @return the hash (a value between 0, including, and the size, excluding)
     */
    private static int hash(int x, int level, int offset, int size) {
        x += level * OFFSETS + offset;
        x = ((x >>> 16) ^ x) * 0x45d9f3b;
        x = ((x >>> 16) ^ x) * 0x45d9f3b;
        x = (x >>> 16) ^ x;
        return Math.abs(x % size);
    }

    /**
     * Compress the hash description using a Huffman coding.
     *
     * @param d the data
     * @return the compressed data
     */
    private static byte[] compress(byte[] d) {
        Deflater deflater = new Deflater();
        deflater.setStrategy(Deflater.HUFFMAN_ONLY);
        deflater.setInput(d);
        deflater.finish();
        ByteArrayOutputStream out2 = new ByteArrayOutputStream(d.length);
        byte[] buffer = new byte[1024];
        while (!deflater.finished()) {
            int count = deflater.deflate(buffer);
            out2.write(buffer, 0, count);
        }
        deflater.end();
        return out2.toByteArray();
    }

    /**
     * Decompress the hash description using a Huffman coding.
     *
     * @param d the data
     * @return the decompressed data
     */
    private static byte[] expand(byte[] d) {
        Inflater inflater = new Inflater();
        inflater.setInput(d);
        ByteArrayOutputStream out = new ByteArrayOutputStream(d.length);
        byte[] buffer = new byte[1024];
        try {
            while (!inflater.finished()) {
                int count = inflater.inflate(buffer);
                out.write(buffer, 0, count);
            }
            inflater.end();
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
        return out.toByteArray();
    }

}
