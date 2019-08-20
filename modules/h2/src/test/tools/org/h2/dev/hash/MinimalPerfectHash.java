/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.hash;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * A minimal perfect hash function tool. It needs about 1.98 bits per key.
 * <p>
 * The algorithm is recursive: sets that contain no or only one entry are not
 * processed as no conflicts are possible. For sets that contain between 2 and
 * 12 entries, a number of hash functions are tested to check if they can store
 * the data without conflict. If no function was found, and for larger sets, the
 * set is split into a (possibly high) number of smaller set, which are
 * processed recursively. The average size of a top-level bucket is about 216
 * entries, and the maximum recursion level is typically 5.
 * <p>
 * At the end of the generation process, the data is compressed using a general
 * purpose compression tool (Deflate / Huffman coding) down to 2.0 bits per key.
 * The uncompressed data is around 2.2 bits per key. With arithmetic coding,
 * about 1.9 bits per key are needed. Generating the hash function takes about
 * 2.5 seconds per million keys with 8 cores (multithreaded). The algorithm
 * automatically scales with the number of available CPUs (using as many threads
 * as there are processors). At the expense of processing time, a lower number
 * of bits per key would be possible (for example 1.84 bits per key with 100000
 * keys, using 32 seconds generation time, with Huffman coding).
 * <p>
 * The memory usage to efficiently calculate hash values is around 2.5 bits per
 * key (the space needed for the uncompressed description, plus 8 bytes for
 * every top-level bucket).
 * <p>
 * At each level, only one user defined hash function per object is called
 * (about 3 hash functions per key). The result is further processed using a
 * supplemental hash function, so that the default user defined hash function
 * doesn't need to be sophisticated (it doesn't need to be non-linear, have a
 * good avalanche effect, or generate random looking data; it just should
 * produce few conflicts if possible).
 * <p>
 * To protect against hash flooding and similar attacks, a secure random seed
 * per hash table is used. For further protection, cryptographically secure
 * functions such as SipHash or SHA-256 can be used. However, such (slower)
 * functions only need to be used if regular hash functions produce too many
 * conflicts. This case is detected when generating the perfect hash function,
 * by checking if there are too many conflicts (more than 2160 entries in one
 * top-level bucket). In this case, the next hash function is used. That way, in
 * the normal case, where no attack is happening, only fast, but less secure,
 * hash functions are called. It is fine to use the regular hashCode method as
 * the level 0 hash function. However, just relying on the regular hashCode
 * method does not work if the key has more than 32 bits, because the risk of
 * collisions is too high. Incorrect universal hash functions are detected (an
 * exception is thrown if there are more than 32 recursion levels).
 * <p>
 * In-place updating of the hash table is not implemented but possible in
 * theory, by patching the hash function description. With a small change,
 * non-minimal perfect hash functions can be calculated (for example 1.22 bits
 * per key at a fill rate of 81%).
 *
 * @param <K> the key type
 */
public class MinimalPerfectHash<K> {

    /**
     * Large buckets are typically divided into buckets of this size.
     */
    private static final int DIVIDE = 6;

    /**
     * For sets larger than this, instead of trying to map then uniquely to a
     * set of the same size, the size of the set is incremented by one. This
     * reduces the time to find a mapping, but the index of the hole also needs
     * to be stored, which increases the space usage.
     */
    private static final int SPEEDUP = 11;

    /**
     * The maximum size of a small bucket (one that is not further split if
     * possible).
     */
    private static final int MAX_SIZE = 14;

    /**
     * The maximum offset for hash functions of small buckets. At most that many
     * hash functions are tried for the given size.
     */
    private static final int[] MAX_OFFSETS = { 0, 0, 8, 18, 47, 123, 319, 831, 2162,
            5622, 14617, 38006, 98815, 256920, 667993 };

    /**
     * The output value to split the bucket into many (more than 2) smaller
     * buckets.
     */
    private static final int SPLIT_MANY = 3;

    /**
     * The minimum output value for a small bucket of a given size.
     */
    private static final int[] SIZE_OFFSETS = new int[MAX_OFFSETS.length + 1];

    /**
     * A secure random generator.
     */
    private static final SecureRandom RANDOM = new SecureRandom();

    static {
        for (int i = SPEEDUP; i < MAX_OFFSETS.length; i++) {
            MAX_OFFSETS[i] = (int) (MAX_OFFSETS[i] * 2.5);
        }
        int last = SPLIT_MANY + 1;
        for (int i = 0; i < MAX_OFFSETS.length; i++) {
            SIZE_OFFSETS[i] = last;
            last += MAX_OFFSETS[i];
        }
        SIZE_OFFSETS[SIZE_OFFSETS.length - 1] = last;
    }

    /**
     * The universal hash function.
     */
    private final UniversalHash<K> hash;

    /**
     * The description of the hash function. Used for calculating the hash of a
     * key.
     */
    private final byte[] data;

    /**
     * The random seed.
     */
    private final int seed;

    /**
     * The size up to the given root-level bucket in the data array. Used to
     * speed up calculating the hash of a key.
     */
    private final int[] rootSize;

    /**
     * The position of the given root-level bucket in the data array. Used to
     * speed up calculating the hash of a key.
     */
    private final int[] rootPos;

    /**
     * The hash function level at the root of the tree. Typically 0, except if
     * the hash function at that level didn't split the entries as expected
     * (which can be due to a bad hash function, or due to an attack).
     */
    private final int rootLevel;

    /**
     * Create a hash object to convert keys to hashes.
     *
     * @param desc the data returned by the generate method
     * @param hash the universal hash function
     */
    public MinimalPerfectHash(byte[] desc, UniversalHash<K> hash) {
        this.hash = hash;
        byte[] b = data = expand(desc);
        seed = ((b[0] & 255) << 24) |
                ((b[1] & 255) << 16) |
                ((b[2] & 255) << 8) |
                (b[3] & 255);
        if (b[4] == SPLIT_MANY) {
            rootLevel = b[b.length - 1] & 255;
            int split = readVarInt(b, 5);
            rootSize = new int[split];
            rootPos = new int[split];
            int pos = 5 + getVarIntLength(b, 5);
            int sizeSum = 0;
            for (int i = 0; i < split; i++) {
                rootSize[i] = sizeSum;
                rootPos[i] = pos;
                int start = pos;
                pos = getNextPos(pos);
                sizeSum += getSizeSum(start, pos);
            }
        } else {
            rootLevel = 0;
            rootSize = null;
            rootPos = null;
        }
    }

    /**
     * Calculate the hash value for the given key.
     *
     * @param x the key
     * @return the hash value
     */
    public int get(K x) {
        return get(4, x, true, rootLevel);
    }

    /**
     * Get the hash value for the given key, starting at a certain position and
     * level.
     *
     * @param pos the start position
     * @param x the key
     * @param isRoot whether this is the root of the tree
     * @param level the level
     * @return the hash value
     */
    private int get(int pos, K x, boolean isRoot, int level) {
        int n = readVarInt(data, pos);
        if (n < 2) {
            return 0;
        } else if (n > SPLIT_MANY) {
            int size = getSize(n);
            int offset = getOffset(n, size);
            if (size >= SPEEDUP) {
                int p = offset % (size + 1);
                offset = offset / (size + 1);
                int result = hash(x, hash, level, seed, offset, size + 1);
                if (result >= p) {
                    result--;
                }
                return result;
            }
            return hash(x, hash, level, seed, offset, size);
        }
        pos++;
        int split;
        if (n == SPLIT_MANY) {
            split = readVarInt(data, pos);
            pos += getVarIntLength(data, pos);
        } else {
            split = n;
        }
        int h = hash(x, hash, level, seed, 0, split);
        int s;
        if (isRoot && rootPos != null) {
            s = rootSize[h];
            pos = rootPos[h];
        } else {
            int start = pos;
            for (int i = 0; i < h; i++) {
                pos = getNextPos(pos);
            }
            s = getSizeSum(start, pos);
        }
        return s + get(pos, x, false, level + 1);
    }

    /**
     * Get the position of the next sibling.
     *
     * @param pos the position of this branch
     * @return the position of the next sibling
     */
    private int getNextPos(int pos) {
        int n = readVarInt(data, pos);
        pos += getVarIntLength(data, pos);
        if (n < 2 || n > SPLIT_MANY) {
            return pos;
        }
        int split;
        if (n == SPLIT_MANY) {
            split = readVarInt(data, pos);
            pos += getVarIntLength(data, pos);
        } else {
            split = n;
        }
        for (int i = 0; i < split; i++) {
            pos = getNextPos(pos);
        }
        return pos;
    }

    /**
     * The sum of the sizes between the start and end position.
     *
     * @param start the start position
     * @param end the end position (excluding)
     * @return the sizes
     */
    private int getSizeSum(int start, int end) {
        int s = 0;
        for (int pos = start; pos < end;) {
            int n = readVarInt(data, pos);
            pos += getVarIntLength(data, pos);
            if (n < 2) {
                s += n;
            } else if (n > SPLIT_MANY) {
                s += getSize(n);
            } else if (n == SPLIT_MANY) {
                pos += getVarIntLength(data, pos);
            }
        }
        return s;
    }

    private static void writeSizeOffset(ByteArrayOutputStream out, int size,
            int offset) {
        writeVarInt(out, SIZE_OFFSETS[size] + offset);
    }

    private static int getOffset(int n, int size) {
        return n - SIZE_OFFSETS[size];
    }

    private static int getSize(int n) {
        for (int i = 0; i < SIZE_OFFSETS.length; i++) {
            if (n < SIZE_OFFSETS[i]) {
                return i - 1;
            }
        }
        return 0;
    }

    /**
     * Generate the minimal perfect hash function data from the given set of
     * integers.
     *
     * @param set the data
     * @param hash the universal hash function
     * @return the hash function description
     */
    public static <K> byte[] generate(Set<K> set, UniversalHash<K> hash) {
        ArrayList<K> list = new ArrayList<>(set);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int seed = RANDOM.nextInt();
        out.write(seed >>> 24);
        out.write(seed >>> 16);
        out.write(seed >>> 8);
        out.write(seed);
        generate(list, hash, 0, seed, out);
        return compress(out.toByteArray());
    }

    /**
     * Generate the perfect hash function data from the given set of integers.
     *
     * @param list the data, in the form of a list
     * @param hash the universal hash function
     * @param level the recursion level
     * @param seed the random seed
     * @param out the output stream
     */
    static <K> void generate(ArrayList<K> list, UniversalHash<K> hash,
            int level, int seed, ByteArrayOutputStream out) {
        int size = list.size();
        if (size <= 1) {
            out.write(size);
            return;
        }
        if (level > 32) {
            throw new IllegalStateException("Too many recursions; " +
                    " incorrect universal hash function?");
        }
        if (size <= MAX_SIZE) {
            int maxOffset = MAX_OFFSETS[size];
            // get the hash codes - we could stop early
            // if we detect that two keys have the same hash
            int[] hashes = new int[size];
            for (int i = 0; i < size; i++) {
                hashes[i] = hash.hashCode(list.get(i), level, seed);
            }
            // use the supplemental hash function to find a way
            // to make the hash code unique within this group -
            // there might be a much faster way than that, by
            // checking which bits of the hash code matter most
            int testSize = size;
            if (size >= SPEEDUP) {
                testSize++;
                maxOffset /= testSize;
            }
            nextOffset:
            for (int offset = 0; offset < maxOffset; offset++) {
                int bits = 0;
                for (int i = 0; i < size; i++) {
                    int x = hashes[i];
                    int h = hash(x, level, offset, testSize);
                    if ((bits & (1 << h)) != 0) {
                        continue nextOffset;
                    }
                    bits |= 1 << h;
                }
                if (size >= SPEEDUP) {
                    int pos = Integer.numberOfTrailingZeros(~bits);
                    writeSizeOffset(out, size, offset * (size + 1) + pos);
                } else {
                    writeSizeOffset(out, size, offset);
                }
                return;
            }
        }
        int split;
        if (size > 57 * DIVIDE) {
            split = size / (36 * DIVIDE);
        } else {
            split = (size - 47) / DIVIDE;
        }
        split = Math.max(2, split);
        boolean isRoot = level == 0;
        ArrayList<ArrayList<K>> lists;
        do {
            lists = new ArrayList<>(split);
            for (int i = 0; i < split; i++) {
                lists.add(new ArrayList<K>(size / split));
            }
            for (int i = 0; i < size; i++) {
                K x = list.get(i);
                ArrayList<K> l = lists.get(hash(x, hash, level, seed, 0, split));
                l.add(x);
                if (isRoot && split >= SPLIT_MANY &&
                        l.size() > 36 * DIVIDE * 10) {
                    // a bad hash function or attack was detected
                    level++;
                    lists = null;
                    break;
                }
            }
        } while (lists == null);
        if (split >= SPLIT_MANY) {
            out.write(SPLIT_MANY);
        }
        writeVarInt(out, split);
        boolean multiThreaded = isRoot && list.size() > 1000;
        list.clear();
        list.trimToSize();
        if (multiThreaded) {
            generateMultiThreaded(lists, hash, level, seed, out);
        } else {
            for (ArrayList<K> s2 : lists) {
                generate(s2, hash, level + 1, seed, out);
            }
        }
        if (isRoot && split >= SPLIT_MANY) {
            out.write(level);
        }
    }

    private static <K> void generateMultiThreaded(
            final ArrayList<ArrayList<K>> lists,
            final UniversalHash<K> hash,
            final int level,
            final int seed,
            ByteArrayOutputStream out) {
        final ArrayList<ByteArrayOutputStream> outList =
                new ArrayList<>();
        int processors = Runtime.getRuntime().availableProcessors();
        Thread[] threads = new Thread[processors];
        final AtomicInteger success = new AtomicInteger();
        final AtomicReference<Exception> failure = new AtomicReference<>();
        for (int i = 0; i < processors; i++) {
            threads[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        while (true) {
                            ArrayList<K> list;
                            ByteArrayOutputStream temp =
                                    new ByteArrayOutputStream();
                            synchronized (lists) {
                                if (lists.isEmpty()) {
                                    break;
                                }
                                list = lists.remove(0);
                                outList.add(temp);
                            }
                            generate(list, hash, level + 1, seed, temp);
                        }
                    } catch (Exception e) {
                        failure.set(e);
                        return;
                    }
                    success.incrementAndGet();
                }
            };
        }
        for (Thread t : threads) {
            t.start();
        }
        try {
            for (Thread t : threads) {
                t.join();
            }
            if (success.get() != threads.length) {
                Exception e = failure.get();
                if (e != null) {
                    throw new RuntimeException(e);
                }
                throw new RuntimeException("Unknown failure in one thread");
            }
            for (ByteArrayOutputStream temp : outList) {
                out.write(temp.toByteArray());
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Calculate the hash of a key. The result depends on the key, the recursion
     * level, and the offset.
     *
     * @param o the key
     * @param level the recursion level
     * @param seed the random seed
     * @param offset the index of the hash function
     * @param size the size of the bucket
     * @return the hash (a value between 0, including, and the size, excluding)
     */
    private static <K> int hash(K o, UniversalHash<K> hash, int level,
            int seed, int offset, int size) {
        int x = hash.hashCode(o, level, seed);
        x += level + offset * 32;
        x = ((x >>> 16) ^ x) * 0x45d9f3b;
        x = ((x >>> 16) ^ x) * 0x45d9f3b;
        x = (x >>> 16) ^ x;
        return (x & (-1 >>> 1)) % size;
    }

    private static int hash(int x, int level, int offset, int size) {
        x += level + offset * 32;
        x = ((x >>> 16) ^ x) * 0x45d9f3b;
        x = ((x >>> 16) ^ x) * 0x45d9f3b;
        x = (x >>> 16) ^ x;
        return (x & (-1 >>> 1)) % size;
    }

    private static int writeVarInt(ByteArrayOutputStream out, int x) {
        int len = 0;
        while ((x & ~0x7f) != 0) {
            out.write((byte) (0x80 | (x & 0x7f)));
            x >>>= 7;
            len++;
        }
        out.write((byte) x);
        return ++len;
    }

    private static int readVarInt(byte[] d, int pos) {
        int x = d[pos++];
        if (x >= 0) {
            return x;
        }
        x &= 0x7f;
        for (int s = 7; s < 64; s += 7) {
            int b = d[pos++];
            x |= (b & 0x7f) << s;
            if (b >= 0) {
                break;
            }
        }
        return x;
    }

    private static int getVarIntLength(byte[] d, int pos) {
        int x = d[pos++];
        if (x >= 0) {
            return 1;
        }
        int len = 2;
        for (int s = 7; s < 64; s += 7) {
            int b = d[pos++];
            if (b >= 0) {
                break;
            }
            len++;
        }
        return len;
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

    /**
     * An interface that can calculate multiple hash values for an object. The
     * returned hash value of two distinct objects may be the same for a given
     * hash function index, but as more hash functions indexes are called for
     * those objects, the returned value must eventually be different.
     * <p>
     * The returned value does not need to be uniformly distributed.
     *
     * @param <T> the type
     */
    public interface UniversalHash<T> {

        /**
         * Calculate the hash of the given object.
         *
         * @param o the object
         * @param index the hash function index (index 0 is used first, so the
         *            method should be very fast with index 0; index 1 and so on
         *            are only called when really needed)
         * @param seed the random seed (always the same for a hash table)
         * @return the hash value
         */
        int hashCode(T o, int index, int seed);

    }

    /**
     * A sample hash implementation for long keys.
     */
    public static class LongHash implements UniversalHash<Long> {

        @Override
        public int hashCode(Long o, int index, int seed) {
            if (index == 0) {
                return o.hashCode();
            } else if (index < 8) {
                long x = o.longValue();
                x += index;
                x = ((x >>> 32) ^ x) * 0x45d9f3b;
                x = ((x >>> 32) ^ x) * 0x45d9f3b;
                return (int) (x ^ (x >>> 32));
            }
            // get the lower or higher 32 bit depending on the index
            int shift = (index & 1) * 32;
            return (int) (o.longValue() >>> shift);
        }

    }

    /**
     * A sample hash implementation for integer keys.
     */
    public static class StringHash implements UniversalHash<String> {

        @Override
        public int hashCode(String o, int index, int seed) {
            if (index == 0) {
                // use the default hash of a string, which might already be
                // available
                return o.hashCode();
            } else if (index < 8) {
                // use a different hash function, which is fast but not
                // necessarily universal, and not cryptographically secure
                return getFastHash(o, index, seed);
            }
            // this method is supposed to be cryptographically secure;
            // we could use SHA-256 for higher indexes
            return getSipHash24(o, index, seed);
        }

        /**
         * A cryptographically weak hash function. It is supposed to be fast.
         *
         * @param o the string
         * @param index the hash function index
         * @param seed the seed
         * @return the hash value
         */
        public static int getFastHash(String o, int index, int seed) {
            int x = (index * 0x9f3b) ^ seed;
            int result = seed + o.length();
            for (int i = 0; i < o.length(); i++) {
                x = 31 + x * 0x9f3b;
                result ^= x * (1 + o.charAt(i));
            }
            return result;
        }

        /**
         * A cryptographically relatively secure hash function. It is supposed
         * to protected against hash-flooding denial-of-service attacks.
         *
         * @param o the string
         * @param k0 key 0
         * @param k1 key 1
         * @return the hash value
         */
        public static int getSipHash24(String o, long k0, long k1) {
            byte[] b = o.getBytes(StandardCharsets.UTF_8);
            return getSipHash24(b, 0, b.length, k0, k1);
        }

        /**
         * A cryptographically relatively secure hash function. It is supposed
         * to protected against hash-flooding denial-of-service attacks.
         *
         * @param b the data
         * @param start the start position
         * @param end the end position plus one
         * @param k0 key 0
         * @param k1 key 1
         * @return the hash value
         */
        public static int getSipHash24(byte[] b, int start, int end, long k0, long k1) {
            long v0 = k0 ^ 0x736f6d6570736575L;
            long v1 = k1 ^ 0x646f72616e646f6dL;
            long v2 = k0 ^ 0x6c7967656e657261L;
            long v3 = k1 ^ 0x7465646279746573L;
            int repeat;
            for (int off = start; off <= end + 8; off += 8) {
                long m;
                if (off <= end) {
                    m = 0;
                    int i = 0;
                    for (; i < 8 && off + i < end; i++) {
                        m |= ((long) b[off + i] & 255) << (8 * i);
                    }
                    if (i < 8) {
                        m |= ((long) end - start) << 56;
                    }
                    v3 ^= m;
                    repeat = 2;
                } else {
                    m = 0;
                    v2 ^= 0xff;
                    repeat = 4;
                }
                for (int i = 0; i < repeat; i++) {
                    v0 += v1;
                    v2 += v3;
                    v1 = Long.rotateLeft(v1, 13);
                    v3 = Long.rotateLeft(v3, 16);
                    v1 ^= v0;
                    v3 ^= v2;
                    v0 = Long.rotateLeft(v0, 32);
                    v2 += v1;
                    v0 += v3;
                    v1 = Long.rotateLeft(v1, 17);
                    v3 = Long.rotateLeft(v3, 21);
                    v1 ^= v2;
                    v3 ^= v0;
                    v2 = Long.rotateLeft(v2, 32);
                }
                v0 ^= m;
            }
            return (int) (v0 ^ v1 ^ v2 ^ v3);
        }

    }

}
