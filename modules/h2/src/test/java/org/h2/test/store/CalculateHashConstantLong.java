/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.io.File;
import java.io.FileOutputStream;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.h2.security.AES;

/**
 * Calculate the constant for the secondary hash function, so that the hash
 * function mixes the input bits as much as possible.
 */
public class CalculateHashConstantLong implements Runnable {

    private static BitSet primeNumbers = new BitSet();
    private static long[] randomValues;
    private static AtomicInteger high = new AtomicInteger(0x20);
    private static Set<Long> candidates =
            Collections.synchronizedSet(new HashSet<Long>());

    private long constant;
    private int[] fromTo = new int[64 * 64];

    private final AES aes = new AES();
    {
        aes.setKey("Hello World Hallo Welt".getBytes());
    }
    private final byte[] data = new byte[16];

    /**
     * Run just this test.
     *
     * @param args ignored
     */
    public static void main(String... args) throws Exception {
        for (int i = 0x0; i < 0x10000; i++) {
            if (BigInteger.valueOf(i).isProbablePrime(20)) {
                primeNumbers.set(i);
            }
        }
        randomValues = getRandomValues(1000, 1);
        Random r = new Random(1);
        for (int i = 0; i < randomValues.length; i++) {
            randomValues[i] = r.nextInt();
        }
        printQuality(new CalculateHashConstantLong() {
            @Override
            public long hash(long x) {
                return secureHash(x);
            }
            @Override
            public String toString() {
                return "AES";
            }
        }, randomValues);
        // Quality of AES
        // Dependencies: 15715..16364
        // Avalanche: 31998
        // AvalancheSum: 3199841
        // Effect: 49456..50584

        printQuality(new CalculateHashConstantLong() {
            @Override
            public long hash(long x) {
                x = (x ^ (x >>> 30)) * 0xbf58476d1ce4e5b9L;
                x = (x ^ (x >>> 27)) * 0x94d049bb133111ebL;
                return x ^ (x >>> 31);
            }
            @Override
            public String toString() {
                return "Test";
            }
        }, randomValues);
        // Quality of Test
        // Dependencies: 14693..16502
        // Avalanche: 31996
        // AvalancheSum: 3199679
        // Effect: 49437..50537

        Thread[] threads = new Thread[8];
        for (int i = 0; i < 8; i++) {
            threads[i] = new Thread(new CalculateHashConstantLong());
            threads[i].start();
        }
        for (int i = 0; i < 8; i++) {
            threads[i].join();
        }

        int finalCount = 10000;
        long[] randomValues = getRandomValues(finalCount, 10);

        CalculateHashConstantLong test;
        int[] minMax;
        test = new CalculateHashConstantLong();
        long best = 0;
        int dist = Integer.MAX_VALUE;
        for (long i : candidates) {
            test.constant = i;
            System.out.println();
            System.out.println("Constant: 0x" + Long.toHexString(i));
            minMax = test.getDependencies(test, randomValues);
            System.out.println("Dependencies: " + minMax[0] + ".." + minMax[1]);
            int d = minMax[1] - minMax[0];
            int av = 0;
            for (int j = 0; j < 100; j++) {
                av += test.getAvalanche(test, randomValues[j]);
            }
            System.out.println("AvalancheSum: " + av);
            minMax = test.getEffect(test, finalCount * 10, 11);
            System.out.println("Effect: " + minMax[0] + ".." + minMax[1]);
            d += minMax[1] - minMax[0];
            if (d < dist) {
                dist = d;
                best = i;
            }
        }
        System.out.println();
        System.out.println("Best constant: 0x" + Long.toHexString(best));
        test.constant = best;
        long collisions = test.getCollisionCount();
        System.out.println("Collisions: " + collisions);
    }

    private static void printQuality(CalculateHashConstantLong test, long[] randomValues) {
        int finalCount = randomValues.length * 10;
        System.out.println("Quality of " + test);
        int[] minMax;
        int av = 0;
        minMax = test.getDependencies(test, randomValues);
        System.out.println("Dependencies: " + minMax[0] + ".." + minMax[1]);
        av = 0;
        for (int j = 0; j < 100; j++) {
            av += test.getAvalanche(test, randomValues[j]);
        }
        System.out.println("Avalanche: " + (av / 100));
        System.out.println("AvalancheSum: " + av);
        minMax = test.getEffect(test, finalCount * 10, 11);
        System.out.println("Effect: " + minMax[0] + ".." + minMax[1]);
        System.out.println("ok=" + test.testCandidate());
    }

    /**
     * Store a random file to be analyzed by the Diehard test.
     */
    void storeRandomFile() throws Exception {
        File f = new File(System.getProperty("user.home") + "/temp/rand.txt");
        FileOutputStream out = new FileOutputStream(f);
        CalculateHashConstantLong test = new CalculateHashConstantLong();
        // Random r = new Random(1);
        byte[] buff = new byte[8];
        // tt.constant = 0x29a907;
        for (int i = 0; i < 10000000 / 8; i++) {
            long y = test.hash(i);
            // int y = r.nextInt();
            writeLong(buff, 0, y);
            out.write(buff);
        }
        out.close();
    }

    private static long[] getRandomValues(int count, int seed) {
        long[] values = new long[count];
        Random r = new Random(seed);
        for (int i = 0; i < count; i++) {
            values[i] = r.nextLong();
        }
        return values;
    }

    @Override
    public void run() {
        while (true) {
            int currentHigh = high.getAndIncrement();
            // if (currentHigh > 0x2d) {
            if (currentHigh > 0xffff) {
                break;
            }
            System.out.println("testing " + Integer.toHexString(currentHigh) + "....");
            addCandidates(currentHigh);
        }
    }

    private void addCandidates(long currentHigh) {
        for (int low = 0; low <= 0xffff; low++) {
            // the lower 16 bits don't have to be a prime number
            // but it seems that's a good restriction
            if (!primeNumbers.get(low)) {
                continue;
            }
            long i = (currentHigh << 48) | ((long) low << 32) | (currentHigh << 16) | low;
            constant = i;
            if (!testCandidate()) {
                continue;
            }
            System.out.println(Long.toHexString(i) +
                    " hit " + i);
            candidates.add(i);
        }
    }

    private boolean testCandidate() {
        // after one bit changes in the input,
        // on average 32 bits of the output change
        int av = getAvalanche(this, 0);
        if (Math.abs(av - 32000) > 1000) {
            return false;
        }
        av = getAvalanche(this, 0xffffffffffffffffL);
        if (Math.abs(av - 32000) > 1000) {
            return false;
        }
        long es = getEffectSquare(this, randomValues);
        if (es > 1100000) {
            System.out.println("fail at a " + es);
            return false;
        }
        int[] minMax = getEffect(this, 10000, 1);
        if (!isWithin(4700, 5300, minMax)) {
            System.out.println("fail at b " + minMax[0] + " " + minMax[1]);
            return false;
        }
        minMax = getDependencies(this, randomValues);
        if (!isWithin(14500, 17000, minMax)) {
            System.out.println("fail at c " + minMax[0] + " " + minMax[1]);
            return false;
        }
        return true;
    }

    long getCollisionCount() {
        // TODO need a way to check this
        return 0;
    }

    private static boolean isWithin(int min, int max, int[] range) {
        return range[0] >= min && range[1] <= max;
    }

    /**
     * Calculate how much the bit changes (output bits that change if an input
     * bit is changed) are independent of each other.
     *
     * @param h the hash object
     * @param values the values to test with
     * @return the minimum and maximum number of output bits that are changed in
     *         combination with another output bit
     */
    int[] getDependencies(CalculateHashConstantLong h, long[] values) {
        Arrays.fill(fromTo, 0);
        for (long x : values) {
            for (int shift = 0; shift < 64; shift++) {
                long x1 = h.hash(x);
                long x2 = h.hash(x ^ (1L << shift));
                long x3 = x1 ^ x2;
                for (int s = 0; s < 64; s++) {
                    if ((x3 & (1L << s)) != 0) {
                        for (int s2 = 0; s2 < 64; s2++) {
                            if (s == s2) {
                                continue;
                            }
                            if ((x3 & (1L << s2)) != 0) {
                                fromTo[s * 64 + s2]++;
                            }
                        }
                    }
                }
            }
        }
        int a = Integer.MAX_VALUE, b = Integer.MIN_VALUE;
        for (int x : fromTo) {
            if (x == 0) {
                continue;
            }
            if (x < a) {
                a = x;
            }
            if (x > b) {
                b = x;
            }
        }
        return new int[] {a, b};
    }

    /**
     * Calculate the number of bits that change if a single bit is changed
     * multiplied by 1000 (expected: 16000 +/- 5%).
     *
     * @param h     the hash object
     * @param value the base value
     * @return the number of bit changes multiplied by 1000
     */
    int getAvalanche(CalculateHashConstantLong h, long value) {
        int changedBitsSum = 0;
        for (int i = 0; i < 64; i++) {
            long x = value ^ (1L << i);
            for (int shift = 0; shift < 64; shift++) {
                long x1 = h.hash(x);
                long x2 = h.hash(x ^ (1L << shift));
                long x3 = x1 ^ x2;
                changedBitsSum += Long.bitCount(x3);
            }
        }
        return changedBitsSum * 1000 / 64 / 64;
    }

    /**
     * Calculate the sum of the square of the distance to the expected
     * probability that an output bit changes if an input bit is changed. The
     * lower the value, the better.
     *
     * @param h      the hash object
     * @param values the values to test with
     * @return sum(distance^2)
     */
    long getEffectSquare(CalculateHashConstantLong h, long[] values) {
        Arrays.fill(fromTo, 0);
        int total = 0;
        for (long x : values) {
            for (int shift = 0; shift < 64; shift++) {
                long x1 = h.hash(x);
                long x2 = h.hash(x ^ (1L << shift));
                long x3 = x1 ^ x2;
                for (int s = 0; s < 64; s++) {
                    if ((x3 & (1L << s)) != 0) {
                        fromTo[shift * 64 + s]++;
                        total++;
                    }
                }
            }
        }
        long sqDist = 0;
        int expected = total / 64 / 64;
        for (int x : fromTo) {
            int dist = Math.abs(x - expected);
            sqDist += dist * dist;
        }
        return sqDist;
    }

    /**
     * Calculate if the bit changes (that an output bit changes if an input
     * bit is changed) are within a certain range.
     *
     * @param h the hash object
     * @param count the number of values to test
     * @param seed the random seed
     * @return the minimum and maximum value of all input-to-output bit changes
     */
    int[] getEffect(CalculateHashConstantLong h, int count, int seed) {
        Random r = new Random();
        r.setSeed(seed);
        Arrays.fill(fromTo, 0);
        for (int i = 0; i < count; i++) {
            long x = r.nextLong();
            for (int shift = 0; shift < 64; shift++) {
                long x1 = h.hash(x);
                long x2 = h.hash(x ^ (1L << shift));
                long x3 = x1 ^ x2;
                for (int s = 0; s < 64; s++) {
                    if ((x3 & (1L << s)) != 0) {
                        fromTo[shift * 64 + s]++;
                    }
                }
            }
        }
        int a = Integer.MAX_VALUE, b = Integer.MIN_VALUE;
        for (int x : fromTo) {
            if (x < a) {
                a = x;
            }
            if (x > b) {
                b = x;
            }
        }
        return new int[] {a, b};
    }

    /**
     * The hash method.
     *
     * @param x the input
     * @return the output
     */
    long hash(long x) {
        x = ((x >>> 32) ^ x) * constant;
        x = ((x >>> 32) ^ x) * constant;
        x = (x >>> 32) ^ x;
        return x;
    }

    /**
     * Calculate a hash using AES.
     *
     * @param x the input
     * @return the output
     */
    long secureHash(long x) {
        writeLong(data, 0, x);
        aes.encrypt(data, 0, 16);
        return readLong(data, 0);
    }

    private static void writeLong(byte[] buff, int pos, long x) {
        writeInt(buff, pos, (int) (x >>> 32));
        writeInt(buff, pos + 4, (int) x);
    }

    private static void writeInt(byte[] buff, int pos, int x) {
        buff[pos++] = (byte) (x >> 24);
        buff[pos++] = (byte) (x >> 16);
        buff[pos++] = (byte) (x >> 8);
        buff[pos++] = (byte) x;
    }

    private static long readLong(byte[] buff, int pos) {
        return (((long) readInt(buff, pos)) << 32) | (readInt(buff, pos + 4) & 0xffffffffL);
    }

    private static int readInt(byte[] buff, int pos) {
        return (buff[pos++] << 24) + ((buff[pos++] & 0xff) << 16) +
                ((buff[pos++] & 0xff) << 8) + (buff[pos] & 0xff);
    }

}
