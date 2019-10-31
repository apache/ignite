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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.h2.security.AES;

/**
 * Calculate the constant for the secondary / supplemental hash function, so
 * that the hash function mixes the input bits as much as possible.
 */
public class CalculateHashConstant implements Runnable {

    private static BitSet primeNumbers = new BitSet();
    private static int[] randomValues;
    private static AtomicInteger high = new AtomicInteger(0x20);
    private static Set<Integer> candidates =
            Collections.synchronizedSet(new HashSet<Integer>());

    private int constant;
    private int[] fromTo = new int[32 * 32];

    private final AES aes = new AES();
    {
        aes.setKey("Hello Welt Hallo Welt".getBytes());
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
        Thread[] threads = new Thread[8];
        for (int i = 0; i < 8; i++) {
            threads[i] = new Thread(new CalculateHashConstant());
            threads[i].start();
        }
        for (int i = 0; i < 8; i++) {
            threads[i].join();
        }
        int finalCount = 10000;
        int[] randomValues = getRandomValues(finalCount, 10);

        System.out.println();
        System.out.println("AES:");
        CalculateHashConstant test = new CalculateHashConstant();
        int[] minMax;
        int av = 0;
        test = new CalculateHashConstant() {
            @Override
            public int hash(int x) {
                return secureHash(x);
            }
        };
        minMax = test.getDependencies(test, randomValues);
        System.out.println("Dependencies: " + minMax[0] + ".." + minMax[1]);
        av = 0;
        for (int j = 0; j < 100; j++) {
            av += test.getAvalanche(test, randomValues[j]);
        }
        System.out.println("AvalancheSum: " + av);
        minMax = test.getEffect(test, finalCount * 10, 11);
        System.out.println("Effect: " + minMax[0] + ".." + minMax[1]);

        test = new CalculateHashConstant();
        int best = 0;
        int dist = Integer.MAX_VALUE;
        for (int i : new int[] {
                0x10b5383, 0x10b65f3, 0x1170d8b, 0x118e97b,
                0x1190d8b, 0x11ab37d, 0x11c65f3, 0x1228357, 0x122a837,
                0x122a907, 0x12b24f7, 0x12c4d05, 0x131a907, 0x131afa3,
                0x131b683, 0x132927d, 0x13298fb, 0x134a837, 0x13698fb,
                0x136afa3, 0x138da0b, 0x138f563, 0x13957c5, 0x1470b1b,
                0x148e97b, 0x14b5827, 0x150a837, 0x151c97d, 0x151ed3d,
                0x1525707, 0x1534b07, 0x1570b9b, 0x158d283, 0x15933eb,
                0x15947cb, 0x15b5f33, 0x166da7d, 0x16af4a3, 0x16b0b47,
                0x16ca907, 0x16ee585, 0x17609a3, 0x1770b23, 0x17a2507,
                0x1855383, 0x18b5383, 0x18d9f3b, 0x18db37d, 0x1922507,
                0x1960d3d, 0x1990d4f, 0x1991a7b, 0x19b4b07, 0x19b5383,
                0x19d9f3b, 0x1a2b683, 0x1a40d8b, 0x1a4b08d, 0x1a698fb,
                0x1a6cf9b, 0x1a714bd, 0x1a89283, 0x1aa86c3, 0x1b14e0b,
                0x1b196fb, 0x1b1f2bd, 0x1b30b1b, 0x1b32507, 0x1b44f75,
                0x1b50ce3, 0x1b5927d, 0x1b6afa3, 0x1b8a6f7, 0x1baa907,
                0x1bb2507, 0x1c2a6f7, 0x1c48357, 0x1c957c5, 0x1cb8357,
                0x1cc4d05, 0x1cd9283, 0x1ce2683, 0x1d198fb, 0x1d45383,
                0x1d4ed3d, 0x1d598fb, 0x1d8d283, 0x1d95f3b, 0x1db24f7,
                0x1db5383, 0x1db997d, 0x1e465f3, 0x1f198fb, 0x1f2b683,
                0x1f4a837, 0x20998fb, 0x20eb683, 0x214a907, 0x2152ec3,
                0x2169f3b, 0x216ed3d, 0x218a6f7, 0x2194b07, 0x21c5707,
                0x22158f9, 0x2250d4f, 0x2252507, 0x2297d63, 0x22aed3d,
                0x22b5383, 0x22ca7d7, 0x23596fb, 0x23633eb, 0x23957c5,
                0x23b24f7, 0x2476e7b, 0x24a57c5, 0x24b5383, 0x252ebb7,
                0x254f547, 0x258d283, 0x2595707, 0x25957c5, 0x25c5707,
                0x262a837, 0x2638357, 0x2645827, 0x265f2bd, 0x266f2bd,
                0x268a6f7, 0x26a0b23, 0x26cce7b, 0x2730b47, 0x2750b23,
                0x275b683, 0x28465f3, 0x2850d4f, 0x28d0b47, 0x291c97d,
                0x2922507, 0x2941a7b, 0x294a907, 0x294b683, 0x295f2bd,
                0x2969f3b, 0x296cfb5, 0x2976e7b, 0x2989f3b, 0x29933eb,
                0x29a907, 0x29b2683, 0x29c8357, 0x29d9f3b, 0x2a1a907,
                0x2a45383, 0x2a52f75, 0x2a85383, 0x2aacf9b, 0x2ac5f33,
                0x2ad1a7b, 0x2ad2507, 0x2b2b683, 0x2b3af8b, 0x2b63e65,
                0x2b8da0b, 0x2b9416b, 0x2bb24f7, 0x2c4b37d, 0x2c6cf9b,
                0x2ca0d13, 0x2cb2507, 0x2cb2983, 0x2cce97b, 0x2ce305b,
                0x2ceb683, 0x2d14e0b, 0x2d1a7b, 0x2d2507, 0x2d2af8b, 0x2d41a7b,
                0x2d7467b, 0x2d8d283, 0x2d960bb, 0x2dab683, 0x2db5f33,
                0x2dd5f3b, 0x2e2ebb7, 0x2e32e85, 0x2e6a7c9, 0x2e85383,
                0x2e8e585, 0x2e960bb, 0x2f3afa3, 0x2f62fa5, 0x30e8639,
                0x3132983, 0x3150d4f, 0x315cf9b, 0x3162fa5, 0x316ce7b,
                0x31914bd, 0x31a927d, 0x31ea83b, 0x31f1a7b, 0x3285383,
                0x3289f3b, 0x32933eb, 0x329afa3, 0x32a5707, 0x32a6f7,
                0x32ae585, 0x32b1a7b, 0x32b4b07, 0x32b5383, 0x32b5827,
                0x32ee97b, 0x330be5b, 0x3314e0b, 0x33317a5, 0x333af8b,
                0x335afa3, 0x335b37d, 0x3371a7b, 0x3393e65, 0x339a907,
                0x33d1a7b, 0x3425707, 0x34606d3, 0x347b37d, 0x349305b,
                0x34b2683, 0x34b683, 0x34dafa3, 0x34ec97d, 0x3512683,
                0x3515383, 0x3515707, 0x352e585, 0x353af75, 0x354b683,
                0x355ed3d, 0x3562fa5, 0x356f2bd, 0x3574135, 0x359afa3,
                0x35ad283, 0x35b2683, 0x35d9f3b, 0x361ed3d, 0x3671a7b,
                0x3672fa5, 0x36cbe5b, 0x37598fb, 0x375c97d, 0x37ca907,
                0x389a907, 0x38c65f3, 0x38cf547, 0x38e33eb, 0x3931a7b,
                0x39598fb, 0x3979283, 0x398ce7b, 0x39933eb, 0x39960bb,
                0x39b1a87, 0x39b5f33, 0x39c8333, 0x39d2507, 0x3a55827,
                0x3a89f3b, 0x3a9f2bd, 0x3ab2983, 0x3aba7d7, 0x3adafa3,
                0x3b196fb, 0x3b29f3b, 0x3b32e85, 0x3b4e97b, 0x3b9260b,
                0x3bb5383, 0x3c4a907, 0x3c4c97d, 0x3c6cf9b, 0x3c95707,
                0x3ca57c5, 0x3caa907, 0x3cb2683, 0x3cb2983, 0x3ce305b,
                0x3d158f9, 0x3d15f65, 0x3d2c685, 0x3d34b07, 0x3d76e7b,
                0x3d8d283, 0x3d8f563, 0x3dae585, 0x3dd60bb, 0x3e5c97d,
                0x3eb2683, 0x3eb467b, 0x3f5927d, 0x3f596fb, 0x414e585,
                0x424b37d, 0x425afa3, 0x42e5383, 0x4315f65, 0x4325707,
                0x434b683, 0x4485383, 0x448e97b, 0x44996fb, 0x44b3e65,
                0x44eafa3, 0x4515707, 0x4532983, 0x4533e65, 0x453e585,
                0x454b683, 0x454e97b, 0x45598fb, 0x455a907, 0x456f2bd,
                0x4595f3b, 0x45d9f3b, 0x461a907, 0x462a837, 0x4645827,
                0x46a2fa5, 0x46aa213, 0x46acf9b, 0x46b2507, 0x46ba837,
                0x4715707, 0x472f547, 0x475c97d, 0x4799283, 0x47a0d3d,
                0x47ca907, 0x482a907, 0x4835707, 0x4844b07, 0x48933eb,
                0x48ab683, 0x48b5827, 0x491d0e7, 0x4922507, 0x4929f3b,
                0x492e97b, 0x4933eb, 0x4934b07, 0x49598fb, 0x495f2bd,
                0x4962fa5, 0x496b683, 0x499da7d, 0x499ed3d, 0x49ab683,
                0x4a2f547, 0x4a32e85, 0x4a5f2bd, 0x4a696fb, 0x4aacf9b,
                0x4ab5383, 0x4aba837, 0x4aeb683, 0x4aeda7d, 0x4b0f69b,
                0x4b12fa5, 0x4b13e65, 0x4b5a907, 0x4b5afa3, 0x4b8afa3,
                0x4b8e585, 0x4b8f58d, 0x4ba5707, 0x4bc5707, 0x4c2a837,
                0x4c5b37d, 0x4c5ed3d, 0x4c67d8d, 0x4c8f58d, 0x4c933eb,
                0x4caec5d, 0x4cd17a5, 0x4ce305b, 0x4cf179b, 0x4cfc979,
                0x4d0be5b, 0x4d1ed3d, 0x4d2507, 0x4d30d8b, 0x4d32983,
                0x4d40ae5, 0x4d40d8b, 0x4d4f697, 0x4d8e97b, 0x4d9f2bd,
                0x4da0d3d, 0x4dce585, 0x4dd1a7b, 0x4df467b, 0x4e25707,
                0x4e4c97d, 0x4e63833, 0x4eb1a7b, 0x4eb2683, 0x4eb683,
                0x4ece97b, 0x4eee585, 0x4f13e65, 0x4f6afa3, 0x504b37d,
                0x50e2683, 0x50eb683, 0x5132e85, 0x514b08d, 0x516e97b,
                0x5198fb, 0x519f2bd, 0x51ab37d, 0x51b2683, 0x522a837,
                0x5232983, 0x52465f3, 0x52660bb, 0x528b3ed, 0x5294193,
                0x5294e0b, 0x52a57c5, 0x52eaf8b, 0x52eda7d, 0x52ee585,
                0x530be93, 0x5314e0b, 0x532e97b, 0x5340b1b, 0x535279d,
                0x53598fb, 0x5429283, 0x54a0b23, 0x54b24f7, 0x54d17a5,
                0x54eb683, 0x55198fb, 0x5523e65, 0x55357c5, 0x553e585,
                0x555cf9b, 0x55a24f7, 0x55d2507, 0x55eda7d, 0x5645f33,
                0x567ecdd, 0x56a0d3d, 0x571305b, 0x5714e0b, 0x574a837,
                0x57a5707, 0x57b65f3, 0x57c65f3, 0x5879283, 0x58ba837,
                0x58ded3d, 0x59598fb, 0x5989f3b, 0x598a83b, 0x59957c5,
                0x599f2bd, 0x59bd37b, 0x59ca907, 0x59d9f3b, 0x59e60bb,
                0x5a4b37d, 0x5a64133, 0x5a6cf9b, 0x5a89f3b, 0x5a927d,
                0x5a94165, 0x5a94193, 0x5a958f9, 0x5a960bb, 0x5aac3d3,
                0x5ad98fb, 0x5ae98fb, 0x5b198fb, 0x5b2e97b, 0x5b40b5d,
                0x5b5c97d, 0x5b75f33, 0x5b8afa3, 0x5b94e0b, 0x5ba24f7,
                0x5cab683, 0x5cb0b47, 0x5cb0ce5, 0x5cba7d7, 0x5d0be93,
                0x5d12683, 0x5d20cc7, 0x5d3e585, 0x5e2a907, 0x5e3467b,
                0x5e5c97d, 0x5e89f3b, 0x5eb2683, 0x5f598fb, 0x5f5a907,
                0x676e7b, 0x695705, 0x6b2983, 0x8998fb, 0x8ab683, 0x94a837,
                0x95f2bd, 0x991a7b, 0x995705, 0xa714bd, 0xa90a63, 0xa933eb,
                0xad98fb, 0xb365f3, 0xb4a907, 0xb598fb, 0xb5c97d, 0xb5ed3d,
                0xb698fb, 0xbd279d, 0xc55383, 0xc7b37d, 0xc8da0b, 0xca0b23,
                0xca96fb, 0xcacf9b, 0xcb2683, 0xcd1a7b, 0xd45383, 0xd4e585,
                0xd6afa3, 0xd94e0b, 0xdaf547, 0xdb1a7b, 0xdca907, 0xdd2e85,
                0xe6da7d, 0xe94e0b, 0xe9a907, 0xeca7d7, 0xf4a837
        }) {
        // for(int i : candidates) {
            test.constant = i;
            System.out.println();
            System.out.println("Constant: 0x" + Integer.toHexString(i));
            minMax = test.getDependencies(test, randomValues);
            System.out.println("Dependencies: " + minMax[0] + ".." + minMax[1]);
            int d = minMax[1] - minMax[0];
            av = 0;
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
        System.out.println("Best constant: 0x" + Integer.toHexString(best));
        test.constant = best;
        long collisions = test.getCollisionCount();
        System.out.println("Collisions: " + collisions);
    }

    /**
     * Calculate the multiplicative inverse of a value (int).
     *
     * @param a the value
     * @return the multiplicative inverse
     */
    static long calcMultiplicativeInverse(long a) {
        return BigInteger.valueOf(a).modPow(
                BigInteger.valueOf((1 << 31) - 1), BigInteger.valueOf(1L << 32)).longValue();
    }

    /**
     * Calculate the multiplicative inverse of a value (long).
     *
     * @param a the value
     * @return the multiplicative inverse
     */
    static long calcMultiplicativeInverseLong(long a) {
        BigInteger oneShift64 = BigInteger.valueOf(1).shiftLeft(64);
        BigInteger oneShift63 = BigInteger.valueOf(1).shiftLeft(63);
        return BigInteger.valueOf(a).modPow(
                oneShift63.subtract(BigInteger.ONE),
                oneShift64).longValue();
    }
    /**
     * Store a random file to be analyzed by the Diehard test.
     */
    void storeRandomFile() throws Exception {
        File f = new File(System.getProperty("user.home") + "/temp/rand.txt");
        FileOutputStream out = new FileOutputStream(f);
        CalculateHashConstant test = new CalculateHashConstant();
        // Random r = new Random(1);
        byte[] buff = new byte[4];
        // tt.constant = 0x29a907;
        for (int i = 0; i < 10000000 / 8; i++) {
            int y = test.hash(i);
            // int y = r.nextInt();
            writeInt(buff, 0, y);
            out.write(buff);
        }
        out.close();
    }

    private static int[] getRandomValues(int count, int seed) {
        int[] values = new int[count];
        Random r = new Random(seed);
        for (int i = 0; i < count; i++) {
            values[i] = r.nextInt();
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

    private void addCandidates(int currentHigh) {
        for (int low = 0; low <= 0xffff; low++) {
            // the lower 16 bits don't have to be a prime number
            // but it seems that's a good restriction
            if (!primeNumbers.get(low)) {
                continue;
            }
            int i = (currentHigh << 16) | low;
            constant = i;
            // after one bit changes in the input,
            // on average 16 bits of the output change
            int av = getAvalanche(this, 0);
            if (Math.abs(av - 16000) > 130) {
                continue;
            }
            av = getAvalanche(this, 0xffffffff);
            if (Math.abs(av - 16000) > 130) {
                continue;
            }
            long es = getEffectSquare(this, randomValues);
            if (es > 259000) {
                continue;
            }
            int[] minMax = getEffect(this, 10000, 1);
            if (!isWithin(4800, 5200, minMax)) {
                continue;
            }
            minMax = getDependencies(this, randomValues);
            // aes: 7788..8183
            if (!isWithin(7720, 8240, minMax)) {
                continue;
            }
            minMax = getEffect(this, 100000, 3);
            if (!isWithin(49000, 51000, minMax)) {
                continue;
            }
            System.out.println(Integer.toHexString(i) +
                    " hit av:" + av + " bits:" + Integer.bitCount(i) + " es:" + es + " prime:" +
                    BigInteger.valueOf(i).isProbablePrime(15));
            candidates.add(i);
        }
    }

    long getCollisionCount() {
        BitSet set = new BitSet();
        BitSet neg = new BitSet();
        long collisions = 0;
        long t = System.nanoTime();
        for (int i = Integer.MIN_VALUE; i != Integer.MAX_VALUE; i++) {
            int x = hash(i);
            if (x >= 0) {
                if (set.get(x)) {
                    collisions++;
                } else {
                    set.set(x);
                }
            } else {
                x = -(x + 1);
                if (neg.get(x)) {
                    collisions++;
                } else {
                    neg.set(x);
                }
            }
            if ((i & 0xfffff) == 0) {
                long n = System.nanoTime();
                if (n - t > TimeUnit.SECONDS.toNanos(5)) {
                    System.out.println(Integer.toHexString(constant) + " " +
                            Integer.toHexString(i) + " collisions: " + collisions);
                    t = n;
                }
            }
        }
        return collisions;
    }

    private static boolean isWithin(int min, int max, int[] range) {
        return range[0] >= min && range[1] <= max;
    }

    /**
     * Calculate how much the bit changes (output bits that change if an input
     * bit is changed) are independent of each other.
     *
     * @param h      the hash object
     * @param values the values to test with
     * @return the minimum and maximum number of output bits that are changed in
     *         combination with another output bit
     */
    int[] getDependencies(CalculateHashConstant h, int[] values) {
        Arrays.fill(fromTo, 0);
        for (int x : values) {
            for (int shift = 0; shift < 32; shift++) {
                int x1 = h.hash(x);
                int x2 = h.hash(x ^ (1 << shift));
                int x3 = x1 ^ x2;
                for (int s = 0; s < 32; s++) {
                    if ((x3 & (1 << s)) != 0) {
                        for (int s2 = 0; s2 < 32; s2++) {
                            if (s == s2) {
                                continue;
                            }
                            if ((x3 & (1 << s2)) != 0) {
                                fromTo[s * 32 + s2]++;
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
    int getAvalanche(CalculateHashConstant h, int value) {
        int changedBitsSum = 0;
        for (int i = 0; i < 32; i++) {
            int x = value ^ (1 << i);
            for (int shift = 0; shift < 32; shift++) {
                int x1 = h.hash(x);
                int x2 = h.hash(x ^ (1 << shift));
                int x3 = x1 ^ x2;
                changedBitsSum += Integer.bitCount(x3);
            }
        }
        return changedBitsSum * 1000 / 32 / 32;
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
    long getEffectSquare(CalculateHashConstant h, int[] values) {
        Arrays.fill(fromTo, 0);
        int total = 0;
        for (int x : values) {
            for (int shift = 0; shift < 32; shift++) {
                int x1 = h.hash(x);
                int x2 = h.hash(x ^ (1 << shift));
                int x3 = x1 ^ x2;
                for (int s = 0; s < 32; s++) {
                    if ((x3 & (1 << s)) != 0) {
                        fromTo[shift * 32 + s]++;
                        total++;
                    }
                }
            }
        }
        long sqDist = 0;
        int expected = total / 32 / 32;
        for (int x : fromTo) {
            int dist = Math.abs(x - expected);
            sqDist += dist * dist;
        }
        return sqDist;
    }

    /**
     * Calculate if the all bit changes (that an output bit changes if an input
     * bit is changed) are within a certain range.
     *
     * @param h the hash object
     * @param count the number of values to test
     * @param seed the random seed
     * @return the minimum and maximum value of all input-to-output bit changes
     */
    int[] getEffect(CalculateHashConstant h, int count, int seed) {
        Random r = new Random();
        r.setSeed(seed);
        Arrays.fill(fromTo, 0);
        for (int i = 0; i < count; i++) {
            int x = r.nextInt();
            for (int shift = 0; shift < 32; shift++) {
                int x1 = h.hash(x);
                int x2 = h.hash(x ^ (1 << shift));
                int x3 = x1 ^ x2;
                for (int s = 0; s < 32; s++) {
                    if ((x3 & (1 << s)) != 0) {
                        fromTo[shift * 32 + s]++;
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
    int hash(int x) {
        // return secureHash(x);
        x = ((x >>> 16) ^ x) * constant;
        x = ((x >>> 16) ^ x) * constant;
        x = (x >>> 16) ^ x;
        return x;
    }

    /**
     * Calculate a hash using AES.
     *
     * @param x the input
     * @return the output
     */
    int secureHash(int x) {
        Arrays.fill(data, (byte) 0);
        writeInt(data, 0, x);
        aes.encrypt(data, 0, 16);
        return readInt(data, 0);
    }

    private static void writeInt(byte[] buff, int pos, int x) {
        buff[pos++] = (byte) (x >> 24);
        buff[pos++] = (byte) (x >> 16);
        buff[pos++] = (byte) (x >> 8);
        buff[pos++] = (byte) x;
    }

    private static int readInt(byte[] buff, int pos) {
        return (buff[pos++] << 24) + ((buff[pos++] & 0xff) << 16) +
                ((buff[pos++] & 0xff) << 8) + (buff[pos] & 0xff);
    }

}
