/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This is a utility class with mathematical helper functions.
 */
public class MathUtils {

    /**
     * The secure random object.
     */
    static SecureRandom cachedSecureRandom;

    /**
     * True if the secure random object is seeded.
     */
    static volatile boolean seeded;

    private MathUtils() {
        // utility class
    }


    /**
     * Round the value up to the next block size. The block size must be a power
     * of two. As an example, using the block size of 8, the following rounding
     * operations are done: 0 stays 0; values 1..8 results in 8, 9..16 results
     * in 16, and so on.
     *
     * @param x the value to be rounded
     * @param blockSizePowerOf2 the block size
     * @return the rounded value
     */
    public static int roundUpInt(int x, int blockSizePowerOf2) {
        return (x + blockSizePowerOf2 - 1) & (-blockSizePowerOf2);
    }

    /**
     * Round the value up to the next block size. The block size must be a power
     * of two. As an example, using the block size of 8, the following rounding
     * operations are done: 0 stays 0; values 1..8 results in 8, 9..16 results
     * in 16, and so on.
     *
     * @param x the value to be rounded
     * @param blockSizePowerOf2 the block size
     * @return the rounded value
     */
    public static long roundUpLong(long x, long blockSizePowerOf2) {
        return (x + blockSizePowerOf2 - 1) & (-blockSizePowerOf2);
    }

    private static synchronized SecureRandom getSecureRandom() {
        if (cachedSecureRandom != null) {
            return cachedSecureRandom;
        }
        // Workaround for SecureRandom problem as described in
        // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6202721
        // Can not do that in a static initializer block, because
        // threads are not started until after the initializer block exits
        try {
            cachedSecureRandom = SecureRandom.getInstance("SHA1PRNG");
            // On some systems, secureRandom.generateSeed() is very slow.
            // In this case it is initialized using our own seed implementation
            // and afterwards (in the thread) using the regular algorithm.
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    try {
                        SecureRandom sr = SecureRandom.getInstance("SHA1PRNG");
                        byte[] seed = sr.generateSeed(20);
                        synchronized (cachedSecureRandom) {
                            cachedSecureRandom.setSeed(seed);
                            seeded = true;
                        }
                    } catch (Exception e) {
                        // NoSuchAlgorithmException
                        warn("SecureRandom", e);
                    }
                }
            };

            try {
                Thread t = new Thread(runnable, "Generate Seed");
                // let the process terminate even if generating the seed is
                // really slow
                t.setDaemon(true);
                t.start();
                Thread.yield();
                try {
                    // normally, generateSeed takes less than 200 ms
                    t.join(400);
                } catch (InterruptedException e) {
                    warn("InterruptedException", e);
                }
                if (!seeded) {
                    byte[] seed = generateAlternativeSeed();
                    // this never reduces randomness
                    synchronized (cachedSecureRandom) {
                        cachedSecureRandom.setSeed(seed);
                    }
                }
            } catch (SecurityException e) {
                // workaround for the Google App Engine: don't use a thread
                runnable.run();
                generateAlternativeSeed();
            }

        } catch (Exception e) {
            // NoSuchAlgorithmException
            warn("SecureRandom", e);
            cachedSecureRandom = new SecureRandom();
        }
        return cachedSecureRandom;
    }

    /**
     * Generate a seed value, using as much unpredictable data as possible.
     *
     * @return the seed
     */
    public static byte[] generateAlternativeSeed() {
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bout);

            // milliseconds and nanoseconds
            out.writeLong(System.currentTimeMillis());
            out.writeLong(System.nanoTime());

            // memory
            out.writeInt(new Object().hashCode());
            Runtime runtime = Runtime.getRuntime();
            out.writeLong(runtime.freeMemory());
            out.writeLong(runtime.maxMemory());
            out.writeLong(runtime.totalMemory());

            // environment
            try {
                String s = System.getProperties().toString();
                // can't use writeUTF, as the string
                // might be larger than 64 KB
                out.writeInt(s.length());
                out.write(s.getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                warn("generateAlternativeSeed", e);
            }

            // host name and ip addresses (if any)
            try {
                // workaround for the Google App Engine: don't use InetAddress
                Class<?> inetAddressClass = Class.forName(
                        "java.net.InetAddress");
                Object localHost = inetAddressClass.getMethod(
                        "getLocalHost").invoke(null);
                String hostName = inetAddressClass.getMethod(
                        "getHostName").invoke(localHost).toString();
                out.writeUTF(hostName);
                Object[] list = (Object[]) inetAddressClass.getMethod(
                        "getAllByName", String.class).invoke(null, hostName);
                Method getAddress = inetAddressClass.getMethod(
                        "getAddress");
                for (Object o : list) {
                    out.write((byte[]) getAddress.invoke(o));
                }
            } catch (Throwable e) {
                // on some system, InetAddress is not supported
                // on some system, InetAddress.getLocalHost() doesn't work
                // for some reason (incorrect configuration)
            }

            // timing (a second thread is already running usually)
            for (int j = 0; j < 16; j++) {
                int i = 0;
                long end = System.currentTimeMillis();
                while (end == System.currentTimeMillis()) {
                    i++;
                }
                out.writeInt(i);
            }

            out.close();
            return bout.toByteArray();
        } catch (IOException e) {
            warn("generateAlternativeSeed", e);
            return new byte[1];
        }
    }

    /**
     * Print a message to system output if there was a problem initializing the
     * random number generator.
     *
     * @param s the message to print
     * @param t the stack trace
     */
    static void warn(String s, Throwable t) {
        // not a fatal problem, but maybe reduced security
        System.out.println("Warning: " + s);
        if (t != null) {
            t.printStackTrace();
        }
    }

    /**
     * Get the value that is equal to or higher than this value, and that is a
     * power of two.
     *
     * @param x the original value
     * @return the next power of two value
     * @throws IllegalArgumentException if x < 0 or x > 0x40000000
     */
    public static int nextPowerOf2(int x) throws IllegalArgumentException {
        if (x == 0) {
            return 1;
        } else if (x < 0 || x > 0x4000_0000 ) {
            throw new IllegalArgumentException("Argument out of range"
                    + " [0x0-0x40000000]. Argument was: " + x);
        }
        x--;
        x |= x >> 1;
        x |= x >> 2;
        x |= x >> 4;
        x |= x >> 8;
        x |= x >> 16;
        return ++x;
    }

    /**
     * Convert a long value to an int value. Values larger than the biggest int
     * value is converted to the biggest int value, and values smaller than the
     * smallest int value are converted to the smallest int value.
     *
     * @param l the value to convert
     * @return the converted int value
     */
    public static int convertLongToInt(long l) {
        if (l <= Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        } else if (l >= Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        } else {
            return (int) l;
        }
    }

    /**
     * Get a cryptographically secure pseudo random long value.
     *
     * @return the random long value
     */
    public static long secureRandomLong() {
        return getSecureRandom().nextLong();
    }

    /**
     * Get a number of pseudo random bytes.
     *
     * @param bytes the target array
     */
    public static void randomBytes(byte[] bytes) {
        ThreadLocalRandom.current().nextBytes(bytes);
    }

    /**
     * Get a number of cryptographically secure pseudo random bytes.
     *
     * @param len the number of bytes
     * @return the random bytes
     */
    public static byte[] secureRandomBytes(int len) {
        if (len <= 0) {
            len = 1;
        }
        byte[] buff = new byte[len];
        getSecureRandom().nextBytes(buff);
        return buff;
    }

    /**
     * Get a pseudo random int value between 0 (including and the given value
     * (excluding). The value is not cryptographically secure.
     *
     * @param lowerThan the value returned will be lower than this value
     * @return the random long value
     */
    public static int randomInt(int lowerThan) {
        return ThreadLocalRandom.current().nextInt(lowerThan);
    }

    /**
     * Get a cryptographically secure pseudo random int value between 0
     * (including and the given value (excluding).
     *
     * @param lowerThan the value returned will be lower than this value
     * @return the random long value
     */
    public static int secureRandomInt(int lowerThan) {
        return getSecureRandom().nextInt(lowerThan);
    }

}
