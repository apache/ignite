/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (http://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.h2.test.unit;

import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.h2.test.TestBase;
import org.h2.util.StringUtils;

/**
 * Tests the string cache facility.
 */
public class TestStringCache extends TestBase {

    /**
     * Flag to indicate the test should stop.
     */
    volatile boolean stop;
    private final Random random = new Random(1);
    private final String[] some = { null, "", "ABC",
            "this is a medium sized string", "1", "2" };
    private boolean useIntern;

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        TestBase.createCaller().init().test();
        new TestStringCache().runBenchmark();
    }

    @Override
    public void test() throws InterruptedException {
        testToUpperToLower();
        StringUtils.clearCache();
        testSingleThread(getSize(5000, 20000));
        testMultiThreads();
    }

    private void testToUpperCache() {
        Random r = new Random();
        String[] test = new String[50];
        for (int i = 0; i < test.length; i++) {
            StringBuilder buff = new StringBuilder();
            for (int a = 0; a < 50; a++) {
                buff.append((char) r.nextInt());
            }
            String a = buff.toString();
            test[i] = a;
        }
        int repeat = 100000;
        int testLen = 0;
        long time = System.nanoTime();
        for (int a = 0; a < repeat; a++) {
            for (String x : test) {
                String y = StringUtils.toUpperEnglish(x);
                testLen += y.length();
            }
        }
        time = System.nanoTime() - time;
        System.out.println("cache " + TimeUnit.NANOSECONDS.toMillis(time));
        time = System.nanoTime();
        for (int a = 0; a < repeat; a++) {
            for (String x : test) {
                String y = x.toUpperCase(Locale.ENGLISH);
                testLen -= y.length();
            }
        }
        time = System.nanoTime() - time;
        System.out.println("toUpperCase " + TimeUnit.NANOSECONDS.toMillis(time));
        assertEquals(0, testLen);
    }

    private void testToUpperToLower() {
        Random r = new Random();
        for (int i = 0; i < 1000; i++) {
            StringBuilder buff = new StringBuilder();
            for (int a = 0; a < 100; a++) {
                buff.append((char) r.nextInt());
            }
            String a = buff.toString();
            String b = StringUtils.toUpperEnglish(a);
            String c = a.toUpperCase(Locale.ENGLISH);
            assertEquals(c, b);
            String d = StringUtils.toLowerEnglish(a);
            String e = a.toLowerCase(Locale.ENGLISH);
            assertEquals(e, d);
        }
    }

    private void runBenchmark() {
        testToUpperCache();
        testToUpperCache();
        testToUpperCache();
        for (int i = 0; i < 6; i++) {
            useIntern = (i % 2) == 0;
            long time = System.nanoTime();
            testSingleThread(100000);
            time = System.nanoTime() - time;
            System.out.println(TimeUnit.NANOSECONDS.toMillis(time) +
                    " ms (useIntern=" + useIntern + ")");
        }

    }

    private String randomString() {
        if (random.nextBoolean()) {
            String s = some[random.nextInt(some.length)];
            if (s != null && random.nextBoolean()) {
                s = new String(s);
            }
            return s;
        }
        int len = random.nextBoolean() ? random.nextInt(1000)
                : random.nextInt(10);
        StringBuilder buff = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            buff.append(random.nextInt(0xfff));
        }
        return buff.toString();
    }

    /**
     * Test one string operation using the string cache.
     */
    void testString() {
        String a = randomString();
        String b;
        if (useIntern) {
            b = a == null ? null : a.intern();
        } else {
            b = StringUtils.cache(a);
        }
        try {
            assertEquals(a, b);
        } catch (Exception e) {
            TestBase.logError("error", e);
        }
    }

    private void testSingleThread(int len) {
        for (int i = 0; i < len; i++) {
            testString();
        }
    }

    private void testMultiThreads() throws InterruptedException {
        int threadCount = getSize(3, 100);
        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (!stop) {
                        testString();
                    }
                }
            });
            threads[i] = t;
        }
        for (int i = 0; i < threadCount; i++) {
            threads[i].start();
        }
        int wait = getSize(200, 2000);
        Thread.sleep(wait);
        stop = true;
        for (int i = 0; i < threadCount; i++) {
            threads[i].join();
        }
    }

}
