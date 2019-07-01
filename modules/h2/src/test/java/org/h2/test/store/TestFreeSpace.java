/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.h2.mvstore.FreeSpaceBitSet;
import org.h2.test.TestBase;
import org.h2.util.Utils;

/**
 * Tests the free space list.
 */
public class TestFreeSpace extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
        testMemoryUsage();
        testPerformance();
    }

    @Override
    public void test() throws Exception {
        testSimple();
        testRandomized();
    }

    private static void testPerformance() {
        for (int i = 0; i < 10; i++) {
            long t = System.nanoTime();

            FreeSpaceBitSet f = new FreeSpaceBitSet(0, 4096);
            // 75 ms

            // FreeSpaceList f = new FreeSpaceList(0, 4096);
            // 13868 ms

            // FreeSpaceTree f = new FreeSpaceTree(0, 4096);
            // 56 ms

            for (int j = 0; j < 100000; j++) {
                f.markUsed(j * 2 * 4096, 4096);
            }
            for (int j = 0; j < 100000; j++) {
                f.free(j * 2 * 4096, 4096);
            }
            for (int j = 0; j < 100000; j++) {
                f.allocate(4096 * 2);
            }
            System.out.println(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t));
        }
    }

    private static void testMemoryUsage() {

        // 16 GB file size
        long size = 16L * 1024 * 1024 * 1024;
        System.gc();
        System.gc();
        long first = Utils.getMemoryUsed();

        FreeSpaceBitSet f = new FreeSpaceBitSet(0, 4096);
        // 512 KB

        // FreeSpaceTree f = new FreeSpaceTree(0, 4096);
        // 64 MB

        // FreeSpaceList f = new FreeSpaceList(0, 4096);
        // too slow

        for (long j = size; j > 0; j -= 4 * 4096) {
            f.markUsed(j, 4096);
        }

        System.gc();
        System.gc();
        long mem = Utils.getMemoryUsed() - first;
        System.out.println("Memory used: " + mem);
        System.out.println("f: " + f.toString().length());

    }

    private void testSimple() {
        FreeSpaceBitSet f1 = new FreeSpaceBitSet(2, 1024);
        FreeSpaceList f2 = new FreeSpaceList(2, 1024);
        FreeSpaceTree f3 = new FreeSpaceTree(2, 1024);
        assertEquals(f1.toString(), f2.toString());
        assertEquals(f1.toString(), f3.toString());
        assertEquals(2 * 1024, f1.allocate(10240));
        assertEquals(2 * 1024, f2.allocate(10240));
        assertEquals(2 * 1024, f3.allocate(10240));
        assertEquals(f1.toString(), f2.toString());
        assertEquals(f1.toString(), f3.toString());
        f1.markUsed(20480, 1024);
        f2.markUsed(20480, 1024);
        f3.markUsed(20480, 1024);
        assertEquals(f1.toString(), f2.toString());
        assertEquals(f1.toString(), f3.toString());
    }

    private void testRandomized() {
        FreeSpaceBitSet f1 = new FreeSpaceBitSet(2, 8);
        FreeSpaceList f2 = new FreeSpaceList(2, 8);
        Random r = new Random(1);
        StringBuilder log = new StringBuilder();
        for (int i = 0; i < 100000; i++) {
            long pos = r.nextInt(1024);
            int length = 1 + r.nextInt(8 * 128);
            switch (r.nextInt(3)) {
            case 0: {
                log.append("allocate(" + length + ");\n");
                long a = f1.allocate(length);
                long b = f2.allocate(length);
                assertEquals(a, b);
                break;
            }
            case 1:
                if (f1.isUsed(pos, length)) {
                    log.append("free(" + pos + ", " + length + ");\n");
                    f1.free(pos, length);
                    f2.free(pos, length);
                }
                break;
            case 2:
                if (f1.isFree(pos, length)) {
                    log.append("markUsed(" + pos + ", " + length + ");\n");
                    f1.markUsed(pos, length);
                    f2.markUsed(pos, length);
                }
                break;
            }
            assertEquals(f1.toString(), f2.toString());
        }
    }

}
