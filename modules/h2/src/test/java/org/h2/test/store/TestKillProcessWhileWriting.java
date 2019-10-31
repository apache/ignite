/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.util.Random;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.test.utils.FilePathUnstable;

/**
 * Tests the MVStore.
 */
public class TestKillProcessWhileWriting extends TestBase {

    private String fileName;
    private int seed;
    private FilePathUnstable fs;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.config.big = true;
        test.test();
    }

    @Override
    public void test() throws Exception {
        fs = FilePathUnstable.register();
        fs.setPartialWrites(false);
        test("unstable:memFS:killProcess.h3");

        if (config.big) {
            try {
                fs.setPartialWrites(true);
                test("unstable:memFS:killProcess.h3");
            } finally {
                fs.setPartialWrites(false);
            }
        }
        FileUtils.delete("unstable:memFS:killProcess.h3");
    }

    private void test(String fileName) throws Exception {
        for (seed = 0; seed < 10; seed++) {
            this.fileName = fileName;
            FileUtils.delete(fileName);
            test(Integer.MAX_VALUE, seed);
            int max = Integer.MAX_VALUE - fs.getDiskFullCount() + 10;
            assertTrue("" + (max - 10), max > 0);
            for (int i = 0; i < max; i++) {
                test(i, seed);
            }
        }
    }

    private void test(int x, int seed) throws Exception {
        FileUtils.delete(fileName);
        fs.setDiskFullCount(x, seed);
        try {
            write();
            verify();
        } catch (Exception e) {
            if (x == Integer.MAX_VALUE) {
                throw e;
            }
            fs.setDiskFullCount(0, seed);
            verify();
        }
    }

    private int write() {
        MVStore s;
        MVMap<Integer, byte[]> m;

        s = new MVStore.Builder().
                fileName(fileName).
                pageSplitSize(50).
                autoCommitDisabled().
                open();
        m = s.openMap("data");
        Random r = new Random(seed);
        int op = 0;
        try {
            for (; op < 100; op++) {
                int k = r.nextInt(100);
                byte[] v = new byte[r.nextInt(100) * 100];
                int type = r.nextInt(10);
                switch (type) {
                case 0:
                case 1:
                case 2:
                case 3:
                    m.put(k, v);
                    break;
                case 4:
                case 5:
                    m.remove(k);
                    break;
                case 6:
                    s.commit();
                    break;
                case 7:
                    s.compact(80, 1024);
                    break;
                case 8:
                    m.clear();
                    break;
                case 9:
                    s.close();
                    s = new MVStore.Builder().
                            fileName(fileName).
                            pageSplitSize(50).
                            autoCommitDisabled().
                            open();
                    m = s.openMap("data");
                    break;
                }
            }
            s.close();
            return 0;
        } catch (Exception e) {
            s.closeImmediately();
            return op;
        }
    }

    private void verify() {

        MVStore s;
        MVMap<Integer, byte[]> m;

        FileUtils.delete(fileName);
        s = new MVStore.Builder().
                fileName(fileName).open();
        m = s.openMap("data");
        for (int i = 0; i < 100; i++) {
            byte[] x = m.get(i);
            if (x == null) {
                break;
            }
            assertEquals(i * 100, x.length);
        }
        s.close();
    }

}
