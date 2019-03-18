/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.store.FileLister;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.util.Task;

/**
 * Tests performance and helps analyze bottlenecks.
 */
public class TestBenchmark extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        testConcurrency();

        // TODO this test is currently disabled

        test(true);
        test(false);
        test(true);
        test(false);
        test(true);
        test(false);
    }

    private void testConcurrency() throws Exception {
        // String fileName = getBaseDir() + "/" + getTestName();
        String fileName = "nioMemFS:/" + getTestName();
        FileUtils.delete(fileName);
        MVStore store = new MVStore.Builder().cacheSize(16).
                fileName(fileName).open();
        MVMap<Integer, byte[]> map = store.openMap("test");
        byte[] data = new byte[1024];
        int count = 1000000;
        for (int i = 0; i < count; i++) {
            map.put(i, data);
        }
        store.close();
        for (int concurrency = 1024; concurrency > 0; concurrency /= 2) {
            testConcurrency(fileName, concurrency, count);
            testConcurrency(fileName, concurrency, count);
            testConcurrency(fileName, concurrency, count);
        }
        FileUtils.delete(fileName);
    }

    private void testConcurrency(String fileName,
            int concurrency, final int count) throws Exception {
        Thread.sleep(1000);
            final MVStore store = new MVStore.Builder().cacheSize(256).
                    cacheConcurrency(concurrency).
                    fileName(fileName).open();
        int threadCount = 128;
        final CountDownLatch wait = new CountDownLatch(1);
        final AtomicInteger counter = new AtomicInteger();
        final AtomicBoolean stopped = new AtomicBoolean();
        Task[] tasks = new Task[threadCount];
        // Profiler prof = new Profiler().startCollecting();
        for (int i = 0; i < threadCount; i++) {
            final int x = i;
            Task t = new Task() {
                @Override
                public void call() throws Exception {
                    MVMap<Integer, byte[]> map = store.openMap("test");
                    Random random = new Random(x);
                    wait.await();
                    while (!stopped.get()) {
                        int key = random.nextInt(count);
                        byte[] data = map.get(key);
                        if (data.length > 1) {
                            counter.incrementAndGet();
                        }
                    }
                }
            };
            t.execute("t" + i);
            tasks[i] = t;
        }
        wait.countDown();
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        stopped.set(true);
        for (Task t : tasks) {
            t.get();
        }
        // System.out.println(prof.getTop(5));
        String msg = "concurrency " + concurrency +
                " threads " + threadCount + " requests: " + counter;
        System.out.println(msg);
        trace(msg);
        store.close();
    }

    private void test(boolean mvStore) throws Exception {
        // testInsertSelect(mvStore);
        // testBinary(mvStore);
        testCreateIndex(mvStore);
    }

    private void testCreateIndex(boolean mvStore) throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        Connection conn;
        Statement stat;
        String url = "mvstore";
        if (mvStore) {
            // ;COMPRESS=TRUE";
            url += ";MV_STORE=TRUE";
        }

        url = getURL(url, true);
        conn = getConnection(url);
        stat = conn.createStatement();
        stat.execute("create table test(id bigint primary key, data bigint)");
        conn.setAutoCommit(false);
        PreparedStatement prep = conn
                .prepareStatement("insert into test values(?, ?)");

//        int rowCount = 10000000;
        int rowCount = 1000000;

        Random r = new Random(1);

        for (int i = 0; i < rowCount; i++) {
            prep.setInt(1, i);
            // prep.setInt(2, i);
            prep.setInt(2, r.nextInt());
            prep.execute();
            if (i % 10000 == 0) {
                conn.commit();
            }
        }

        long start = System.nanoTime();
        // Profiler prof = new Profiler().startCollecting();
        stat.execute("create index on test(data)");
        // System.out.println(prof.getTop(5));

        System.out.println(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + " "
                + (mvStore ? "mvstore" : "default"));
        conn.createStatement().execute("shutdown compact");
        conn.close();
        for (String f : FileLister.getDatabaseFiles(getBaseDir(), "mvstore", true)) {
            System.out.println("   " + f + " " + FileUtils.size(f));
        }
    }

    private void testBinary(boolean mvStore) throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        Connection conn;
        Statement stat;
        String url = "mvstore";
        if (mvStore) {
            url += ";MV_STORE=TRUE";
        }

        url = getURL(url, true);
        conn = getConnection(url);
        stat = conn.createStatement();
        stat.execute("create table test(id bigint primary key, data blob)");
        conn.setAutoCommit(false);
        PreparedStatement prep = conn
                .prepareStatement("insert into test values(?, ?)");
        byte[] data = new byte[1024 * 1024];

        int rowCount = 100;
        int readCount = 20 * rowCount;

        long start = System.nanoTime();

        for (int i = 0; i < rowCount; i++) {
            prep.setInt(1, i);
            randomize(data, i);
            prep.setBytes(2, data);
            prep.execute();
            if (i % 100 == 0) {
                conn.commit();
            }
        }

        prep = conn.prepareStatement("select * from test where id = ?");
        for (int i = 0; i < readCount; i++) {
            prep.setInt(1, i % rowCount);
            prep.executeQuery();
        }

        System.out.println(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + " "
                + (mvStore ? "mvstore" : "default"));
        conn.close();
    }

    private static void randomize(byte[] data, int i) {
        Random r = new Random(i);
        r.nextBytes(data);
    }

    private void testInsertSelect(boolean mvStore) throws Exception {

        FileUtils.deleteRecursive(getBaseDir(), true);
        Connection conn;
        Statement stat;
        String url = "mvstore";
        if (mvStore) {
            url += ";MV_STORE=TRUE;LOG=0;COMPRESS=TRUE";
        }

        url = getURL(url, true);
        conn = getConnection(url);
        stat = conn.createStatement();
        stat.execute("create table test(id bigint primary key, name varchar)");
        conn.setAutoCommit(false);
        PreparedStatement prep = conn
                .prepareStatement("insert into test values(?, ?)");
        String data = "Hello World";

        int rowCount = 100000;
        int readCount = 20 * rowCount;

        for (int i = 0; i < rowCount; i++) {
            prep.setInt(1, i);
            prep.setString(2, data);
            prep.execute();
            if (i % 100 == 0) {
                conn.commit();
            }
        }
        long start = System.nanoTime();

        prep = conn.prepareStatement("select * from test where id = ?");
        for (int i = 0; i < readCount; i++) {
            prep.setInt(1, i % rowCount);
            prep.executeQuery();
        }

        System.out.println(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + " "
                + (mvStore ? "mvstore" : "default"));
        conn.createStatement().execute("shutdown compact");
        conn.close();
        for (String f : FileLister.getDatabaseFiles(getBaseDir(), "mvstore", true)) {
            System.out.println("   " + f + " " + FileUtils.size(f));
        }

    }

}
