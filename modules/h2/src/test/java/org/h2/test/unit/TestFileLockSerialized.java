/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.h2.api.ErrorCode;
import org.h2.jdbc.JdbcConnection;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.util.SortedProperties;
import org.h2.util.Task;

/**
 * Test the serialized (server-less) mode.
 */
public class TestFileLockSerialized extends TestBase {

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
        if (config.mvStore) {
            return;
        }
        println("testSequence");
        testSequence();
        println("testAutoIncrement");
        testAutoIncrement();
        println("testSequenceFlush");
        testSequenceFlush();
        println("testLeftLogFiles");
        testLeftLogFiles();
        println("testWrongDatabaseInstanceOnReconnect");
        testWrongDatabaseInstanceOnReconnect();
        println("testCache()");
        testCache();
        println("testBigDatabase(false)");
        testBigDatabase(false);
        println("testBigDatabase(true)");
        testBigDatabase(true);
        println("testCheckpointInUpdateRaceCondition");
        testCheckpointInUpdateRaceCondition();
        println("testConcurrentUpdates");
        testConcurrentUpdates();
        println("testThreeMostlyReaders true");
        testThreeMostlyReaders(true);
        println("testThreeMostlyReaders false");
        testThreeMostlyReaders(false);
        println("testTwoReaders");
        testTwoReaders();
        println("testTwoWriters");
        testTwoWriters();
        println("testPendingWrite");
        testPendingWrite();
        println("testKillWriter");
        testKillWriter();
        println("testConcurrentReadWrite");
        testConcurrentReadWrite();
        deleteDb("fileLockSerialized");
    }

    private void testSequence() throws Exception {
        deleteDb("fileLockSerialized");
        String url = "jdbc:h2:" + getBaseDir() + "/fileLockSerialized" +
                ";FILE_LOCK=SERIALIZED;OPEN_NEW=TRUE;RECONNECT_CHECK_DELAY=10";
        ResultSet rs;
        Connection conn1 = getConnection(url);
        Statement stat1 = conn1.createStatement();
        stat1.execute("create sequence seq");
        // 5 times RECONNECT_CHECK_DELAY
        Thread.sleep(100);
        rs = stat1.executeQuery("call seq.nextval");
        rs.next();
        conn1.close();
    }

    private void testSequenceFlush() throws Exception {
        deleteDb("fileLockSerialized");
        String url = "jdbc:h2:" + getBaseDir() +
                "/fileLockSerialized;FILE_LOCK=SERIALIZED;OPEN_NEW=TRUE";
        ResultSet rs;
        Connection conn1 = getConnection(url);
        Statement stat1 = conn1.createStatement();
        stat1.execute("create sequence seq");
        rs = stat1.executeQuery("call seq.nextval");
        rs.next();
        assertEquals(1, rs.getInt(1));
        Connection conn2 = getConnection(url);
        Statement stat2 = conn2.createStatement();
        rs = stat2.executeQuery("call seq.nextval");
        rs.next();
        assertEquals(2, rs.getInt(1));
        conn1.close();
        conn2.close();
    }

    private void testThreeMostlyReaders(final boolean write) throws Exception {
        boolean longRun = false;
        deleteDb("fileLockSerialized");
        final String url = "jdbc:h2:" + getBaseDir() +
                "/fileLockSerialized;FILE_LOCK=SERIALIZED;OPEN_NEW=TRUE";

        Connection conn = getConnection(url);
        conn.createStatement().execute("create table test(id int) as select 1");
        conn.close();

        final int len = 10;
        final Exception[] ex = { null };
        final boolean[] stop = { false };
        Thread[] threads = new Thread[len];
        for (int i = 0; i < len; i++) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Connection c = getConnection(url);
                        PreparedStatement p = c
                                .prepareStatement("select * from test where id = ?");
                        while (!stop[0]) {
                            Thread.sleep(100);
                            if (write) {
                                if (Math.random() > 0.9) {
                                    c.createStatement().execute(
                                            "update test set id = id");
                                }
                            }
                            p.setInt(1, 1);
                            p.executeQuery();
                            p.clearParameters();
                        }
                        c.close();
                    } catch (Exception e) {
                        ex[0] = e;
                    }
                }
            });
            t.start();
            threads[i] = t;
        }
        if (longRun) {
            Thread.sleep(40000);
        } else {
            Thread.sleep(1000);
        }
        stop[0] = true;
        for (int i = 0; i < len; i++) {
            threads[i].join();
        }
        if (ex[0] != null) {
            throw ex[0];
        }
        getConnection(url).close();
    }

    private void testTwoReaders() throws Exception {
        deleteDb("fileLockSerialized");
        String url = "jdbc:h2:" + getBaseDir() +
                "/fileLockSerialized;FILE_LOCK=SERIALIZED;OPEN_NEW=TRUE";
        Connection conn1 = getConnection(url);
        conn1.createStatement().execute("create table test(id int)");
        Connection conn2 = getConnection(url);
        Statement stat2 = conn2.createStatement();
        stat2.execute("drop table test");
        stat2.execute("create table test(id identity) as select 1");
        conn2.close();
        conn1.close();
        getConnection(url).close();
    }

    private void testTwoWriters() throws Exception {
        deleteDb("fileLockSerialized");
        String url = "jdbc:h2:" + getBaseDir() + "/fileLockSerialized";
        final String writeUrl = url + ";FILE_LOCK=SERIALIZED;OPEN_NEW=TRUE";
        Connection conn = getConnection(writeUrl, "sa", "sa");
        conn.createStatement()
                .execute(
                        "create table test(id identity) as " +
                        "select x from system_range(1, 100)");
        conn.close();
        Task task = new Task() {
            @Override
            public void call() throws Exception {
                while (!stop) {
                    Thread.sleep(10);
                    Connection c = getConnection(writeUrl, "sa", "sa");
                    c.createStatement().execute("select * from test");
                    c.close();
                }
            }
        }.execute();
        Thread.sleep(20);
        for (int i = 0; i < 2; i++) {
            conn = getConnection(writeUrl, "sa", "sa");
            Statement stat = conn.createStatement();
            stat.execute("drop table test");
            stat.execute("create table test(id identity) as " +
                    "select x from system_range(1, 100)");
            conn.createStatement().execute("select * from test");
            conn.close();
        }
        Thread.sleep(100);
        conn = getConnection(writeUrl, "sa", "sa");
        conn.createStatement().execute("select * from test");
        conn.close();
        task.get();
    }

    private void testPendingWrite() throws Exception {
        deleteDb("fileLockSerialized");
        String url = "jdbc:h2:" + getBaseDir() + "/fileLockSerialized";
        String writeUrl = url +
                ";FILE_LOCK=SERIALIZED;OPEN_NEW=TRUE;WRITE_DELAY=0";

        Connection conn = getConnection(writeUrl, "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key)");
        Thread.sleep(100);
        String propFile = getBaseDir() + "/fileLockSerialized.lock.db";
        SortedProperties p = SortedProperties.loadProperties(propFile);
        p.setProperty("changePending", "true");
        p.setProperty("modificationDataId", "1000");
        try (OutputStream out = FileUtils.newOutputStream(propFile, false)) {
            p.store(out, "test");
        }
        Thread.sleep(100);
        stat.execute("select * from test");
        conn.close();
    }

    private void testKillWriter() throws Exception {
        deleteDb("fileLockSerialized");
        String url = "jdbc:h2:" + getBaseDir() + "/fileLockSerialized";
        String writeUrl = url +
                ";FILE_LOCK=SERIALIZED;OPEN_NEW=TRUE;WRITE_DELAY=0";

        Connection conn = getConnection(writeUrl, "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key)");
        ((JdbcConnection) conn).setPowerOffCount(1);
        assertThrows(ErrorCode.DATABASE_IS_CLOSED, stat).execute(
                "insert into test values(1)");

        Connection conn2 = getConnection(writeUrl, "sa", "sa");
        Statement stat2 = conn2.createStatement();
        stat2.execute("insert into test values(1)");
        printResult(stat2, "select * from test");

        conn2.close();

        assertThrows(ErrorCode.DATABASE_IS_CLOSED, conn).close();
    }

    private void testConcurrentReadWrite() throws Exception {
        deleteDb("fileLockSerialized");

        String url = "jdbc:h2:" + getBaseDir() + "/fileLockSerialized";
        String writeUrl = url + ";FILE_LOCK=SERIALIZED;OPEN_NEW=TRUE";
        // ;TRACE_LEVEL_SYSTEM_OUT=3
        // String readUrl = writeUrl + ";ACCESS_MODE_DATA=R";

        trace(" create database");
        Class.forName("org.h2.Driver");
        Connection conn = getConnection(writeUrl, "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key)");

        Connection conn3 = getConnection(writeUrl, "sa", "sa");
        PreparedStatement prep3 = conn3
                .prepareStatement("insert into test values(?)");

        Connection conn2 = getConnection(writeUrl, "sa", "sa");
        Statement stat2 = conn2.createStatement();
        printResult(stat2, "select * from test");

        stat2.execute("create local temporary table temp(name varchar) not persistent");
        printResult(stat2, "select * from temp");

        trace(" insert row 1");
        stat.execute("insert into test values(1)");
        trace(" insert row 2");
        prep3.setInt(1, 2);
        prep3.execute();
        printResult(stat2, "select * from test");
        printResult(stat2, "select * from temp");

        conn.close();
        conn2.close();
        conn3.close();
    }

    private void printResult(Statement stat, String sql) throws SQLException {
        trace("  query: " + sql);
        ResultSet rs = stat.executeQuery(sql);
        int rowCount = 0;
        while (rs.next()) {
            trace("   " + rs.getString(1));
            rowCount++;
        }
        trace("   " + rowCount + " row(s)");
    }

    private void testConcurrentUpdates() throws Exception {
        boolean longRun = false;
        if (longRun) {
            for (int waitTime = 100; waitTime < 10000; waitTime += 20) {
                for (int howManyThreads = 1; howManyThreads < 10; howManyThreads++) {
                    testConcurrentUpdates(waitTime, howManyThreads, waitTime *
                            howManyThreads * 10);
                }
            }
        } else {
            testConcurrentUpdates(100, 4, 2000);
        }
    }

    private void testAutoIncrement() throws Exception {
        boolean longRun = false;
        if (longRun) {
            for (int waitTime = 100; waitTime < 10000; waitTime += 20) {
                for (int howManyThreads = 1; howManyThreads < 10; howManyThreads++) {
                    testAutoIncrement(waitTime, howManyThreads, 2000);
                }
            }
        } else {
            testAutoIncrement(400, 2, 2000);
        }
    }

    private void testAutoIncrement(final int waitTime, int howManyThreads,
            int runTime) throws Exception {
        println("testAutoIncrement waitTime: " + waitTime +
                " howManyThreads: " + howManyThreads + " runTime: " + runTime);
        deleteDb("fileLockSerialized");
        final String url = "jdbc:h2:" +
                getBaseDir() +
                "/fileLockSerialized;FILE_LOCK=SERIALIZED;OPEN_NEW=TRUE;" +
                "AUTO_RECONNECT=TRUE;MAX_LENGTH_INPLACE_LOB=8192;" +
                "COMPRESS_LOB=DEFLATE;CACHE_SIZE=65536";

        Connection conn = getConnection(url);
        conn.createStatement().execute(
                "create table test(id int auto_increment, id2 int)");
        conn.close();

        final long endTime = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(runTime);
        final Exception[] ex = { null };
        final Connection[] connList = new Connection[howManyThreads];
        final boolean[] stop = { false };
        final int[] nextInt = { 0 };
        Thread[] threads = new Thread[howManyThreads];
        for (int i = 0; i < howManyThreads; i++) {
            final int finalNrOfConnection = i;
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Connection c = getConnection(url);
                        connList[finalNrOfConnection] = c;
                        while (!stop[0]) {
                            synchronized (nextInt) {
                                ResultSet rs = c.createStatement()
                                        .executeQuery(
                                                "select id, id2 from test");
                                while (rs.next()) {
                                    if (rs.getInt(1) != rs.getInt(2)) {
                                        throw new Exception(Thread
                                                .currentThread().getId() +
                                                " nextInt: " +
                                                nextInt[0] +
                                                " rs.getInt(1): " +
                                                rs.getInt(1) +
                                                " rs.getInt(2): " +
                                                rs.getInt(2));
                                    }
                                }
                                nextInt[0]++;
                                Statement stat = c.createStatement();
                                stat.execute("insert into test (id2) values(" +
                                        nextInt[0] + ")");
                                ResultSet rsKeys = stat.getGeneratedKeys();
                                while (rsKeys.next()) {
                                    assertEquals(nextInt[0], rsKeys.getInt(1));
                                }
                                rsKeys.close();
                            }
                            Thread.sleep(waitTime);
                        }
                        c.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                        ex[0] = e;
                    }
                }
            });
            t.start();
            threads[i] = t;
        }
        while ((ex[0] == null) && (System.nanoTime() < endTime)) {
            Thread.sleep(10);
        }

        stop[0] = true;
        for (int i = 0; i < howManyThreads; i++) {
            threads[i].join();
        }
        if (ex[0] != null) {
            throw ex[0];
        }
        getConnection(url).close();
        deleteDb("fileLockSerialized");
    }

    private void testConcurrentUpdates(final int waitTime, int howManyThreads,
            int runTime) throws Exception {
        println("testConcurrentUpdates waitTime: " + waitTime +
                " howManyThreads: " + howManyThreads + " runTime: " + runTime);
        deleteDb("fileLockSerialized");
        final String url = "jdbc:h2:" +
                getBaseDir() +
                "/fileLockSerialized;FILE_LOCK=SERIALIZED;OPEN_NEW=TRUE;" +
                "AUTO_RECONNECT=TRUE;MAX_LENGTH_INPLACE_LOB=8192;" +
                "COMPRESS_LOB=DEFLATE;CACHE_SIZE=65536";

        Connection conn = getConnection(url);
        conn.createStatement().execute("create table test(id int)");
        conn.createStatement().execute("insert into test values(1)");
        conn.close();

        final long endTime = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(runTime);
        final Exception[] ex = { null };
        final Connection[] connList = new Connection[howManyThreads];
        final boolean[] stop = { false };
        final int[] lastInt = { 1 };
        Thread[] threads = new Thread[howManyThreads];
        for (int i = 0; i < howManyThreads; i++) {
            final int finalNrOfConnection = i;
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Connection c = getConnection(url);
                        connList[finalNrOfConnection] = c;
                        while (!stop[0]) {
                            ResultSet rs = c.createStatement().executeQuery(
                                    "select * from test");
                            rs.next();
                            if (rs.getInt(1) != lastInt[0]) {
                                throw new Exception(finalNrOfConnection +
                                        "  Expected: " + lastInt[0] + " got " +
                                        rs.getInt(1));
                            }
                            Thread.sleep(waitTime);
                            if (Math.random() > 0.7) {
                                int newLastInt = (int) (Math.random() * 1000);
                                c.createStatement().execute(
                                        "update test set id = " + newLastInt);
                                lastInt[0] = newLastInt;
                            }
                        }
                        c.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                        ex[0] = e;
                    }
                }
            });
            t.start();
            threads[i] = t;
        }
        while ((ex[0] == null) && (System.nanoTime() < endTime)) {
            Thread.sleep(10);
        }

        stop[0] = true;
        for (int i = 0; i < howManyThreads; i++) {
            threads[i].join();
        }
        if (ex[0] != null) {
            throw ex[0];
        }
        getConnection(url).close();
        deleteDb("fileLockSerialized");
    }

    /**
     * If a checkpoint occurs between beforeWriting and checkWritingAllowed then
     * the result of checkWritingAllowed is READ_ONLY, which is wrong.
     *
     * Also, if a checkpoint started before beforeWriting, and ends between
     * between beforeWriting and checkWritingAllowed, then the same error
     * occurs.
     */
    private void testCheckpointInUpdateRaceCondition() throws Exception {
        boolean longRun = false;
        deleteDb("fileLockSerialized");
        String url = "jdbc:h2:" + getBaseDir() +
                "/fileLockSerialized;FILE_LOCK=SERIALIZED;OPEN_NEW=TRUE";

        Connection conn = getConnection(url);
        conn.createStatement().execute("create table test(id int)");
        conn.createStatement().execute("insert into test values(1)");
        for (int i = 0; i < (longRun ? 10000 : 5); i++) {
            Thread.sleep(402);
            conn.createStatement().execute("update test set id = " + i);
        }
        conn.close();
        deleteDb("fileLockSerialized");
    }

    /**
     * Caches must be cleared. Session.reconnect only closes the DiskFile (which
     * is associated with the cache) if there is one session
     */
    private void testCache() throws Exception {
        deleteDb("fileLockSerialized");

        String urlShared = "jdbc:h2:" + getBaseDir() +
                "/fileLockSerialized;FILE_LOCK=SERIALIZED";

        Connection connShared1 = getConnection(urlShared);
        Statement statement1 = connShared1.createStatement();
        Connection connShared2 = getConnection(urlShared);
        Statement statement2 = connShared2.createStatement();

        statement1.execute("create table test1(id int)");
        statement1.execute("insert into test1 values(1)");

        ResultSet rs = statement1.executeQuery("select id from test1");
        rs.close();
        rs = statement2.executeQuery("select id from test1");
        rs.close();

        statement1.execute("update test1 set id=2");
        Thread.sleep(500);

        rs = statement2.executeQuery("select id from test1");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        rs.close();

        connShared1.close();
        connShared2.close();
        deleteDb("fileLockSerialized");
    }

    private void testWrongDatabaseInstanceOnReconnect() throws Exception {
        deleteDb("fileLockSerialized");

        String urlShared = "jdbc:h2:" + getBaseDir() +
                "/fileLockSerialized;FILE_LOCK=SERIALIZED";
        String urlForNew = urlShared + ";OPEN_NEW=TRUE";

        Connection connShared1 = getConnection(urlShared);
        Statement statement1 = connShared1.createStatement();
        Connection connShared2 = getConnection(urlShared);
        Connection connNew = getConnection(urlForNew);
        statement1.execute("create table test1(id int)");
        connShared1.close();
        connShared2.close();
        connNew.close();
        deleteDb("fileLockSerialized");
    }

    private void testBigDatabase(boolean withCache) {
        boolean longRun = false;
        final int howMuchRows = longRun ? 2000000 : 500000;
        deleteDb("fileLockSerialized");
        int cacheSizeKb = withCache ? 5000 : 0;

        final CountDownLatch importFinishedLatch = new CountDownLatch(1);
        final CountDownLatch select1FinishedLatch = new CountDownLatch(1);

        final String url = "jdbc:h2:" + getBaseDir() + "/fileLockSerialized" +
                ";FILE_LOCK=SERIALIZED" + ";OPEN_NEW=TRUE" + ";CACHE_SIZE=" +
                cacheSizeKb;
        final Task importUpdateTask = new Task() {
            @Override
            public void call() throws Exception {
                Connection conn = getConnection(url);
                Statement stat = conn.createStatement();
                stat.execute("create table test(id int, id2 int)");
                for (int i = 0; i < howMuchRows; i++) {
                    stat.execute("insert into test values(" + i + ", " + i +
                            ")");
                }
                importFinishedLatch.countDown();

                select1FinishedLatch.await();

                stat.execute("update test set id2=999 where id=500");
                conn.close();
            }
        };
        importUpdateTask.execute();

        Task selectTask = new Task() {
            @Override
            public void call() throws Exception {
                Connection conn = getConnection(url);
                Statement stat = conn.createStatement();
                importFinishedLatch.await();

                ResultSet rs = stat
                        .executeQuery("select id2 from test where id=500");
                assertTrue(rs.next());
                assertEquals(500, rs.getInt(1));
                rs.close();
                select1FinishedLatch.countDown();

                // wait until the other task finished
                importUpdateTask.get();

                // can't use the exact same query, otherwise it would use
                // the query cache
                rs = stat.executeQuery("select id2 from test where id=500+0");
                assertTrue(rs.next());
                assertEquals(999, rs.getInt(1));
                rs.close();
                conn.close();
            }
        };
        selectTask.execute();

        importUpdateTask.get();
        selectTask.get();
        deleteDb("fileLockSerialized");
    }

    private void testLeftLogFiles() throws Exception {
        deleteDb("fileLockSerialized");

        // without serialized
        String url;
        url = "jdbc:h2:" + getBaseDir() + "/fileLockSerialized";
        Connection conn = getConnection(url);
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int)");
        stat.execute("insert into test values(0)");
        conn.close();

        List<String> filesWithoutSerialized = FileUtils
                .newDirectoryStream(getBaseDir());
        deleteDb("fileLockSerialized");

        // with serialized
        url = "jdbc:h2:" + getBaseDir() +
                "/fileLockSerialized;FILE_LOCK=SERIALIZED";
        conn = getConnection(url);
        stat = conn.createStatement();
        stat.execute("create table test(id int)");
        Thread.sleep(500);
        stat.execute("insert into test values(0)");
        conn.close();

        List<String> filesWithSerialized = FileUtils
                .newDirectoryStream(getBaseDir());
        if (filesWithoutSerialized.size() != filesWithSerialized.size()) {
            for (int i = 0; i < filesWithoutSerialized.size(); i++) {
                if (!filesWithSerialized
                        .contains(filesWithoutSerialized.get(i))) {
                    System.out
                            .println("File left from 'without serialized' mode: " +
                                    filesWithoutSerialized.get(i));
                }
            }
            for (int i = 0; i < filesWithSerialized.size(); i++) {
                if (!filesWithoutSerialized
                        .contains(filesWithSerialized.get(i))) {
                    System.out
                            .println("File left from 'with serialized' mode: " +
                                    filesWithSerialized.get(i));
                }
            }
            fail("With serialized it must create the same files than without serialized");
        }
        deleteDb("fileLockSerialized");
    }

}
