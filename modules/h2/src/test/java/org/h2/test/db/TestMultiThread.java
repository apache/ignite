/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.h2.api.ErrorCode;
import org.h2.jdbc.JdbcSQLException;
import org.h2.test.TestAll;
import org.h2.test.TestBase;
import org.h2.util.IOUtils;
import org.h2.util.SmallLRUCache;
import org.h2.util.SynchronizedVerifier;
import org.h2.util.Task;

/**
 * Multi-threaded tests.
 */
public class TestMultiThread extends TestBase implements Runnable {

    private boolean stop;
    private TestMultiThread parent;
    private Random random;

    public TestMultiThread() {
        // nothing to do
    }

    private TestMultiThread(TestAll config, TestMultiThread parent) {
        this.config = config;
        this.parent = parent;
        random = new Random();
    }

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
        testConcurrentSchemaChange();
        testConcurrentLobAdd();
        testConcurrentView();
        testConcurrentAlter();
        testConcurrentAnalyze();
        testConcurrentInsertUpdateSelect();
        testLockModeWithMultiThreaded();
        testViews();
        testConcurrentInsert();
        testConcurrentUpdate();
    }

    private void testConcurrentSchemaChange() throws Exception {
        String db = getTestName();
        deleteDb(db);
        final String url = getURL(db + ";MULTI_THREADED=1;LOCK_TIMEOUT=10000", true);
        try (Connection conn = getConnection(url)) {
            Task[] tasks = new Task[2];
            for (int i = 0; i < tasks.length; i++) {
                final int x = i;
                Task t = new Task() {
                    @Override
                    public void call() throws Exception {
                        try (Connection c2 = getConnection(url)) {
                            Statement stat = c2.createStatement();
                            for (int i = 0; !stop; i++) {
                                stat.execute("create table test" + x + "_" + i);
                                c2.getMetaData().getTables(null, null, null, null);
                                stat.execute("drop table test" + x + "_" + i);
                            }
                        }
                    }
                };
                tasks[i] = t;
                t.execute();
            }
            Thread.sleep(1000);
            for (Task t : tasks) {
                t.get();
            }
        }
    }

    private void testConcurrentLobAdd() throws Exception {
        String db = getTestName();
        deleteDb(db);
        final String url = getURL(db + ";MULTI_THREADED=1", true);
        try (Connection conn = getConnection(url)) {
            Statement stat = conn.createStatement();
            stat.execute("create table test(id identity, data clob)");
            Task[] tasks = new Task[2];
            for (int i = 0; i < tasks.length; i++) {
                Task t = new Task() {
                    @Override
                    public void call() throws Exception {
                        try (Connection c2 = getConnection(url)) {
                            PreparedStatement p2 = c2
                                    .prepareStatement("insert into test(data) values(?)");
                            while (!stop) {
                                p2.setCharacterStream(1, new StringReader(new String(
                                        new char[10 * 1024])));
                                p2.execute();
                            }
                        }
                    }
                };
                tasks[i] = t;
                t.execute();
            }
            Thread.sleep(500);
            for (Task t : tasks) {
                t.get();
            }
        }
    }

    private void testConcurrentView() throws Exception {
        if (config.mvcc || config.mvStore) {
            return;
        }
        String db = getTestName();
        deleteDb(db);
        final String url = getURL(db + ";MULTI_THREADED=1", true);
        final Random r = new Random();
        try (Connection conn = getConnection(url)) {
            Statement stat = conn.createStatement();
            StringBuilder buff = new StringBuilder();
            buff.append("create table test(id int");
            final int len = 3;
            for (int i = 0; i < len; i++) {
                buff.append(", x" + i + " int");
            }
            buff.append(")");
            stat.execute(buff.toString());
            stat.execute("create view test_view as select * from test");
            stat.execute("insert into test(id) select x from system_range(1, 2)");
            Task t = new Task() {
                @Override
                public void call() throws Exception {
                    Connection c2 = getConnection(url);
                    while (!stop) {
                        c2.prepareStatement("select * from test_view where x" +
                                r.nextInt(len) + "=1");
                    }
                    c2.close();
                }
            };
            t.execute();
            SynchronizedVerifier.setDetect(SmallLRUCache.class, true);
            for (int i = 0; i < 1000; i++) {
                conn.prepareStatement("select * from test_view where x" +
                        r.nextInt(len) + "=1");
            }
            t.get();
            SynchronizedVerifier.setDetect(SmallLRUCache.class, false);
        }
    }

    private void testConcurrentAlter() throws Exception {
        deleteDb(getTestName());
        try (final Connection conn = getConnection(getTestName())) {
            Statement stat = conn.createStatement();
            Task t = new Task() {
                @Override
                public void call() throws Exception {
                    while (!stop) {
                        conn.prepareStatement("select * from test");
                    }
                }
            };
            stat.execute("create table test(id int)");
            t.execute();
            for (int i = 0; i < 200; i++) {
                stat.execute("alter table test add column x int");
                stat.execute("alter table test drop column x");
            }
            t.get();
        }
    }

    private void testConcurrentAnalyze() throws Exception {
        if (config.mvcc) {
            return;
        }
        deleteDb(getTestName());
        final String url = getURL("concurrentAnalyze;MULTI_THREADED=1", true);
        try (Connection conn = getConnection(url)) {
            Statement stat = conn.createStatement();
            stat.execute("create table test(id bigint primary key) " +
                    "as select x from system_range(1, 1000)");
            Task t = new Task() {
                @Override
                public void call() throws SQLException {
                    try (Connection conn2 = getConnection(url)) {
                        for (int i = 0; i < 1000; i++) {
                            conn2.createStatement().execute("analyze");
                        }
                    }
                }
            };
            t.execute();
            Thread.yield();
            for (int i = 0; i < 1000; i++) {
                conn.createStatement().execute("analyze");
            }
            t.get();
            stat.execute("drop table test");
        }
    }

    private void testConcurrentInsertUpdateSelect() throws Exception {
        try (Connection conn = getConnection()) {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE TEST(ID IDENTITY, NAME VARCHAR)");
            int len = getSize(10, 200);
            Thread[] threads = new Thread[len];
            for (int i = 0; i < len; i++) {
                threads[i] = new Thread(new TestMultiThread(config, this));
            }
            for (int i = 0; i < len; i++) {
                threads[i].start();
            }
            int sleep = getSize(400, 10000);
            Thread.sleep(sleep);
            this.stop = true;
            for (int i = 0; i < len; i++) {
                threads[i].join();
            }
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM TEST");
            rs.next();
            trace("max id=" + rs.getInt(1));
        }
    }

    private Connection getConnection() throws SQLException {
        return getConnection("jdbc:h2:mem:" + getTestName());
    }

    @Override
    public void run() {
        try (Connection conn = getConnection()) {
            Statement stmt = conn.createStatement();
            while (!parent.stop) {
                stmt.execute("SELECT COUNT(*) FROM TEST");
                stmt.execute("INSERT INTO TEST VALUES(NULL, 'Hi')");
                PreparedStatement prep = conn.prepareStatement(
                        "UPDATE TEST SET NAME='Hello' WHERE ID=?");
                prep.setInt(1, random.nextInt(10000));
                prep.execute();
                prep = conn.prepareStatement("SELECT * FROM TEST WHERE ID=?");
                prep.setInt(1, random.nextInt(10000));
                ResultSet rs = prep.executeQuery();
                while (rs.next()) {
                    rs.getString("NAME");
                }
            }
        } catch (Exception e) {
            logError("multi", e);
        }
    }

    private void testLockModeWithMultiThreaded() throws Exception {
        // currently the combination of LOCK_MODE=0 and MULTI_THREADED
        // is not supported
        deleteDb("lockMode");
        final String url = getURL("lockMode;MULTI_THREADED=1", true);
        try (Connection conn = getConnection(url)) {
            DatabaseMetaData meta = conn.getMetaData();
            assertFalse(meta.supportsTransactionIsolationLevel(
                    Connection.TRANSACTION_READ_UNCOMMITTED));
        }
        deleteDb("lockMode");
    }

    private void testViews() throws Exception {
        // currently the combination of LOCK_MODE=0 and MULTI_THREADED
        // is not supported
        deleteDb("lockMode");
        final String url = getURL("lockMode;MULTI_THREADED=1", true);

        // create some common tables and views
        ExecutorService executor = Executors.newFixedThreadPool(8);
        Connection conn = getConnection(url);
        try {
            Statement stat = conn.createStatement();
            stat.execute(
                    "CREATE TABLE INVOICE(INVOICE_ID INT PRIMARY KEY, AMOUNT DECIMAL)");
            stat.execute("CREATE VIEW INVOICE_VIEW as SELECT * FROM INVOICE");

            stat.execute(
                    "CREATE TABLE INVOICE_DETAIL(DETAIL_ID INT PRIMARY KEY, " +
                    "INVOICE_ID INT, DESCRIPTION VARCHAR)");
            stat.execute(
                    "CREATE VIEW INVOICE_DETAIL_VIEW as SELECT * FROM INVOICE_DETAIL");

            stat.close();

            // create views that reference the common views in different threads
            ArrayList<Future<Void>> jobs = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                final int j = i;
                jobs.add(executor.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        try (Connection conn2 = getConnection(url)) {
                            Statement stat2 = conn2.createStatement();

                            stat2.execute("CREATE VIEW INVOICE_VIEW" + j
                                    + " as SELECT * FROM INVOICE_VIEW");

                            // the following query intermittently results in a
                            // NullPointerException
                            stat2.execute("CREATE VIEW INVOICE_DETAIL_VIEW" + j
                                    + " as SELECT DTL.* FROM INVOICE_VIEW" + j
                                    + " INV JOIN INVOICE_DETAIL_VIEW DTL "
                                    + "ON INV.INVOICE_ID = DTL.INVOICE_ID"
                                    + " WHERE DESCRIPTION='TEST'");

                            ResultSet rs = stat2
                                    .executeQuery("SELECT * FROM INVOICE_VIEW" + j);
                            rs.next();
                            rs.close();

                            rs = stat2.executeQuery(
                                    "SELECT * FROM INVOICE_DETAIL_VIEW" + j);
                            rs.next();
                            rs.close();

                            stat2.close();
                        }
                        return null;
                    }
                }));
            }
            // check for exceptions
            for (Future<Void> job : jobs) {
                try {
                    job.get();
                } catch (ExecutionException ex) {
                    // ignore timeout exceptions, happens periodically when the
                    // machine is really busy and it's not the thing we are
                    // trying to test
                    if (!(ex.getCause() instanceof JdbcSQLException)
                            || ((JdbcSQLException) ex.getCause())
                                    .getErrorCode() != ErrorCode.LOCK_TIMEOUT_1) {
                        throw ex;
                    }
                }
            }
        } finally {
            IOUtils.closeSilently(conn);
            executor.shutdown();
            executor.awaitTermination(20, TimeUnit.SECONDS);
        }

        deleteDb("lockMode");
    }

    private void testConcurrentInsert() throws Exception {
        deleteDb("lockMode");

        final String url = getURL("lockMode;MULTI_THREADED=1;LOCK_TIMEOUT=10000", true);
        int threadCount = 25;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        Connection conn = getConnection(url);
        try {
            conn.createStatement().execute(
                    "CREATE TABLE IF NOT EXISTS TRAN (ID NUMBER(18,0) not null PRIMARY KEY)");

            final ArrayList<Callable<Void>> callables = new ArrayList<>();
            for (int i = 0; i < threadCount; i++) {
                final long initialTransactionId = i * 1000000L;
                callables.add(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        try (Connection taskConn = getConnection(url)) {
                            taskConn.setAutoCommit(false);
                            PreparedStatement insertTranStmt = taskConn
                                    .prepareStatement("INSERT INTO tran (id) VALUES(?)");
                            // to guarantee uniqueness
                            long tranId = initialTransactionId;
                            for (int j = 0; j < 1000; j++) {
                                insertTranStmt.setLong(1, tranId++);
                                insertTranStmt.execute();
                                taskConn.commit();
                            }
                        }
                        return null;
                    }
                });
            }

            final ArrayList<Future<Void>> jobs = new ArrayList<>();
            for (int i = 0; i < threadCount; i++) {
                jobs.add(executor.submit(callables.get(i)));
            }
            // check for exceptions
            for (Future<Void> job : jobs) {
                job.get(5, TimeUnit.MINUTES);
            }
        } finally {
            IOUtils.closeSilently(conn);
            executor.shutdown();
            executor.awaitTermination(20, TimeUnit.SECONDS);
        }

        deleteDb("lockMode");
    }

    private void testConcurrentUpdate() throws Exception {
        deleteDb("lockMode");

        final int objectCount = 10000;
        final String url = getURL("lockMode;MULTI_THREADED=1;LOCK_TIMEOUT=10000", true);
        int threadCount = 25;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        Connection conn = getConnection(url);
        try {
            conn.createStatement().execute(
                    "CREATE TABLE IF NOT EXISTS ACCOUNT" +
                    "(ID NUMBER(18,0) not null PRIMARY KEY, BALANCE NUMBER null)");
            final PreparedStatement mergeAcctStmt = conn
                    .prepareStatement("MERGE INTO Account(id, balance) key (id) VALUES (?, ?)");
            for (int i = 0; i < objectCount; i++) {
                mergeAcctStmt.setLong(1, i);
                mergeAcctStmt.setBigDecimal(2, BigDecimal.ZERO);
                mergeAcctStmt.execute();
            }

            final ArrayList<Callable<Void>> callables = new ArrayList<>();
            for (int i = 0; i < threadCount; i++) {
                callables.add(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        try (Connection taskConn = getConnection(url)) {
                            taskConn.setAutoCommit(false);
                            final PreparedStatement updateAcctStmt = taskConn
                                    .prepareStatement("UPDATE account SET balance = ? WHERE id = ?");
                            for (int j = 0; j < 1000; j++) {
                                updateAcctStmt.setDouble(1, Math.random());
                                updateAcctStmt.setLong(2, (int) (Math.random() * objectCount));
                                updateAcctStmt.execute();
                                taskConn.commit();
                            }
                        }
                        return null;
                    }
                });
            }

            final ArrayList<Future<Void>> jobs = new ArrayList<>();
            for (int i = 0; i < threadCount; i++) {
                jobs.add(executor.submit(callables.get(i)));
            }
            // check for exceptions
            for (Future<Void> job : jobs) {
                job.get(5, TimeUnit.MINUTES);
            }
        } finally {
            IOUtils.closeSilently(conn);
            executor.shutdown();
            executor.awaitTermination(20, TimeUnit.SECONDS);
        }

        deleteDb("lockMode");
    }
}
