/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.h2.api.DatabaseEventListener;
import org.h2.api.ErrorCode;
import org.h2.result.Row;
import org.h2.result.RowImpl;
import org.h2.store.Page;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.util.IOUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.New;

/**
 * Test the page store.
 */
public class TestPageStore extends TestBase {

    /**
     * The events log.
     */
    static StringBuilder eventBuffer = new StringBuilder();

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        System.setProperty("h2.check2", "true");
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        if (config.memory) {
            return;
        }
        deleteDb(null);
        testDropTempTable();
        testLogLimitFalsePositive();
        testLogLimit();
        testRecoverLobInDatabase();
        testWriteTransactionLogBeforeData();
        testDefrag();
        testInsertReverse();
        testInsertDelete();
        testCheckpoint();
        testDropRecreate();
        testDropAll();
        testCloseTempTable();
        testDuplicateKey();
        testUpdateOverflow();
        testTruncateReconnect();
        testReverseIndex();
        testLargeUpdates();
        testLargeInserts();
        testLargeDatabaseFastOpen();
        testUniqueIndexReopen();
        testLargeRows();
        testRecoverDropIndex();
        testDropPk();
        testCreatePkLater();
        testTruncate();
        testLargeIndex();
        testUniqueIndex();
        testCreateIndexLater();
        testFuzzOperations();
        deleteDb(null);
    }

    private void testDropTempTable() throws SQLException {
        deleteDb("pageStoreDropTemp");
        Connection c1 = getConnection("pageStoreDropTemp");
        Connection c2 = getConnection("pageStoreDropTemp");
        c1.setAutoCommit(false);
        c2.setAutoCommit(false);
        Statement s1 = c1.createStatement();
        Statement s2 = c2.createStatement();
        s1.execute("create local temporary table a(id int primary key)");
        s1.execute("insert into a values(1)");
        c1.commit();
        c1.close();
        s2.execute("create table b(id int primary key)");
        s2.execute("insert into b values(1)");
        c2.commit();
        s2.execute("checkpoint sync");
        s2.execute("shutdown immediately");
        try {
            c2.close();
        } catch (SQLException e) {
            // ignore
        }
        c1 = getConnection("pageStoreDropTemp");
        c1.close();
        deleteDb("pageStoreDropTemp");
    }

    private void testLogLimit() throws Exception {
        if (config.mvStore) {
            return;
        }
        deleteDb("pageStoreLogLimit");
        Connection conn, conn2;
        Statement stat, stat2;
        String url = "pageStoreLogLimit;TRACE_LEVEL_FILE=2";
        conn = getConnection(url);
        stat = conn.createStatement();
        stat.execute("create table test(id int primary key)");
        conn.setAutoCommit(false);
        stat.execute("insert into test values(1)");

        conn2 = getConnection(url);
        stat2 = conn2.createStatement();
        stat2.execute("create table t2(id identity, name varchar)");
        stat2.execute("set max_log_size 1");
        for (int i = 0; i < 10; i++) {
            stat2.execute("insert into t2(name) " +
                    "select space(100) from system_range(1, 1000)");
        }
        InputStream in = FileUtils.newInputStream(getBaseDir() +
                "/pageStoreLogLimit.trace.db");
        String s = IOUtils.readStringAndClose(new InputStreamReader(in), -1);
        assertContains(s, "Transaction log could not be truncated");
        conn.commit();
        ResultSet rs = stat2.executeQuery("select * from test");
        assertTrue(rs.next());
        conn2.close();
        conn.close();
    }

    private void testLogLimitFalsePositive() throws Exception {
        deleteDb("pageStoreLogLimitFalsePositive");
        String url = "pageStoreLogLimitFalsePositive;TRACE_LEVEL_FILE=2";
        Connection conn = getConnection(url);
        Statement stat = conn.createStatement();
        stat.execute("set max_log_size 1");
        stat.execute("create table test(x varchar)");
        for (int i = 0; i < 1000; ++i) {
            stat.execute("insert into test values (space(2000))");
        }
        stat.execute("checkpoint");
        InputStream in = FileUtils.newInputStream(getBaseDir() +
                "/pageStoreLogLimitFalsePositive.trace.db");
        String s = IOUtils.readStringAndClose(new InputStreamReader(in), -1);
        assertFalse(s.indexOf("Transaction log could not be truncated") > 0);
        conn.close();
    }

    private void testRecoverLobInDatabase() throws SQLException {
        deleteDb("pageStoreRecoverLobInDatabase");
        String url = getURL("pageStoreRecoverLobInDatabase;" +
                "MVCC=TRUE;CACHE_SIZE=1", true);
        Connection conn;
        Statement stat;
        conn = getConnection(url, getUser(), getPassword());
        stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name clob)");
        stat.execute("create index idx_id on test(id)");
        stat.execute("insert into test " +
                "select x, space(1100+x) from system_range(1, 100)");
        Random r = new Random(1);
        ArrayList<Connection> list = New.arrayList();
        for (int i = 0; i < 10; i++) {
            Connection conn2 = getConnection(url, getUser(), getPassword());
            list.add(conn2);
            Statement stat2 = conn2.createStatement();
            conn2.setAutoCommit(false);
            if (r.nextBoolean()) {
                stat2.execute("update test set id = id where id = " + r.nextInt(100));
            } else {
                stat2.execute("delete from test where id = " + r.nextInt(100));
            }
        }
        stat.execute("shutdown immediately");
        JdbcUtils.closeSilently(conn);
        for (Connection c : list) {
            JdbcUtils.closeSilently(c);
        }
        conn = getConnection(url, getUser(), getPassword());
        conn.close();
    }

    private void testWriteTransactionLogBeforeData() throws SQLException {
        deleteDb("pageStoreWriteTransactionLogBeforeData");
        String url = getURL("pageStoreWriteTransactionLogBeforeData;" +
                "CACHE_SIZE=16;WRITE_DELAY=1000000", true);
        Connection conn;
        Statement stat;
        conn = getConnection(url, getUser(), getPassword());
        stat = conn.createStatement();
        stat.execute("create table test(name varchar) as select space(100000)");
        for (int i = 0; i < 100; i++) {
            stat.execute("create table test" + i + "(id int) " +
                    "as select x from system_range(1, 1000)");
        }
        conn.close();
        conn = getConnection(url, getUser(), getPassword());
        stat = conn.createStatement();
        stat.execute("drop table test0");
        stat.execute("select * from test");
        stat.execute("shutdown immediately");
        try {
            conn.close();
        } catch (Exception e) {
            // ignore
        }
        conn = getConnection(url, getUser(), getPassword());
        stat = conn.createStatement();
        for (int i = 1; i < 100; i++) {
            stat.execute("select * from test" + i);
        }
        conn.close();
    }

    private void testDefrag() throws SQLException {
        if (config.reopen || config.multiThreaded) {
            return;
        }
        deleteDb("pageStoreDefrag");
        Connection conn = getConnection(
                "pageStoreDefrag;LOG=0;UNDO_LOG=0;LOCK_MODE=0");
        Statement stat = conn.createStatement();
        int tableCount = 10;
        int rowCount = getSize(1000, 100000);
        for (int i = 0; i < tableCount; i++) {
            stat.execute("create table test" + i + "(id int primary key, " +
                    "string1 varchar, string2 varchar, string3 varchar)");
        }
        for (int j = 0; j < tableCount; j++) {
            PreparedStatement prep = conn.prepareStatement(
                    "insert into test" + j + " values(?, ?, ?, ?)");
            for (int i = 0; i < rowCount; i++) {
                prep.setInt(1, i);
                prep.setInt(2, i);
                prep.setInt(3, i);
                prep.setInt(4, i);
                prep.execute();
            }
        }
        stat.executeUpdate("shutdown defrag");
        conn.close();
    }

    private void testInsertReverse() throws SQLException {
        deleteDb("pageStoreInsertReverse");
        Connection conn;
        conn = getConnection("pageStoreInsertReverse");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, data varchar)");
        stat.execute("insert into test select -x, space(100) " +
                "from system_range(1, 1000)");
        stat.execute("drop table test");
        stat.execute("create table test(id int primary key, data varchar)");
        stat.execute("insert into test select -x, space(2048) " +
                "from system_range(1, 1000)");
        conn.close();
    }

    private void testInsertDelete() {
        Row[] x = new Row[0];
        Row r = new RowImpl(null, 0);
        x = Page.insert(x, 0, 0, r);
        assertTrue(x[0] == r);
        Row r2 = new RowImpl(null, 0);
        x = Page.insert(x, 1, 0, r2);
        assertTrue(x[0] == r2);
        assertTrue(x[1] == r);
        Row r3 = new RowImpl(null, 0);
        x = Page.insert(x, 2, 1, r3);
        assertTrue(x[0] == r2);
        assertTrue(x[1] == r3);
        assertTrue(x[2] == r);

        x = Page.remove(x, 3, 1);
        assertTrue(x[0] == r2);
        assertTrue(x[1] == r);
        x = Page.remove(x, 2, 0);
        assertTrue(x[0] == r);
        x = Page.remove(x, 1, 0);
    }

    private void testCheckpoint() throws SQLException {
        deleteDb("pageStoreCheckpoint");
        Connection conn;
        conn = getConnection("pageStoreCheckpoint");
        Statement stat = conn.createStatement();
        stat.execute("create table test(data varchar)");
        stat.execute("create sequence seq");
        stat.execute("set max_log_size 1");
        conn.setAutoCommit(false);
        stat.execute("insert into test select space(1000) from system_range(1, 1000)");
        long before = System.nanoTime();
        stat.execute("select nextval('SEQ') from system_range(1, 100000)");
        long after = System.nanoTime();
        // it's hard to test - basically it shouldn't checkpoint too often
        if (after - before > TimeUnit.SECONDS.toNanos(20)) {
            if (!config.reopen) {
                fail("Checkpoint took " + TimeUnit.NANOSECONDS.toMillis(after - before) + " ms");
            }
        }
        stat.execute("drop table test");
        stat.execute("drop sequence seq");
        conn.close();
    }

    private void testDropRecreate() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("pageStoreDropRecreate");
        Connection conn;
        conn = getConnection("pageStoreDropRecreate");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int)");
        stat.execute("create index idx_test on test(id)");
        stat.execute("create table test2(id int)");
        stat.execute("drop table test");
        // this will re-used the object id of the test table,
        // which is lower than the object id of test2
        stat.execute("create index idx_test on test2(id)");
        conn.close();
        conn = getConnection("pageStoreDropRecreate");
        conn.close();
    }

    private void testDropAll() throws SQLException {
        deleteDb("pageStoreDropAll");
        Connection conn;
        String url = "pageStoreDropAll";
        conn = getConnection(url);
        Statement stat = conn.createStatement();
        stat.execute("CREATE TEMP TABLE A(A INT)");
        stat.execute("CREATE TABLE B(A VARCHAR IDENTITY)");
        stat.execute("CREATE TEMP TABLE C(A INT)");
        conn.close();
        conn = getConnection(url);
        stat = conn.createStatement();
        stat.execute("DROP ALL OBJECTS");
        conn.close();
    }

    private void testCloseTempTable() throws SQLException {
        deleteDb("pageStoreCloseTempTable");
        Connection conn;
        String url = "pageStoreCloseTempTable;CACHE_SIZE=0";
        conn = getConnection(url);
        Statement stat = conn.createStatement();
        stat.execute("create local temporary table test(id int)");
        conn.rollback();
        Connection conn2 = getConnection(url);
        Statement stat2 = conn2.createStatement();
        stat2.execute("create table test2 as select x from system_range(1, 5000)");
        stat2.execute("shutdown immediately");
        assertThrows(ErrorCode.DATABASE_IS_CLOSED, conn).close();
        assertThrows(ErrorCode.DATABASE_IS_CLOSED, conn2).close();
    }

    private void testDuplicateKey() throws SQLException {
        deleteDb("pageStoreDuplicateKey");
        Connection conn;
        conn = getConnection("pageStoreDuplicateKey");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("insert into test values(0, space(3000))");
        try {
            stat.execute("insert into test values(0, space(3000))");
        } catch (SQLException e) {
            // ignore
        }
        stat.execute("select * from test");
        conn.close();
    }

    private void testTruncateReconnect() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("pageStoreTruncateReconnect");
        Connection conn;
        conn = getConnection("pageStoreTruncateReconnect");
        conn.createStatement().execute(
                "create table test(id int primary key, name varchar)");
        conn.createStatement().execute(
                "insert into test(id) select x from system_range(1, 390)");
        conn.createStatement().execute("checkpoint");
        conn.createStatement().execute("shutdown immediately");
        JdbcUtils.closeSilently(conn);
        conn = getConnection("pageStoreTruncateReconnect");
        conn.createStatement().execute("truncate table test");
        conn.createStatement().execute(
                "insert into test(id) select x from system_range(1, 390)");
        conn.createStatement().execute("shutdown immediately");
        JdbcUtils.closeSilently(conn);
        conn = getConnection("pageStoreTruncateReconnect");
        conn.close();
    }

    private void testUpdateOverflow() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("pageStoreUpdateOverflow");
        Connection conn;
        conn = getConnection("pageStoreUpdateOverflow");
        conn.createStatement().execute("create table test" +
                "(id int primary key, name varchar)");
        conn.createStatement().execute(
                "insert into test values(0, space(3000))");
        conn.createStatement().execute("checkpoint");
        conn.createStatement().execute("shutdown immediately");

        JdbcUtils.closeSilently(conn);
        conn = getConnection("pageStoreUpdateOverflow");
        conn.createStatement().execute("update test set id = 1");
        conn.createStatement().execute("shutdown immediately");

        JdbcUtils.closeSilently(conn);
        conn = getConnection("pageStoreUpdateOverflow");
        conn.close();
    }

    private void testReverseIndex() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("pageStoreReverseIndex");
        Connection conn = getConnection("pageStoreReverseIndex");
        Statement stat = conn.createStatement();
        stat.execute("create table test(x int, y varchar default space(200))");
        for (int i = 30; i < 100; i++) {
            stat.execute("insert into test(x) select null from system_range(1, " + i + ")");
            stat.execute("insert into test(x) select x from system_range(1, " + i + ")");
            stat.execute("create index idx on test(x desc, y)");
            ResultSet rs = stat.executeQuery("select min(x) from test");
            rs.next();
            assertEquals(1, rs.getInt(1));
            stat.execute("drop index idx");
            stat.execute("truncate table test");
        }
        conn.close();
    }

    private void testLargeUpdates() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("pageStoreLargeUpdates");
        Connection conn;
        conn = getConnection("pageStoreLargeUpdates");
        Statement stat = conn.createStatement();
        int size = 1500;
        stat.execute("call rand(1)");
        stat.execute(
                "create table test(id int primary key, data varchar, test int) as " +
                "select x, '', 123 from system_range(1, " + size + ")");
        Random random = new Random(1);
        PreparedStatement prep = conn.prepareStatement(
                "update test set data=space(?) where id=?");
        for (int i = 0; i < 2500; i++) {
            int id = random.nextInt(size);
            int newSize = random.nextInt(6000);
            prep.setInt(1, newSize);
            prep.setInt(2, id);
            prep.execute();
        }
        conn.close();
        conn = getConnection("pageStoreLargeUpdates");
        stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select * from test where test<>123");
        assertFalse(rs.next());
        conn.close();
    }

    private void testLargeInserts() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("pageStoreLargeInserts");
        Connection conn;
        conn = getConnection("pageStoreLargeInserts");
        Statement stat = conn.createStatement();
        stat.execute("create table test(data varchar)");
        stat.execute("insert into test values(space(1024 * 1024))");
        stat.execute("insert into test values(space(1024 * 1024))");
        conn.close();
    }

    private void testLargeDatabaseFastOpen() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("pageStoreLargeDatabaseFastOpen");
        Connection conn;
        String url = "pageStoreLargeDatabaseFastOpen";
        conn = getConnection(url);
        conn.createStatement().execute(
                "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        conn.createStatement().execute(
                "create unique index idx_test_name on test(name)");
        conn.createStatement().execute(
                "INSERT INTO TEST " +
                "SELECT X, X || space(10) FROM SYSTEM_RANGE(1, 1000)");
        conn.close();
        conn = getConnection(url);
        conn.createStatement().execute("DELETE FROM TEST WHERE ID=1");
        conn.createStatement().execute("CHECKPOINT");
        conn.createStatement().execute("SHUTDOWN IMMEDIATELY");
        try {
            conn.close();
        } catch (SQLException e) {
            // ignore
        }
        eventBuffer.setLength(0);
        conn = getConnection(url + ";DATABASE_EVENT_LISTENER='" +
                MyDatabaseEventListener.class.getName() + "'");
        assertEquals("init;opened;", eventBuffer.toString());
        conn.close();
    }

    private void testUniqueIndexReopen() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("pageStoreUniqueIndexReopen");
        Connection conn;
        String url = "pageStoreUniqueIndexReopen";
        conn = getConnection(url);
        conn.createStatement().execute(
                "CREATE TABLE test(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        conn.createStatement().execute(
                "create unique index idx_test_name on test(name)");
        conn.createStatement().execute("INSERT INTO TEST VALUES(1, 'Hello')");
        conn.close();
        conn = getConnection(url);
        assertThrows(ErrorCode.DUPLICATE_KEY_1, conn.createStatement())
                .execute("INSERT INTO TEST VALUES(2, 'Hello')");
        conn.close();
    }

    private void testLargeRows() throws Exception {
        if (config.memory) {
            return;
        }
        for (int i = 0; i < 10; i++) {
            testLargeRows(i);
        }
    }

    private void testLargeRows(int seed) throws Exception {
        deleteDb("pageStoreLargeRows");
        String url = getURL("pageStoreLargeRows;CACHE_SIZE=16", true);
        Connection conn = null;
        Statement stat = null;
        int count = 0;
        try {
            Class.forName("org.h2.Driver");
            conn = DriverManager.getConnection(url);
            stat = conn.createStatement();
            int tableCount = 1;
            PreparedStatement[] insert = new PreparedStatement[tableCount];
            PreparedStatement[] deleteMany = new PreparedStatement[tableCount];
            PreparedStatement[] updateMany = new PreparedStatement[tableCount];
            for (int i = 0; i < tableCount; i++) {
                stat.execute("create table test" + i +
                        "(id int primary key, name varchar)");
                stat.execute("create index idx_test" + i + " on test" + i +
                        "(name)");
                insert[i] = conn.prepareStatement("insert into test" + i +
                        " values(?, ? || space(?))");
                deleteMany[i] = conn.prepareStatement("delete from test" + i +
                        " where id between ? and ?");
                updateMany[i] = conn.prepareStatement("update test" + i +
                        " set name=? || space(?) where id between ? and ?");
            }
            Random random = new Random(seed);
            for (int i = 0; i < 1000; i++) {
                count = i;
                PreparedStatement p;
                if (random.nextInt(100) < 95) {
                    p = insert[random.nextInt(tableCount)];
                    p.setInt(1, i);
                    p.setInt(2, i);
                    if (random.nextInt(30) == 5) {
                        p.setInt(3, 3000);
                    } else {
                        p.setInt(3, random.nextInt(100));
                    }
                    p.execute();
                } else if (random.nextInt(100) < 90) {
                    p = updateMany[random.nextInt(tableCount)];
                    p.setInt(1, i);
                    p.setInt(2, random.nextInt(50));
                    int first = random.nextInt(1 + i);
                    p.setInt(3, first);
                    p.setInt(4, first + random.nextInt(50));
                    p.executeUpdate();
                } else {
                    p = deleteMany[random.nextInt(tableCount)];
                    int first = random.nextInt(1 + i);
                    p.setInt(1, first);
                    p.setInt(2, first + random.nextInt(100));
                    p.executeUpdate();
                }
            }
            conn.close();
            conn = DriverManager.getConnection(url);
            conn.close();
            conn = DriverManager.getConnection(url);
            stat = conn.createStatement();
            stat.execute("script to '" + getBaseDir() + "/pageStoreLargeRows.sql'");
            conn.close();
            FileUtils.delete(getBaseDir() + "/pageStoreLargeRows.sql");
        } catch (Exception e) {
            if (stat != null) {
                try {
                    stat.execute("shutdown immediately");
                } catch (SQLException e2) {
                    // ignore
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e2) {
                    // ignore
                }
            }
            throw new RuntimeException("count: " + count, e);
        }
    }

    private void testRecoverDropIndex() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("pageStoreRecoverDropIndex");
        Connection conn = getConnection("pageStoreRecoverDropIndex");
        Statement stat = conn.createStatement();
        stat.execute("set write_delay 0");
        stat.execute("create table test(id int, name varchar) " +
                "as select x, x from system_range(1, 1400)");
        stat.execute("create index idx_name on test(name)");
        conn.close();
        conn = getConnection("pageStoreRecoverDropIndex");
        stat = conn.createStatement();
        stat.execute("drop index idx_name");
        stat.execute("shutdown immediately");
        try {
            conn.close();
        } catch (SQLException e) {
            // ignore
        }
        conn = getConnection("pageStoreRecoverDropIndex;cache_size=1");
        conn.close();
    }

    private void testDropPk() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("pageStoreDropPk");
        Connection conn;
        Statement stat;
        conn = getConnection("pageStoreDropPk");
        stat = conn.createStatement();
        stat.execute("create table test(id int primary key)");
        stat.execute("insert into test values(" + Integer.MIN_VALUE + "), (" +
                Integer.MAX_VALUE + ")");
        stat.execute("alter table test drop primary key");
        conn.close();
        conn = getConnection("pageStoreDropPk");
        stat = conn.createStatement();
        stat.execute("insert into test values(" + Integer.MIN_VALUE + "), (" +
                Integer.MAX_VALUE + ")");
        conn.close();
    }

    private void testCreatePkLater() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("pageStoreCreatePkLater");
        Connection conn;
        Statement stat;
        conn = getConnection("pageStoreCreatePkLater");
        stat = conn.createStatement();
        stat.execute("create table test(id int not null) as select 100");
        stat.execute("create primary key on test(id)");
        conn.close();
        conn = getConnection("pageStoreCreatePkLater");
        stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select * from test where id = 100");
        assertTrue(rs.next());
        conn.close();
    }

    private void testTruncate() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("pageStoreTruncate");
        Connection conn = getConnection("pageStoreTruncate");
        Statement stat = conn.createStatement();
        stat.execute("set write_delay 0");
        stat.execute("create table test(id int) as select 1");
        stat.execute("truncate table test");
        stat.execute("insert into test values(1)");
        stat.execute("shutdown immediately");
        try {
            conn.close();
        } catch (SQLException e) {
            // ignore
        }
        conn = getConnection("pageStoreTruncate");
        conn.close();
    }

    private void testLargeIndex() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("pageStoreLargeIndex");
        Connection conn = getConnection("pageStoreLargeIndex");
        conn.createStatement().execute(
                "create table test(id varchar primary key, d varchar)");
        PreparedStatement prep = conn.prepareStatement(
                "insert into test values(?, space(500))");
        for (int i = 0; i < 20000; i++) {
            prep.setString(1, "" + i);
            prep.executeUpdate();
        }
        conn.close();
    }

    private void testUniqueIndex() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("pageStoreUniqueIndex");
        Connection conn = getConnection("pageStoreUniqueIndex");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT UNIQUE)");
        stat.execute("INSERT INTO TEST VALUES(1)");
        conn.close();
        conn = getConnection("pageStoreUniqueIndex");
        assertThrows(ErrorCode.DUPLICATE_KEY_1,
                conn.createStatement()).execute("INSERT INTO TEST VALUES(1)");
        conn.close();
    }

    private void testCreateIndexLater() throws SQLException {
        deleteDb("pageStoreCreateIndexLater");
        Connection conn = getConnection("pageStoreCreateIndexLater");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(NAME VARCHAR) AS SELECT 1");
        stat.execute("CREATE INDEX IDX_N ON TEST(NAME)");
        stat.execute("INSERT INTO TEST SELECT X FROM SYSTEM_RANGE(20, 100)");
        stat.execute("INSERT INTO TEST SELECT X FROM SYSTEM_RANGE(1000, 1100)");
        stat.execute("SHUTDOWN IMMEDIATELY");
        assertThrows(ErrorCode.DATABASE_IS_CLOSED, conn).close();
        conn = getConnection("pageStoreCreateIndexLater");
        conn.close();
    }

    private void testFuzzOperations() throws Exception {
        int best = Integer.MAX_VALUE;
        for (int i = 0; i < 10; i++) {
            int x = testFuzzOperationsSeed(i, 10);
            if (x >= 0 && x < best) {
                best = x;
                fail("op:" + x + " seed:" + i);
            }
        }
    }

    private int testFuzzOperationsSeed(int seed, int len) throws SQLException {
        deleteDb("pageStoreFuzz");
        Connection conn = getConnection("pageStoreFuzz");
        Statement stat = conn.createStatement();
        log("DROP TABLE IF EXISTS TEST;");
        stat.execute("DROP TABLE IF EXISTS TEST");
        log("CREATE TABLE TEST(ID INT PRIMARY KEY, " +
                "NAME VARCHAR DEFAULT 'Hello World');");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, " +
                "NAME VARCHAR DEFAULT 'Hello World')");
        Set<Integer> rows = new TreeSet<>();
        Random random = new Random(seed);
        for (int i = 0; i < len; i++) {
            int op = random.nextInt(3);
            Integer x = random.nextInt(100);
            switch (op) {
            case 0:
                if (!rows.contains(x)) {
                    log("insert into test(id) values(" + x + ");");
                    stat.execute("INSERT INTO TEST(ID) VALUES(" + x + ");");
                    rows.add(x);
                }
                break;
            case 1:
                if (rows.contains(x)) {
                    log("delete from test where id=" + x + ";");
                    stat.execute("DELETE FROM TEST WHERE ID=" + x);
                    rows.remove(x);
                }
                break;
            case 2:
                conn.close();
                conn = getConnection("pageStoreFuzz");
                stat = conn.createStatement();
                ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
                log("--reconnect");
                for (int test : rows) {
                    if (!rs.next()) {
                        log("error: expected next");
                        conn.close();
                        return i;
                    }
                    int y = rs.getInt(1);
                    // System.out.println(" " + x);
                    if (y != test) {
                        log("error: " + y + " <> " + test);
                        conn.close();
                        return i;
                    }
                }
                if (rs.next()) {
                    log("error: unexpected next");
                    conn.close();
                    return i;
                }
            }
        }
        conn.close();
        return -1;
    }

    private void log(String m) {
        trace("   " + m);
    }

    /**
     * A database event listener used in this test.
     */
    public static final class MyDatabaseEventListener implements
            DatabaseEventListener {

        @Override
        public void closingDatabase() {
            event("closing");
        }

        @Override
        public void exceptionThrown(SQLException e, String sql) {
            event("exceptionThrown " + e + " " + sql);
        }

        @Override
        public void init(String url) {
            event("init");
        }

        @Override
        public void opened() {
            event("opened");
        }

        @Override
        public void setProgress(int state, String name, int x, int max) {
            if (name.startsWith("SYS:SYS_ID")) {
                // ignore
                return;
            }
            switch (state) {
            case DatabaseEventListener.STATE_STATEMENT_START:
            case DatabaseEventListener.STATE_STATEMENT_END:
            case DatabaseEventListener.STATE_STATEMENT_PROGRESS:
                return;
            }
            event("setProgress " + state + " " + name + " " + x + " " + max);
        }

        private static void event(String s) {
            eventBuffer.append(s).append(';');
        }
    }
}
