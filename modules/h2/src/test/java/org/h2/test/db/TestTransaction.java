/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * Transactional tests, including transaction isolation tests, and tests related
 * to savepoints.
 */
public class TestTransaction extends TestDb {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase init = TestBase.createCaller().init();
        init.config.multiThreaded = true;
        init.test();
    }

    @Override
    public void test() throws Exception {
        testClosingConnectionWithSessionTempTable();
        testClosingConnectionWithLockedTable();
        testConstraintCreationRollback();
        testCommitOnAutoCommitChange();
        testConcurrentSelectForUpdate();
        testLogMode();
        testRollback();
        testRollback2();
        testForUpdate();
        testForUpdate2();
        testForUpdate3();
        testUpdate();
        testMergeUsing();
        testDelete();
        testSetTransaction();
        testReferential();
        testSavepoint();
        testIsolation();
        deleteDb("transaction");
    }

    private void testConstraintCreationRollback() throws SQLException {
        deleteDb("transaction");
        Connection conn = getConnection("transaction");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int, p int)");
        stat.execute("insert into test values(1, 2)");
        try {
            stat.execute("alter table test add constraint fail " +
                    "foreign key(p) references test(id)");
            fail();
        } catch (SQLException e) {
            // expected
        }
        stat.execute("insert into test values(1, 2)");
        stat.execute("drop table test");
        conn.close();
    }

    private void testCommitOnAutoCommitChange() throws SQLException {
        deleteDb("transaction");
        Connection conn = getConnection("transaction");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key)");

        Connection conn2 = getConnection("transaction");
        Statement stat2 = conn2.createStatement();

        conn.setAutoCommit(false);
        stat.execute("insert into test values(1)");

        // should have no effect
        conn.setAutoCommit(false);

        ResultSet rs;
        if (config.mvStore) {
            rs = stat2.executeQuery("select count(*) from test");
            rs.next();
            assertEquals(0, rs.getInt(1));
        } else {
            assertThrows(ErrorCode.LOCK_TIMEOUT_1, stat2).
                executeQuery("select count(*) from test");
        }

        // should commit
        conn.setAutoCommit(true);

        rs = stat2.executeQuery("select * from test");
        assertTrue(rs.next());

        stat.execute("drop table test");

        conn2.close();
        conn.close();
    }

    private void testLogMode() throws SQLException {
        if (config.memory) {
            return;
        }
        if (config.mvStore) {
            return;
        }
        deleteDb("transaction");
        testLogMode(0);
        testLogMode(1);
        testLogMode(2);
    }

    private void testLogMode(int logMode) throws SQLException {
        Connection conn;
        Statement stat;
        ResultSet rs;
        conn = getConnection("transaction");
        stat = conn.createStatement();
        stat.execute("create table test(id int primary key) as select 1");
        stat.execute("set write_delay 0");
        stat.execute("set log " + logMode);
        rs = stat.executeQuery(
                "select value from information_schema.settings where name = 'LOG'");
        rs.next();
        assertEquals(logMode, rs.getInt(1));
        stat.execute("insert into test values(2)");
        stat.execute("shutdown immediately");
        try {
            conn.close();
        } catch (SQLException e) {
            // expected
        }
        conn = getConnection("transaction");
        stat = conn.createStatement();
        rs = stat.executeQuery("select * from test order by id");
        assertTrue(rs.next());
        if (logMode != 0) {
            assertTrue(rs.next());
        }
        assertFalse(rs.next());
        stat.execute("drop table test");
        conn.close();
    }

    private void testConcurrentSelectForUpdate() throws SQLException {
        deleteDb("transaction");
        Connection conn = getConnection("transaction");
        conn.setAutoCommit(false);
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("create table test2(id int primary key, name varchar)");
        stat.execute("insert into test values(1, 'Hello'), (2, 'World')");
        stat.execute("insert into test2 values(1, 'A'), (2, 'B')");
        conn.commit();
        testConcurrentSelectForUpdateImpl(conn, "*");
        testConcurrentSelectForUpdateImpl(conn, "*, count(*) over ()");
        conn.close();
    }

    private void testConcurrentSelectForUpdateImpl(Connection conn, String expressions) throws SQLException {
        Connection conn2;
        PreparedStatement prep;
        prep = conn.prepareStatement("select * from test for update");
        prep.execute();
        conn2 = getConnection("transaction");
        conn2.setAutoCommit(false);
        assertThrows(ErrorCode.LOCK_TIMEOUT_1, conn2.createStatement()).
                execute("select " + expressions + " from test for update");
        conn2.close();
        conn.commit();

        prep = conn.prepareStatement("select " + expressions
                + " from test join test2 on test.id = test2.id for update");
        prep.execute();
        conn2 = getConnection("transaction");
        conn2.setAutoCommit(false);
        assertThrows(ErrorCode.LOCK_TIMEOUT_1, conn2.createStatement()).
                execute("select * from test for update");
        assertThrows(ErrorCode.LOCK_TIMEOUT_1, conn2.createStatement()).
                execute("select * from test2 for update");
        conn2.close();
        conn.commit();
    }

    private void testForUpdate() throws SQLException {
        deleteDb("transaction");
        Connection conn = getConnection("transaction");
        conn.setAutoCommit(false);
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("insert into test values(1, 'Hello'), (2, 'World')");
        conn.commit();
        PreparedStatement prep = conn.prepareStatement(
                "select * from test where id = 1 for update");
        prep.execute();
        // releases the lock
        conn.commit();
        prep.execute();
        Connection conn2 = getConnection("transaction");
        conn2.setAutoCommit(false);
        Statement stat2 = conn2.createStatement();
        if (config.mvStore) {
            stat2.execute("update test set name = 'Welt' where id = 2");
        }
        assertThrows(ErrorCode.LOCK_TIMEOUT_1, stat2).
                execute("update test set name = 'Hallo' where id = 1");
        conn2.close();
        conn.close();
    }

    private void testForUpdate2() throws Exception {
        // Exclude some configurations to avoid spending too much time in sleep()
        if (config.mvStore && !config.multiThreaded || config.networked || config.cipher != null) {
            return;
        }
        deleteDb("transaction");
        Connection conn1 = getConnection("transaction");
        Connection conn2 = getConnection("transaction");
        Statement stat1 = conn1.createStatement();
        stat1.execute("CREATE TABLE TEST (ID INT PRIMARY KEY, V INT)");
        conn1.setAutoCommit(false);
        conn2.createStatement().execute("SET LOCK_TIMEOUT 2000");
        if (config.mvStore) {
            testForUpdate2(conn1, stat1, conn2, false);
        }
        testForUpdate2(conn1, stat1, conn2, true);
        conn1.close();
        conn2.close();
    }

    private void testForUpdate2(Connection conn1, Statement stat1, Connection conn2, boolean forUpdate)
            throws Exception {
        testForUpdate2(conn1, stat1, conn2, forUpdate, false);
        testForUpdate2(conn1, stat1, conn2, forUpdate, true);
    }

    private void testForUpdate2(Connection conn1, Statement stat1, Connection conn2, boolean forUpdate,
            boolean window) throws Exception {
        testForUpdate2(conn1, stat1, conn2, forUpdate, window, false, false);
        testForUpdate2(conn1, stat1, conn2, forUpdate, window, false, true);
        testForUpdate2(conn1, stat1, conn2, forUpdate, window, true, false);
    }

    private void testForUpdate2(Connection conn1, Statement stat1, final Connection conn2, boolean forUpdate,
            boolean window, boolean deleted, boolean excluded) throws Exception {
        stat1.execute("MERGE INTO TEST KEY(ID) VALUES (1, 1)");
        conn1.commit();
        stat1.execute(deleted ? "DELETE FROM TEST WHERE ID = 1" : "UPDATE TEST SET V = 2 WHERE ID = 1");
        final int[] res = new int[1];
        final Exception[] ex = new Exception[1];
        StringBuilder builder = new StringBuilder("SELECT V");
        if (window) {
            builder.append(", RANK() OVER (ORDER BY ID)");
        }
        builder.append(" FROM TEST WHERE ID = 1");
        if (excluded) {
            builder.append(" AND V = 1");
        }
        if (forUpdate) {
            builder.append(" FOR UPDATE");
        }
        String query = builder.toString();
        final PreparedStatement prep2 = conn2.prepareStatement(query);
        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    ResultSet resultSet = prep2.executeQuery();
                    res[0] = resultSet.next() ? resultSet.getInt(1) : -1;
                    conn2.commit();
                } catch (SQLException e) {
                    ex[0] = e;
                }
            }
        };
        t.start();
        Thread.sleep(500);
        conn1.commit();
        t.join();
        if (ex[0] != null) {
            throw ex[0];
        }
        assertEquals(forUpdate ? (deleted || excluded) ? -1 : 2 : 1, res[0]);
    }

    private void testForUpdate3() throws Exception {
        // Exclude some configurations to avoid spending too much time in sleep()
        if (config.mvStore && !config.multiThreaded || config.networked || config.cipher != null) {
            return;
        }
        deleteDb("transaction");
        Connection conn1 = getConnection("transaction");
        final Connection conn2 = getConnection("transaction");
        Statement stat1 = conn1.createStatement();
        stat1.execute("CREATE TABLE TEST (ID INT PRIMARY KEY, V INT UNIQUE)");
        conn1.setAutoCommit(false);
        conn2.createStatement().execute("SET LOCK_TIMEOUT 2000");
        stat1.execute("MERGE INTO TEST KEY(ID) VALUES (1, 1), (2, 2), (3, 3), (4, 4)");
        conn1.commit();
        stat1.execute("UPDATE TEST SET V = 10 - V");
        final Exception[] ex = new Exception[1];
        StringBuilder builder = new StringBuilder("SELECT V FROM TEST ORDER BY V FOR UPDATE");
        String query = builder.toString();
        final PreparedStatement prep2 = conn2.prepareStatement(query);
        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    ResultSet resultSet = prep2.executeQuery();
                    int previous = -1;
                    while (resultSet.next()) {
                        int value = resultSet.getInt(1);
                        assertTrue(previous + ">=" + value, previous < value);
                        previous = value;
                    }
                    conn2.commit();
                } catch (SQLException e) {
                    ex[0] = e;
                }
            }
        };
        t.start();
        Thread.sleep(500);
        conn1.commit();
        t.join();
        if (ex[0] != null) {
            throw ex[0];
        }
        conn1.close();
        conn2.close();
    }

    private void testUpdate() throws Exception {
        final int count = 50;
        deleteDb("transaction");
        final Connection conn1 = getConnection("transaction");
        conn1.setAutoCommit(false);
        Connection conn2 = getConnection("transaction");
        conn2.setAutoCommit(false);
        Statement stat1 = conn1.createStatement();
        Statement stat2 = conn2.createStatement();
        stat1.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, VALUE BOOLEAN) AS "
                + "SELECT X, FALSE FROM GENERATE_SERIES(1, " + count + ')');
        conn1.commit();
        stat1.executeQuery("SELECT * FROM TEST").close();
        stat2.executeQuery("SELECT * FROM TEST").close();
        final int[] r = new int[1];
        Thread t = new Thread() {
            @Override
            public void run() {
                int sum = 0;
                try {
                    PreparedStatement prep = conn1.prepareStatement(
                            "UPDATE TEST SET VALUE = TRUE WHERE ID = ? AND NOT VALUE");
                    for (int i = 1; i <= count; i++) {
                        prep.setInt(1, i);
                        prep.addBatch();
                    }
                    int[] a = prep.executeBatch();
                    for (int i : a) {
                        sum += i;
                    }
                    conn1.commit();
                } catch (SQLException e) {
                    // Ignore
                }
                r[0] = sum;
            }
        };
        t.start();
        int sum = 0;
        PreparedStatement prep = conn2.prepareStatement(
                "UPDATE TEST SET VALUE = TRUE WHERE ID = ? AND NOT VALUE");
        for (int i = 1; i <= count; i++) {
            prep.setInt(1, i);
            prep.addBatch();
        }
        int[] a = prep.executeBatch();
        for (int i : a) {
            sum += i;
        }
        conn2.commit();
        t.join();
        assertEquals(count, sum + r[0]);
        conn2.close();
        conn1.close();
    }

    private void testMergeUsing() throws Exception {
        final int count = 50;
        deleteDb("transaction");
        final Connection conn1 = getConnection("transaction");
        conn1.setAutoCommit(false);
        Connection conn2 = getConnection("transaction");
        conn2.setAutoCommit(false);
        Statement stat1 = conn1.createStatement();
        Statement stat2 = conn2.createStatement();
        stat1.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, VALUE BOOLEAN) AS "
                + "SELECT X, FALSE FROM GENERATE_SERIES(1, " + count + ')');
        conn1.commit();
        stat1.executeQuery("SELECT * FROM TEST").close();
        stat2.executeQuery("SELECT * FROM TEST").close();
        final int[] r = new int[1];
        Thread t = new Thread() {
            @Override
            public void run() {
                int sum = 0;
                try {
                    PreparedStatement prep = conn1.prepareStatement(
                            "MERGE INTO TEST T USING (SELECT ?1::INT X) S ON T.ID = S.X AND NOT T.VALUE"
                            + " WHEN MATCHED THEN UPDATE SET T.VALUE = TRUE"
                            + " WHEN NOT MATCHED THEN INSERT VALUES (10000 + ?1, FALSE)");
                    for (int i = 1; i <= count; i++) {
                        prep.setInt(1, i);
                        prep.addBatch();
                    }
                    int[] a = prep.executeBatch();
                    for (int i : a) {
                        sum += i;
                    }
                    conn1.commit();
                } catch (SQLException e) {
                    // Ignore
                }
                r[0] = sum;
            }
        };
        t.start();
        int sum = 0;
        PreparedStatement prep = conn2.prepareStatement(
                "MERGE INTO TEST T USING (SELECT ?1::INT X) S ON T.ID = S.X AND NOT T.VALUE"
                + " WHEN MATCHED THEN UPDATE SET T.VALUE = TRUE"
                + " WHEN NOT MATCHED THEN INSERT VALUES (10000 + ?1, FALSE)");
        for (int i = 1; i <= count; i++) {
            prep.setInt(1, i);
            prep.addBatch();
        }
        int[] a = prep.executeBatch();
        for (int i : a) {
            sum += i;
        }
        conn2.commit();
        t.join();
        assertEquals(count * 2, sum + r[0]);
        conn2.close();
        conn1.close();
    }

    private void testDelete() throws Exception {
        String sql1 = "DELETE FROM TEST WHERE ID = ? AND NOT VALUE";
        String sql2 = "UPDATE TEST SET VALUE = TRUE WHERE ID = ? AND NOT VALUE";
        testDeleteImpl(sql1, sql2);
        testDeleteImpl(sql2, sql1);
    }

    private void testDeleteImpl(final String sql1, String sql2) throws Exception {
        final int count = 50;
        deleteDb("transaction");
        final Connection conn1 = getConnection("transaction");
        conn1.setAutoCommit(false);
        Connection conn2 = getConnection("transaction");
        conn2.setAutoCommit(false);
        Statement stat1 = conn1.createStatement();
        Statement stat2 = conn2.createStatement();
        stat1.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, VALUE BOOLEAN) AS "
                + "SELECT X, FALSE FROM GENERATE_SERIES(1, " + count + ')');
        conn1.commit();
        stat1.executeQuery("SELECT * FROM TEST").close();
        stat2.executeQuery("SELECT * FROM TEST").close();
        final int[] r = new int[1];
        Thread t = new Thread() {
            @Override
            public void run() {
                int sum = 0;
                try {
                    PreparedStatement prep = conn1.prepareStatement(sql1);
                    for (int i = 1; i <= count; i++) {
                        prep.setInt(1, i);
                        prep.addBatch();
                    }
                    int[] a = prep.executeBatch();
                    for (int i : a) {
                        sum += i;
                    }
                    conn1.commit();
                } catch (SQLException e) {
                    // Ignore
                }
                r[0] = sum;
            }
        };
        t.start();
        int sum = 0;
        PreparedStatement prep = conn2.prepareStatement(
                sql2);
        for (int i = 1; i <= count; i++) {
            prep.setInt(1, i);
            prep.addBatch();
        }
        int[] a = prep.executeBatch();
        for (int i : a) {
            sum += i;
        }
        conn2.commit();
        t.join();
        assertEquals(count, sum + r[0]);
        conn2.close();
        conn1.close();
    }

    private void testRollback() throws SQLException {
        deleteDb("transaction");
        Connection conn = getConnection("transaction");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int)");
        stat.execute("create index idx_id on test(id)");
        stat.execute("insert into test values(1), (1), (1)");
        if (!config.memory) {
            conn.close();
            conn = getConnection("transaction");
            stat = conn.createStatement();
        }
        conn.setAutoCommit(false);
        stat.execute("delete from test");
        conn.rollback();
        ResultSet rs;
        rs = stat.executeQuery("select * from test");
        assertResultRowCount(3, rs);
        rs = stat.executeQuery("select * from test where id = 1");
        assertResultRowCount(3, rs);
        conn.close();

        conn = getConnection("transaction");
        stat = conn.createStatement();
        stat.execute("create table master(id int) as select 1");
        stat.execute("create table child1(id int references master(id) " +
                "on delete cascade)");
        stat.execute("insert into child1 values(1), (1), (1)");
        stat.execute("create table child2(id int references master(id)) as select 1");
        if (!config.memory) {
            conn.close();
            conn = getConnection("transaction");
        }
        stat = conn.createStatement();
        assertThrows(
                ErrorCode.REFERENTIAL_INTEGRITY_VIOLATED_CHILD_EXISTS_1, stat).
                execute("delete from master");
        conn.rollback();
        rs = stat.executeQuery("select * from master where id=1");
        assertResultRowCount(1, rs);
        rs = stat.executeQuery("select * from child1");
        assertResultRowCount(3, rs);
        rs = stat.executeQuery("select * from child1 where id=1");
        assertResultRowCount(3, rs);
        conn.close();
    }

    private void testRollback2() throws SQLException {
        deleteDb("transaction");
        Connection conn = getConnection("transaction");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int)");
        stat.execute("create index idx_id on test(id)");
        stat.execute("insert into test values(1), (1)");
        if (!config.memory) {
            conn.close();
            conn = getConnection("transaction");
            stat = conn.createStatement();
        }
        conn.setAutoCommit(false);
        stat.execute("delete from test");
        conn.rollback();
        ResultSet rs;
        rs = stat.executeQuery("select * from test where id = 1");
        assertResultRowCount(2, rs);
        conn.close();

        conn = getConnection("transaction");
        stat = conn.createStatement();
        stat.execute("create table master(id int) as select 1");
        stat.execute("create table child1(id int references master(id) " +
                "on delete cascade)");
        stat.execute("insert into child1 values(1), (1)");
        stat.execute("create table child2(id int references master(id)) as select 1");
        if (!config.memory) {
            conn.close();
            conn = getConnection("transaction");
        }
        stat = conn.createStatement();
        assertThrows(
                ErrorCode.REFERENTIAL_INTEGRITY_VIOLATED_CHILD_EXISTS_1, stat).
                execute("delete from master");
        rs = stat.executeQuery("select * from master where id=1");
        assertResultRowCount(1, rs);
        rs = stat.executeQuery("select * from child1 where id=1");
        assertResultRowCount(2, rs);
        conn.close();
    }

    private void testSetTransaction() throws SQLException {
        deleteDb("transaction");
        Connection conn = getConnection("transaction");
        conn.setAutoCommit(false);
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int)");
        stat.execute("insert into test values(1)");
        stat.execute("set @x = 1");
        conn.commit();
        assertSingleValue(stat, "select id from test", 1);
        assertSingleValue(stat, "call @x", 1);

        stat.execute("update test set id=2");
        stat.execute("set @x = 2");
        conn.rollback();
        assertSingleValue(stat, "select id from test", 1);
        assertSingleValue(stat, "call @x", 2);

        conn.close();
    }

    private void testReferential() throws SQLException {
        deleteDb("transaction");
        Connection c1 = getConnection("transaction");
        c1.setAutoCommit(false);
        Statement s1 = c1.createStatement();
        s1.execute("drop table if exists a");
        s1.execute("drop table if exists b");
        s1.execute("create table a (id integer identity not null, " +
                "code varchar(10) not null, primary key(id))");
        s1.execute("create table b (name varchar(100) not null, a integer, " +
                "primary key(name), foreign key(a) references a(id))");
        Connection c2 = getConnection("transaction");
        c2.setAutoCommit(false);
        s1.executeUpdate("insert into A(code) values('one')");
        Statement s2 = c2.createStatement();
        if (config.mvStore) {
            assertThrows(
                    ErrorCode.REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1, s2).
                    executeUpdate("insert into B values('two', 1)");
        } else {
            assertThrows(ErrorCode.LOCK_TIMEOUT_1, s2).
                    executeUpdate("insert into B values('two', 1)");
        }
        c2.commit();
        c1.rollback();
        c1.close();
        c2.close();
    }

    private void testClosingConnectionWithLockedTable() throws SQLException {
        deleteDb("transaction");
        Connection c1 = getConnection("transaction");
        Connection c2 = getConnection("transaction");
        c1.setAutoCommit(false);
        c2.setAutoCommit(false);

        Statement s1 = c1.createStatement();
        s1.execute("create table a (id integer identity not null, " +
                "code varchar(10) not null, primary key(id))");
        s1.executeUpdate("insert into a(code) values('one')");
        c1.commit();
        s1.executeQuery("select * from a for update");
        c1.close();

        Statement s2 = c2.createStatement();
        s2.executeQuery("select * from a for update");
        c2.close();
    }

    private void testClosingConnectionWithSessionTempTable() throws SQLException {
        deleteDb("transaction");
        Connection c1 = getConnection("transaction");
        Connection c2 = getConnection("transaction");
        c1.setAutoCommit(false);
        c2.setAutoCommit(false);

        Statement s1 = c1.createStatement();
        s1.execute("create local temporary table a (id int, x BLOB)");
        c1.commit();
        c1.close();

        Statement s2 = c2.createStatement();
        s2.execute("create table c (id int)");
        c2.close();
    }

    private void testSavepoint() throws SQLException {
        deleteDb("transaction");
        Connection conn = getConnection("transaction");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST0(ID IDENTITY, NAME VARCHAR)");
        stat.execute("CREATE TABLE TEST1(NAME VARCHAR, " +
                "ID IDENTITY, X TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");
        conn.setAutoCommit(false);
        int[] count = new int[2];
        int[] countCommitted = new int[2];
        int[] countSave = new int[2];
        int len = getSize(2000, 10000);
        Random random = new Random(10);
        Savepoint sp = null;
        for (int i = 0; i < len; i++) {
            int tableId = random.nextInt(2);
            String table = "TEST" + tableId;
            int op = random.nextInt(6);
            switch (op) {
            case 0:
                stat.execute("INSERT INTO " + table + "(NAME) VALUES('op" + i + "')");
                count[tableId]++;
                break;
            case 1:
                if (count[tableId] > 0) {
                    int updateCount = stat.executeUpdate(
                            "DELETE FROM " + table +
                            " WHERE ID=SELECT MIN(ID) FROM " + table);
                    assertEquals(1, updateCount);
                    count[tableId]--;
                }
                break;
            case 2:
                sp = conn.setSavepoint();
                countSave[0] = count[0];
                countSave[1] = count[1];
                break;
            case 3:
                if (sp != null) {
                    conn.rollback(sp);
                    count[0] = countSave[0];
                    count[1] = countSave[1];
                }
                break;
            case 4:
                conn.commit();
                sp = null;
                countCommitted[0] = count[0];
                countCommitted[1] = count[1];
                break;
            case 5:
                conn.rollback();
                sp = null;
                count[0] = countCommitted[0];
                count[1] = countCommitted[1];
                break;
            default:
            }
            checkTableCount(stat, "TEST0", count[0]);
            checkTableCount(stat, "TEST1", count[1]);
        }
        conn.close();
    }

    private void checkTableCount(Statement stat, String tableName, int count)
            throws SQLException {
        ResultSet rs;
        rs = stat.executeQuery("SELECT COUNT(*) FROM " + tableName);
        rs.next();
        assertEquals(count, rs.getInt(1));
    }

    private void testIsolation() throws SQLException {
        Connection conn = getConnection("transaction");
        trace("default TransactionIsolation=" + conn.getTransactionIsolation());
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        assertTrue(conn.getTransactionIsolation() ==
                Connection.TRANSACTION_READ_COMMITTED);
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        assertTrue(conn.getTransactionIsolation() ==
                Connection.TRANSACTION_SERIALIZABLE);
        Statement stat = conn.createStatement();
        assertTrue(conn.getAutoCommit());
        conn.setAutoCommit(false);
        assertFalse(conn.getAutoCommit());
        conn.setAutoCommit(true);
        assertTrue(conn.getAutoCommit());
        test(stat, "CREATE TABLE TEST(ID INT PRIMARY KEY)");
        conn.commit();
        test(stat, "INSERT INTO TEST VALUES(0)");
        conn.rollback();
        testValue(stat, "SELECT COUNT(*) FROM TEST", "1");
        conn.setAutoCommit(false);
        test(stat, "DELETE FROM TEST");
        // testValue("SELECT COUNT(*) FROM TEST", "0");
        conn.rollback();
        testValue(stat, "SELECT COUNT(*) FROM TEST", "1");
        conn.commit();
        conn.setAutoCommit(true);
        testNestedResultSets(conn);
        conn.setAutoCommit(false);
        testNestedResultSets(conn);
        conn.close();
    }

    private void testNestedResultSets(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        test(stat, "CREATE TABLE NEST1(ID INT PRIMARY KEY,VALUE VARCHAR(255))");
        test(stat, "CREATE TABLE NEST2(ID INT PRIMARY KEY,VALUE VARCHAR(255))");
        DatabaseMetaData meta = conn.getMetaData();
        ArrayList<String> result = new ArrayList<>();
        ResultSet rs1, rs2;
        rs1 = meta.getTables(null, null, "NEST%", null);
        while (rs1.next()) {
            String table = rs1.getString("TABLE_NAME");
            rs2 = meta.getColumns(null, null, table, null);
            while (rs2.next()) {
                String column = rs2.getString("COLUMN_NAME");
                trace("Table: " + table + " Column: " + column);
                result.add(table + "." + column);
            }
        }
        // should be NEST1.ID, NEST1.NAME, NEST2.ID, NEST2.NAME
        assertEquals(result.toString(), 4, result.size());
        result = new ArrayList<>();
        test(stat, "INSERT INTO NEST1 VALUES(1,'A')");
        test(stat, "INSERT INTO NEST1 VALUES(2,'B')");
        test(stat, "INSERT INTO NEST2 VALUES(1,'1')");
        test(stat, "INSERT INTO NEST2 VALUES(2,'2')");
        Statement s1 = conn.createStatement();
        Statement s2 = conn.createStatement();
        rs1 = s1.executeQuery("SELECT * FROM NEST1 ORDER BY ID");
        while (rs1.next()) {
            rs2 = s2.executeQuery("SELECT * FROM NEST2 ORDER BY ID");
            while (rs2.next()) {
                String v1 = rs1.getString("VALUE");
                String v2 = rs2.getString("VALUE");
                result.add(v1 + "/" + v2);
            }
        }
        // should be A/1, A/2, B/1, B/2
        assertEquals(result.toString(), 4, result.size());
        result = new ArrayList<>();
        rs1 = s1.executeQuery("SELECT * FROM NEST1 ORDER BY ID");
        rs2 = s1.executeQuery("SELECT * FROM NEST2 ORDER BY ID");
        assertThrows(ErrorCode.OBJECT_CLOSED, rs1).next();
        // this is already closed, so but closing again should no do any harm
        rs1.close();
        while (rs2.next()) {
            String v1 = rs2.getString("VALUE");
            result.add(v1);
        }
        // should be A, B
        assertEquals(result.toString(), 2, result.size());
        test(stat, "DROP TABLE NEST1");
        test(stat, "DROP TABLE NEST2");
    }

    private void testValue(Statement stat, String sql, String data)
            throws SQLException {
        ResultSet rs = stat.executeQuery(sql);
        rs.next();
        String s = rs.getString(1);
        assertEquals(data, s);
    }

    private void test(Statement stat, String sql) throws SQLException {
        trace(sql);
        stat.execute(sql);
    }

}
