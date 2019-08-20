/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.h2.api.ErrorCode;
import org.h2.command.dml.Select;
import org.h2.result.SortOrder;
import org.h2.test.TestBase;
import org.h2.tools.SimpleResultSet;
import org.h2.value.ValueInt;

/**
 * Index tests.
 */
public class TestIndex extends TestBase {

    private static int testFunctionIndexCounter;

    private Connection conn;
    private Statement stat;
    private final Random random = new Random();

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws SQLException {
        deleteDb("index");
        testOrderIndex();
        testIndexTypes();
        testHashIndexOnMemoryTable();
        testErrorMessage();
        testDuplicateKeyException();
        testConcurrentUpdate();
        testNonUniqueHashIndex();
        testRenamePrimaryKey();
        testRandomized();
        testDescIndex();
        testHashIndex();

        if (config.networked && config.big) {
            return;
        }

        random.setSeed(100);

        deleteDb("index");
        testWideIndex(147);
        testWideIndex(313);
        testWideIndex(979);
        testWideIndex(1200);
        testWideIndex(2400);
        if (config.big) {
            Random r = new Random();
            for (int j = 0; j < 10; j++) {
                int i = r.nextInt(3000);
                if ((i % 100) == 0) {
                    println("width: " + i);
                }
                testWideIndex(i);
            }
        }

        testLike();
        reconnect();
        testConstraint();
        testLargeIndex();
        testMultiColumnIndex();
        // long time;
        // time = System.nanoTime();
        testHashIndex(true, false);

        testHashIndex(false, false);
        testHashIndex(true, true);
        testHashIndex(false, true);

        testMultiColumnHashIndex();

        testFunctionIndex();

        conn.close();
        deleteDb("index");
    }

    private void testOrderIndex() throws SQLException {
        Connection conn = getConnection("index");
        stat = conn.createStatement();
        stat.execute("create table test(id int, name varchar)");
        stat.execute("insert into test values (2, 'a'), (1, 'a')");
        stat.execute("create index on test(name)");
        ResultSet rs = stat.executeQuery(
                "select id from test where name = 'a' order by id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());
        conn.close();
        deleteDb("index");
    }

    private void testIndexTypes() throws SQLException {
        Connection conn = getConnection("index");
        stat = conn.createStatement();
        for (String type : new String[] { "unique", "hash", "unique hash" }) {
            stat.execute("create table test(id int)");
            stat.execute("create " + type + " index idx_name on test(id)");
            stat.execute("insert into test select x from system_range(1, 1000)");
            ResultSet rs = stat.executeQuery("select * from test where id=100");
            assertTrue(rs.next());
            assertFalse(rs.next());
            stat.execute("delete from test where id=100");
            rs = stat.executeQuery("select * from test where id=100");
            assertFalse(rs.next());
            rs = stat.executeQuery("select min(id), max(id) from test");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(1000, rs.getInt(2));
            stat.execute("drop table test");
        }
        conn.close();
        deleteDb("index");
    }

    private void testErrorMessage() throws SQLException {
        reconnect();
        stat.execute("create table test(id int primary key, name varchar)");
        testErrorMessage("PRIMARY", "KEY", " ON PUBLIC.TEST(ID)");
        stat.execute("create table test(id int, name varchar primary key)");
        testErrorMessage("PRIMARY_KEY_2 ON PUBLIC.TEST(NAME)");
        stat.execute("create table test(id int, name varchar, primary key(id, name))");
        testErrorMessage("PRIMARY_KEY_2 ON PUBLIC.TEST(ID, NAME)");
        stat.execute("create table test(id int, name varchar, primary key(name, id))");
        testErrorMessage("PRIMARY_KEY_2 ON PUBLIC.TEST(NAME, ID)");
        stat.execute("create table test(id int, name int primary key)");
        testErrorMessage("PRIMARY", "KEY", " ON PUBLIC.TEST(NAME)");
        stat.execute("create table test(id int, name int, unique(name))");
        testErrorMessage("CONSTRAINT_INDEX_2 ON PUBLIC.TEST(NAME)");
        stat.execute("create table test(id int, name int, " +
                "constraint abc unique(name, id))");
        testErrorMessage("ABC_INDEX_2 ON PUBLIC.TEST(NAME, ID)");
    }

    private void testErrorMessage(String... expected) throws SQLException {
        try {
            stat.execute("INSERT INTO TEST VALUES(1, 1)");
            stat.execute("INSERT INTO TEST VALUES(1, 1)");
            fail();
        } catch (SQLException e) {
            String m = e.getMessage();
            int start = m.indexOf('\"'), end = m.indexOf('\"', start + 1);
            String s = m.substring(start + 1, end);
            for (String t : expected) {
                assertContains(s, t);
            }
        }
        stat.execute("drop table test");
    }

    private void testDuplicateKeyException() throws SQLException {
        reconnect();
        stat.execute("create table test(id int primary key, name varchar(255))");
        stat.execute("create unique index idx_test_name on test(name)");
        stat.execute("insert into TEST values(1, 'Hello')");
        try {
            stat.execute("insert into TEST values(2, 'Hello')");
            fail();
        } catch (SQLException ex) {
            assertEquals(ErrorCode.DUPLICATE_KEY_1, ex.getErrorCode());
            String m = ex.getMessage();
            // The format of the VALUES clause varies a little depending on the
            // type of the index, so just test that we're getting useful info
            // back.
            assertContains(m, "IDX_TEST_NAME ON PUBLIC.TEST(NAME)");
            assertContains(m, "'Hello'");
        }
        stat.execute("drop table test");
    }

    private class ConcurrentUpdateThread extends Thread {
        private final AtomicInteger concurrentUpdateId, concurrentUpdateValue;

        private final PreparedStatement psInsert, psDelete;

        boolean haveDuplicateKeyException;

        ConcurrentUpdateThread(Connection c, AtomicInteger concurrentUpdateId,
                AtomicInteger concurrentUpdateValue) throws SQLException {
            this.concurrentUpdateId = concurrentUpdateId;
            this.concurrentUpdateValue = concurrentUpdateValue;
            psInsert = c.prepareStatement("insert into test(id, value) values (?, ?)");
            psDelete = c.prepareStatement("delete from test where value = ?");
        }

        @Override
        public void run() {
            for (int i = 0; i < 10000; i++) {
                try {
                    if (Math.random() > 0.05) {
                        psInsert.setInt(1, concurrentUpdateId.incrementAndGet());
                        psInsert.setInt(2, concurrentUpdateValue.get());
                        psInsert.executeUpdate();
                    } else {
                        psDelete.setInt(1, concurrentUpdateValue.get());
                        psDelete.executeUpdate();
                    }
                } catch (SQLException ex) {
                    switch (ex.getErrorCode()) {
                    case 23505:
                        haveDuplicateKeyException = true;
                        break;
                    case 90131:
                        // Unlikely but possible
                        break;
                    default:
                        ex.printStackTrace();
                    }
                }
                if (Math.random() > 0.95)
                    concurrentUpdateValue.incrementAndGet();
            }
        }
    }

    private void testConcurrentUpdate() throws SQLException {
        Connection c = getConnection("index");
        Statement stat = c.createStatement();
        stat.execute("create table test(id int primary key, value int)");
        stat.execute("create unique index idx_value_name on test(value)");
        PreparedStatement check = c.prepareStatement("select value from test");
        ConcurrentUpdateThread[] threads = new ConcurrentUpdateThread[4];
        AtomicInteger concurrentUpdateId = new AtomicInteger(), concurrentUpdateValue = new AtomicInteger();

        // The same connection
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new ConcurrentUpdateThread(c, concurrentUpdateId, concurrentUpdateValue);
        }
        testConcurrentUpdateRun(threads, check);
        // Different connections
        Connection[] connections = new Connection[threads.length];
        for (int i = 0; i < threads.length; i++) {
            Connection c2 = getConnection("index");
            connections[i] = c2;
            threads[i] = new ConcurrentUpdateThread(c2, concurrentUpdateId, concurrentUpdateValue);
        }
        testConcurrentUpdateRun(threads, check);
        for (Connection c2 : connections) {
            c2.close();
        }
        stat.execute("drop table test");
        c.close();
    }

    private void testConcurrentUpdateRun(ConcurrentUpdateThread[] threads, PreparedStatement check)
            throws SQLException {
        for (ConcurrentUpdateThread t : threads) {
            t.start();
        }
        boolean haveDuplicateKeyException = false;
        for (ConcurrentUpdateThread t : threads) {
            try {
                t.join();
                haveDuplicateKeyException |= t.haveDuplicateKeyException;
            } catch (InterruptedException e) {
            }
        }
        assertTrue("haveDuplicateKeys", haveDuplicateKeyException);
        HashSet<Integer> set = new HashSet<>();
        try (ResultSet rs = check.executeQuery()) {
            while (rs.next()) {
                if (!set.add(rs.getInt(1))) {
                    fail("unique index violation");
                }
            }
        }
    }

    private void testNonUniqueHashIndex() throws SQLException {
        reconnect();
        stat.execute("create memory table test(id bigint, data bigint)");
        stat.execute("create hash index on test(id)");
        Random rand = new Random(1);
        PreparedStatement prepInsert = conn.prepareStatement(
                "insert into test values(?, ?)");
        PreparedStatement prepDelete = conn.prepareStatement(
                "delete from test where id=?");
        PreparedStatement prepSelect = conn.prepareStatement(
                "select count(*) from test where id=?");
        HashMap<Long, Integer> map = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            long key = rand.nextInt(10) * 1000000000L;
            Integer r = map.get(key);
            int result = r == null ? 0 : (int) r;
            if (rand.nextBoolean()) {
                prepSelect.setLong(1, key);
                ResultSet rs = prepSelect.executeQuery();
                rs.next();
                assertEquals(result, rs.getInt(1));
            } else {
                if (rand.nextBoolean()) {
                    prepInsert.setLong(1, key);
                    prepInsert.setInt(2, rand.nextInt());
                    prepInsert.execute();
                    map.put(key, result + 1);
                } else {
                    prepDelete.setLong(1, key);
                    prepDelete.execute();
                    map.put(key, 0);
                }
            }
        }
        stat.execute("drop table test");
        conn.close();
    }

    private void testRenamePrimaryKey() throws SQLException {
        if (config.memory) {
            return;
        }
        reconnect();
        stat.execute("create table test(id int not null)");
        stat.execute("alter table test add constraint x primary key(id)");
        ResultSet rs;
        rs = conn.getMetaData().getIndexInfo(null, null, "TEST", true, false);
        rs.next();
        String old = rs.getString("INDEX_NAME");
        stat.execute("alter index " + old + " rename to y");
        rs = conn.getMetaData().getIndexInfo(null, null, "TEST", true, false);
        rs.next();
        assertEquals("Y", rs.getString("INDEX_NAME"));
        reconnect();
        rs = conn.getMetaData().getIndexInfo(null, null, "TEST", true, false);
        rs.next();
        assertEquals("Y", rs.getString("INDEX_NAME"));
        stat.execute("drop table test");
    }

    private void testRandomized() throws SQLException {
        boolean reopen = !config.memory;
        Random rand = new Random(1);
        reconnect();
        stat.execute("drop all objects");
        stat.execute("CREATE TABLE TEST(ID identity)");
        int len = getSize(100, 1000);
        for (int i = 0; i < len; i++) {
            switch (rand.nextInt(4)) {
            case 0:
                if (rand.nextInt(10) == 0) {
                    if (reopen) {
                        trace("reconnect");
                        reconnect();
                    }
                }
                break;
            case 1:
                trace("insert");
                stat.execute("insert into test(id) values(null)");
                break;
            case 2:
                trace("delete");
                stat.execute("delete from test");
                break;
            case 3:
                trace("insert 1-100");
                stat.execute("insert into test select null from system_range(1, 100)");
                break;
            }
        }
        stat.execute("drop table test");
    }

    private void testHashIndex() throws SQLException {
        reconnect();
        stat.execute("create table testA(id int primary key, name varchar)");
        stat.execute("create table testB(id int primary key hash, name varchar)");
        int len = getSize(300, 3000);
        stat.execute("insert into testA select x, 'Hello' from " +
                "system_range(1, " + len + ")");
        stat.execute("insert into testB select x, 'Hello' from " +
                "system_range(1, " + len + ")");
        Random rand = new Random(1);
        for (int i = 0; i < len; i++) {
            int x = rand.nextInt(len);
            String sql = "";
            switch (rand.nextInt(3)) {
            case 0:
                sql = "delete from testA where id = " + x;
                break;
            case 1:
                sql = "update testA set name = " + rand.nextInt(100) + " where id = " + x;
                break;
            case 2:
                sql = "select name from testA where id = " + x;
                break;
            default:
            }
            boolean result = stat.execute(sql);
            if (result) {
                ResultSet rs = stat.getResultSet();
                String s1 = rs.next() ? rs.getString(1) : null;
                rs = stat.executeQuery(sql.replace('A', 'B'));
                String s2 = rs.next() ? rs.getString(1) : null;
                assertEquals(s1, s2);
            } else {
                int count1 = stat.getUpdateCount();
                int count2 = stat.executeUpdate(sql.replace('A', 'B'));
                assertEquals(count1, count2);
            }
        }
        stat.execute("drop table testA, testB");
        conn.close();
    }

    private void reconnect() throws SQLException {
        if (conn != null) {
            conn.close();
            conn = null;
        }
        conn = getConnection("index");
        stat = conn.createStatement();
    }

    private void testDescIndex() throws SQLException {
        if (config.memory) {
            return;
        }
        ResultSet rs;
        reconnect();
        stat.execute("CREATE TABLE TEST(ID INT)");
        stat.execute("CREATE INDEX IDX_ND ON TEST(ID DESC)");
        rs = conn.getMetaData().getIndexInfo(null, null, "TEST", false, false);
        rs.next();
        assertEquals("D", rs.getString("ASC_OR_DESC"));
        assertEquals(SortOrder.DESCENDING, rs.getInt("SORT_TYPE"));
        stat.execute("INSERT INTO TEST SELECT X FROM SYSTEM_RANGE(1, 30)");
        rs = stat.executeQuery(
                "SELECT COUNT(*) FROM TEST WHERE ID BETWEEN 10 AND 20");
        rs.next();
        assertEquals(11, rs.getInt(1));
        reconnect();
        rs = conn.getMetaData().getIndexInfo(null, null, "TEST", false, false);
        rs.next();
        assertEquals("D", rs.getString("ASC_OR_DESC"));
        assertEquals(SortOrder.DESCENDING, rs.getInt("SORT_TYPE"));
        rs = stat.executeQuery(
                "SELECT COUNT(*) FROM TEST WHERE ID BETWEEN 10 AND 20");
        rs.next();
        assertEquals(11, rs.getInt(1));
        stat.execute("DROP TABLE TEST");

        stat.execute("create table test(x int, y int)");
        stat.execute("insert into test values(1, 1), (1, 2)");
        stat.execute("create index test_x_y on test (x desc, y desc)");
        rs = stat.executeQuery("select * from test where x=1 and y<2");
        assertTrue(rs.next());

        conn.close();
    }

    private String getRandomString(int len) {
        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < len; i++) {
            buff.append((char) ('a' + random.nextInt(26)));
        }
        return buff.toString();
    }

    private void testWideIndex(int length) throws SQLException {
        reconnect();
        stat.execute("drop all objects");
        stat.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR)");
        stat.execute("CREATE INDEX IDXNAME ON TEST(NAME)");
        for (int i = 0; i < 100; i++) {
            stat.execute("INSERT INTO TEST VALUES(" + i +
                    ", SPACE(" + length + ") || " + i + " )");
        }
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY NAME");
        while (rs.next()) {
            int id = rs.getInt("ID");
            String name = rs.getString("NAME");
            assertEquals("" + id, name.trim());
        }
        if (!config.memory) {
            reconnect();
            rs = stat.executeQuery("SELECT * FROM TEST ORDER BY NAME");
            while (rs.next()) {
                int id = rs.getInt("ID");
                String name = rs.getString("NAME");
                assertEquals("" + id, name.trim());
            }
        }
        stat.execute("drop all objects");
    }

    private void testLike() throws SQLException {
        reconnect();
        stat.execute("CREATE TABLE ABC(ID INT, NAME VARCHAR)");
        stat.execute("INSERT INTO ABC VALUES(1, 'Hello')");
        PreparedStatement prep = conn.prepareStatement(
                "SELECT * FROM ABC WHERE NAME LIKE CAST(? AS VARCHAR)");
        prep.setString(1, "Hi%");
        prep.execute();
        stat.execute("DROP TABLE ABC");
    }

    private void testConstraint() throws SQLException {
        if (config.memory) {
            return;
        }
        stat.execute("CREATE TABLE PARENT(ID INT PRIMARY KEY)");
        stat.execute("CREATE TABLE CHILD(ID INT PRIMARY KEY, " +
                "PID INT, FOREIGN KEY(PID) REFERENCES PARENT(ID))");
        reconnect();
        stat.execute("DROP TABLE PARENT");
        stat.execute("DROP TABLE CHILD");
    }

    private void testLargeIndex() throws SQLException {
        random.setSeed(10);
        for (int i = 1; i < 100; i += getSize(1000, 7)) {
            stat.execute("DROP TABLE IF EXISTS TEST");
            stat.execute("CREATE TABLE TEST(NAME VARCHAR(" + i + "))");
            stat.execute("CREATE INDEX IDXNAME ON TEST(NAME)");
            PreparedStatement prep = conn.prepareStatement(
                    "INSERT INTO TEST VALUES(?)");
            for (int j = 0; j < getSize(2, 5); j++) {
                prep.setString(1, getRandomString(i));
                prep.execute();
            }
            if (!config.memory) {
                conn.close();
                conn = getConnection("index");
                stat = conn.createStatement();
            }
            ResultSet rs = stat.executeQuery(
                    "SELECT COUNT(*) FROM TEST WHERE NAME > 'mdd'");
            rs.next();
            int count = rs.getInt(1);
            trace(i + " count=" + count);
        }

        stat.execute("DROP TABLE IF EXISTS TEST");
    }

    private void testHashIndex(boolean primaryKey, boolean hash)
            throws SQLException {
        if (config.memory) {
            return;
        }

        reconnect();

        stat.execute("DROP TABLE IF EXISTS TEST");
        if (primaryKey) {
            stat.execute("CREATE TABLE TEST(A INT PRIMARY KEY " +
                    (hash ? "HASH" : "") + ", B INT)");
        } else {
            stat.execute("CREATE TABLE TEST(A INT, B INT)");
            stat.execute("CREATE UNIQUE " + (hash ? "HASH" : "") + " INDEX ON TEST(A)");
        }
        PreparedStatement prep;
        prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
        int len = getSize(5, 1000);
        for (int a = 0; a < len; a++) {
            prep.setInt(1, a);
            prep.setInt(2, a);
            prep.execute();
            assertEquals(1,
                    getValue("SELECT COUNT(*) FROM TEST WHERE A=" + a));
            assertEquals(0,
                    getValue("SELECT COUNT(*) FROM TEST WHERE A=-1-" + a));
        }

        reconnect();

        prep = conn.prepareStatement("DELETE FROM TEST WHERE A=?");
        for (int a = 0; a < len; a++) {
            if (getValue("SELECT COUNT(*) FROM TEST WHERE A=" + a) != 1) {
                assertEquals(1,
                        getValue("SELECT COUNT(*) FROM TEST WHERE A=" + a));
            }
            prep.setInt(1, a);
            assertEquals(1, prep.executeUpdate());
        }
        assertEquals(0, getValue("SELECT COUNT(*) FROM TEST"));
    }

    private void testMultiColumnIndex() throws SQLException {
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(A INT, B INT)");
        PreparedStatement prep;
        prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
        int len = getSize(3, 260);
        for (int a = 0; a < len; a++) {
            prep.setInt(1, a);
            prep.setInt(2, a);
            prep.execute();
        }
        stat.execute("INSERT INTO TEST SELECT A, B FROM TEST");
        stat.execute("CREATE INDEX ON TEST(A, B)");
        prep = conn.prepareStatement("DELETE FROM TEST WHERE A=?");
        for (int a = 0; a < len; a++) {
            log("SELECT * FROM TEST");
            assertEquals(2,
                    getValue("SELECT COUNT(*) FROM TEST WHERE A=" + (len - a - 1)));
            assertEquals((len - a) * 2, getValue("SELECT COUNT(*) FROM TEST"));
            prep.setInt(1, len - a - 1);
            prep.execute();
        }
        assertEquals(0, getValue("SELECT COUNT(*) FROM TEST"));
    }

    private void testMultiColumnHashIndex() throws SQLException {
        if (config.memory) {
            return;
        }

        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(A INT, B INT, DATA VARCHAR(255))");
        stat.execute("CREATE UNIQUE HASH INDEX IDX_AB ON TEST(A, B)");
        PreparedStatement prep;
        prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?, ?)");
        // speed is quadratic (len*len)
        int len = getSize(2, 14);
        for (int a = 0; a < len; a++) {
            for (int b = 0; b < len; b += 2) {
                prep.setInt(1, a);
                prep.setInt(2, b);
                prep.setString(3, "i(" + a + "," + b + ")");
                prep.execute();
            }
        }

        reconnect();

        prep = conn.prepareStatement(
                "UPDATE TEST SET DATA=DATA||? WHERE A=? AND B=?");
        for (int a = 0; a < len; a++) {
            for (int b = 0; b < len; b += 2) {
                prep.setString(1, "u(" + a + "," + b + ")");
                prep.setInt(2, a);
                prep.setInt(3, b);
                prep.execute();
            }
        }

        reconnect();

        ResultSet rs = stat.executeQuery(
                "SELECT * FROM TEST WHERE DATA <> 'i('||a||','||b||')u('||a||','||b||')'");
        assertFalse(rs.next());
        assertEquals(len * (len / 2), getValue("SELECT COUNT(*) FROM TEST"));
        stat.execute("DROP TABLE TEST");
    }

    private void testHashIndexOnMemoryTable() throws SQLException {
        reconnect();
        stat.execute("drop table if exists hash_index_test");
        stat.execute("create memory table hash_index_test as " +
                "select x as id, x % 10 as data from (select *  from system_range(1, 100))");
        stat.execute("create hash index idx2 on hash_index_test(data)");
        assertEquals(10,
                getValue("select count(*) from hash_index_test where data = 1"));

        stat.execute("drop index idx2");
        stat.execute("create unique hash index idx2 on hash_index_test(id)");
        assertEquals(1,
                getValue("select count(*) from hash_index_test where id = 1"));
    }

    private int getValue(String sql) throws SQLException {
        ResultSet rs = stat.executeQuery(sql);
        rs.next();
        return rs.getInt(1);
    }

    private void log(String sql) throws SQLException {
        trace(sql);
        ResultSet rs = stat.executeQuery(sql);
        int cols = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            StringBuilder buff = new StringBuilder();
            for (int i = 0; i < cols; i++) {
                if (i > 0) {
                    buff.append(", ");
                }
                buff.append("[" + i + "]=" + rs.getString(i + 1));
            }
            trace(buff.toString());
        }
        trace("---done---");
    }

    /**
     * This method is called from the database.
     *
     * @return the result set
     */
    public static ResultSet testFunctionIndexFunction() {
        // There are additional callers like JdbcConnection.prepareCommand() and
        // CommandContainer.recompileIfRequired()
        for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
            if (element.getClassName().startsWith(Select.class.getName())) {
                testFunctionIndexCounter++;
                break;
            }
        }
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("ID", Types.INTEGER, ValueInt.PRECISION, 0);
        rs.addColumn("VALUE", Types.INTEGER, ValueInt.PRECISION, 0);
        rs.addRow(1, 10);
        rs.addRow(2, 20);
        rs.addRow(3, 30);
        return rs;
    }

    private void testFunctionIndex() throws SQLException {
        testFunctionIndexCounter = 0;
        stat.execute("CREATE ALIAS TEST_INDEX FOR \"" + TestIndex.class.getName() + ".testFunctionIndexFunction\"");
        try (ResultSet rs = stat.executeQuery("SELECT * FROM TEST_INDEX() WHERE ID = 1 OR ID = 3")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(10, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertEquals(30, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            stat.execute("DROP ALIAS TEST_INDEX");
        }
        assertEquals(1, testFunctionIndexCounter);
    }

}
