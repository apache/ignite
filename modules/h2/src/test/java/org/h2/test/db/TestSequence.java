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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.h2.api.Trigger;
import org.h2.test.TestBase;
import org.h2.util.Task;

/**
 * Tests the sequence feature of this database.
 */
public class TestSequence extends TestBase {

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
        testConcurrentCreate();
        testSchemaSearchPath();
        testAlterSequenceColumn();
        testAlterSequence();
        testCache();
        testTwo();
        testMetaTable();
        testCreateWithMinValue();
        testCreateWithMaxValue();
        testCreationErrors();
        testCreateSql();
        testDefaultMinMax();
        deleteDb("sequence");
    }

    private void testConcurrentCreate() throws Exception {
        deleteDb("sequence");
        final String url = getURL("sequence;MULTI_THREADED=1;LOCK_TIMEOUT=2000", true);
        Connection conn = getConnection(url);
        Task[] tasks = new Task[2];
        try {
            Statement stat = conn.createStatement();
            stat.execute("create table dummy(id bigint primary key)");
            stat.execute("create table test(id bigint primary key)");
            stat.execute("create sequence test_seq cache 2");
            for (int i = 0; i < tasks.length; i++) {
                final int x = i;
                tasks[i] = new Task() {
                    @Override
                    public void call() throws Exception {
                        try (Connection conn = getConnection(url)) {
                            PreparedStatement prep = conn.prepareStatement(
                                    "insert into test(id) values(next value for test_seq)");
                            PreparedStatement prep2 = conn.prepareStatement(
                                    "delete from test");
                            while (!stop) {
                                prep.execute();
                                if (Math.random() < 0.01) {
                                    prep2.execute();
                                }
                                if (Math.random() < 0.01) {
                                    createDropTrigger(conn);
                                }
                            }
                        }
                    }

                    private void createDropTrigger(Connection conn) throws Exception {
                        String triggerName = "t_" + x;
                        Statement stat = conn.createStatement();
                        stat.execute("create trigger " + triggerName +
                                " before insert on dummy call \"" +
                                TriggerTest.class.getName() + "\"");
                        stat.execute("drop trigger " + triggerName);
                    }

                }.execute();
            }
            Thread.sleep(1000);
            for (Task t : tasks) {
                t.get();
            }
        } finally {
            for (Task t : tasks) {
                t.join();
            }
            conn.close();
        }
    }

    private void testSchemaSearchPath() throws SQLException {
        deleteDb("sequence");
        Connection conn = getConnection("sequence");
        Statement stat = conn.createStatement();
        stat.execute("CREATE SCHEMA TEST");
        stat.execute("CREATE SEQUENCE TEST.TEST_SEQ");
        stat.execute("SET SCHEMA_SEARCH_PATH PUBLIC, TEST");
        stat.execute("CALL TEST_SEQ.NEXTVAL");
        stat.execute("CALL TEST_SEQ.CURRVAL");
        conn.close();
    }

    private void testAlterSequenceColumn() throws SQLException {
        deleteDb("sequence");
        Connection conn = getConnection("sequence");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT , NAME VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        stat.execute("ALTER TABLE TEST ALTER COLUMN ID INT IDENTITY");
        stat.execute("ALTER TABLE test ALTER COLUMN ID RESTART WITH 3");
        stat.execute("INSERT INTO TEST (name) VALUES('Other World')");
        conn.close();
    }

    private void testAlterSequence() throws SQLException {
        test("create sequence s; alter sequence s restart with 2", null, 2, 3, 4);
        test("create sequence s; alter sequence s restart with 7", null, 7, 8, 9, 10);
        test("create sequence s; alter sequence s restart with 11 " +
        "minvalue 3 maxvalue 12 cycle", null, 11, 12, 3, 4);
        test("create sequence s; alter sequence s restart with 5 cache 2",
                null, 5, 6, 7, 8);
        test("create sequence s; alter sequence s restart with 9 " +
                "maxvalue 12 nocycle nocache",
                "Sequence \"S\" has run out of numbers", 9, 10, 11, 12);
    }

    private void testCache() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("sequence");
        Connection conn = getConnection("sequence");
        Statement stat = conn.createStatement();
        stat.execute("create sequence test_Sequence");
        stat.execute("create sequence test_Sequence3 cache 3");
        conn.close();
        conn = getConnection("sequence");
        stat = conn.createStatement();
        stat.execute("call next value for test_Sequence");
        stat.execute("call next value for test_Sequence3");
        ResultSet rs = stat.executeQuery("select * from " +
                "information_schema.sequences order by sequence_name");
        rs.next();
        assertEquals("TEST_SEQUENCE", rs.getString("SEQUENCE_NAME"));
        assertEquals("32", rs.getString("CACHE"));
        rs.next();
        assertEquals("TEST_SEQUENCE3", rs.getString("SEQUENCE_NAME"));
        assertEquals("3", rs.getString("CACHE"));
        assertFalse(rs.next());
        conn.close();
    }

    private void testMetaTable() throws SQLException {
        deleteDb("sequence");
        Connection conn = getConnection("sequence");
        Statement stat = conn.createStatement();
        stat.execute("create sequence a");
        stat.execute("create sequence b start with 7 minvalue 5 " +
                "maxvalue 9 cycle increment by 2 nocache");
        stat.execute("create sequence c start with -4 minvalue -9 " +
                "maxvalue -3 no cycle increment by -2 cache 3");

        if (!config.memory) {
            conn.close();
            conn = getConnection("sequence");
        }

        stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select * from " +
                "information_schema.sequences order by sequence_name");
        rs.next();
        assertEquals("SEQUENCE", rs.getString("SEQUENCE_CATALOG"));
        assertEquals("PUBLIC", rs.getString("SEQUENCE_SCHEMA"));
        assertEquals("A", rs.getString("SEQUENCE_NAME"));
        assertEquals(0, rs.getLong("CURRENT_VALUE"));
        assertEquals(1, rs.getLong("INCREMENT"));
        assertEquals(false, rs.getBoolean("IS_GENERATED"));
        assertEquals("", rs.getString("REMARKS"));
        assertEquals(32, rs.getLong("CACHE"));
        assertEquals(1, rs.getLong("MIN_VALUE"));
        assertEquals(Long.MAX_VALUE, rs.getLong("MAX_VALUE"));
        assertEquals(false, rs.getBoolean("IS_CYCLE"));
        rs.next();
        assertEquals("SEQUENCE", rs.getString("SEQUENCE_CATALOG"));
        assertEquals("PUBLIC", rs.getString("SEQUENCE_SCHEMA"));
        assertEquals("B", rs.getString("SEQUENCE_NAME"));
        assertEquals(5, rs.getLong("CURRENT_VALUE"));
        assertEquals(2, rs.getLong("INCREMENT"));
        assertEquals(false, rs.getBoolean("IS_GENERATED"));
        assertEquals("", rs.getString("REMARKS"));
        assertEquals(1, rs.getLong("CACHE"));
        assertEquals(5, rs.getLong("MIN_VALUE"));
        assertEquals(9, rs.getLong("MAX_VALUE"));
        assertEquals(true, rs.getBoolean("IS_CYCLE"));
        rs.next();
        assertEquals("SEQUENCE", rs.getString("SEQUENCE_CATALOG"));
        assertEquals("PUBLIC", rs.getString("SEQUENCE_SCHEMA"));
        assertEquals("C", rs.getString("SEQUENCE_NAME"));
        assertEquals(-2, rs.getLong("CURRENT_VALUE"));
        assertEquals(-2, rs.getLong("INCREMENT"));
        assertEquals(false, rs.getBoolean("IS_GENERATED"));
        assertEquals("", rs.getString("REMARKS"));
        assertEquals(3, rs.getLong("CACHE"));
        assertEquals(-9, rs.getLong("MIN_VALUE"));
        assertEquals(-3, rs.getLong("MAX_VALUE"));
        assertEquals(false, rs.getBoolean("IS_CYCLE"));
        assertFalse(rs.next());
        conn.close();
    }

    private void testCreateWithMinValue() throws SQLException {
        test("create sequence s minvalue 3", null, 3, 4, 5, 6);
        test("create sequence s minvalue -3 increment by -1 cycle",
                null, -1, -2, -3, -1);
        test("create sequence s minvalue -3 increment by -1",
                "Sequence \"S\" has run out of numbers", -1, -2, -3);
        test("create sequence s minvalue -3 increment by -1 nocycle",
                "Sequence \"S\" has run out of numbers", -1, -2, -3);
        test("create sequence s minvalue -3 increment by -1 no cycle",
                "Sequence \"S\" has run out of numbers", -1, -2, -3);
        test("create sequence s minvalue -3 increment by -1 nocache cycle",
                null, -1, -2, -3, -1);
        test("create sequence s minvalue -3 increment by -1 nocache",
                "Sequence \"S\" has run out of numbers", -1, -2, -3);
        test("create sequence s minvalue -3 increment by -1 nocache nocycle",
                "Sequence \"S\" has run out of numbers", -1, -2, -3);
        test("create sequence s minvalue -3 increment by -1 no cache no cycle",
                "Sequence \"S\" has run out of numbers", -1, -2, -3);
    }

    private void testCreateWithMaxValue() throws SQLException {
        test("create sequence s maxvalue -3 increment by -1",
                null, -3, -4, -5, -6);
        test("create sequence s maxvalue 3 cycle", null, 1, 2, 3, 1);
        test("create sequence s maxvalue 3",
                "Sequence \"S\" has run out of numbers", 1, 2, 3);
        test("create sequence s maxvalue 3 nocycle",
                "Sequence \"S\" has run out of numbers", 1, 2, 3);
        test("create sequence s maxvalue 3 no cycle",
                "Sequence \"S\" has run out of numbers", 1, 2, 3);
        test("create sequence s maxvalue 3 nocache cycle",
                null, 1, 2, 3, 1);
        test("create sequence s maxvalue 3 nocache",
                "Sequence \"S\" has run out of numbers", 1, 2, 3);
        test("create sequence s maxvalue 3 nocache nocycle",
                "Sequence \"S\" has run out of numbers", 1, 2, 3);
        test("create sequence s maxvalue 3 no cache no cycle",
                "Sequence \"S\" has run out of numbers", 1, 2, 3);
    }

    private void testCreationErrors() throws SQLException {
        deleteDb("sequence");
        Connection conn = getConnection("sequence");
        Statement stat = conn.createStatement();
        expectError(
                stat,
                "create sequence a minvalue 5 start with 2",
                "Unable to create or alter sequence \"A\" because of " +
                "invalid attributes (start value \"2\", " +
                "min value \"5\", max value \"" + Long.MAX_VALUE +
                "\", increment \"1\")");
        expectError(
                stat,
                "create sequence b maxvalue 5 start with 7",
                "Unable to create or alter sequence \"B\" because of " +
                "invalid attributes (start value \"7\", " +
                        "min value \"1\", max value \"5\", increment \"1\")");
        expectError(
                stat,
                "create sequence c minvalue 5 maxvalue 2",
                "Unable to create or alter sequence \"C\" because of " +
                "invalid attributes (start value \"5\", " +
                "min value \"5\", max value \"2\", increment \"1\")");
        expectError(
                stat,
                "create sequence d increment by 0",
                "Unable to create or alter sequence \"D\" because of " +
                "invalid attributes (start value \"1\", " +
                "min value \"1\", max value \"" +
                Long.MAX_VALUE + "\", increment \"0\")");
        expectError(stat,
                "create sequence e minvalue 1 maxvalue 5 increment 99",
                "Unable to create or alter sequence \"E\" because of " +
                "invalid attributes (start value \"1\", " +
                "min value \"1\", max value \"5\", increment \"99\")");
        conn.close();
    }

    private void testCreateSql() throws SQLException {
        deleteDb("sequence");
        Connection conn = getConnection("sequence");
        Statement stat = conn.createStatement();
        stat.execute("create sequence a");
        stat.execute("create sequence b start with 5 increment by 2 " +
                "minvalue 3 maxvalue 7 cycle nocache");
        stat.execute("create sequence c start with 3 increment by 1 " +
                "minvalue 2 maxvalue 9 nocycle cache 2");
        stat.execute("create sequence d nomaxvalue no minvalue no cache nocycle");
        stat.execute("create sequence e cache 1");
        List<String> script = new ArrayList<>();
        ResultSet rs = stat.executeQuery("script nodata");
        while (rs.next()) {
            script.add(rs.getString(1));
        }
        Collections.sort(script);
        assertEquals("CREATE SEQUENCE PUBLIC.A START WITH 1;", script.get(0));
        assertEquals("CREATE SEQUENCE PUBLIC.B START " +
                "WITH 5 INCREMENT BY 2 " +
                "MINVALUE 3 MAXVALUE 7 CYCLE CACHE 1;", script.get(1));
        assertEquals("CREATE SEQUENCE PUBLIC.C START " +
                "WITH 3 MINVALUE 2 MAXVALUE 9 CACHE 2;",
                script.get(2));
        assertEquals("CREATE SEQUENCE PUBLIC.D START " +
                "WITH 1 CACHE 1;", script.get(3));
        assertEquals("CREATE SEQUENCE PUBLIC.E START " +
                "WITH 1 CACHE 1;", script.get(4));
        conn.close();
    }

    private void testDefaultMinMax() throws SQLException {
        // test that we calculate default MIN and MAX values correctly
        deleteDb("sequence");
        Connection conn = getConnection("sequence");
        Statement stat = conn.createStatement();
        stat.execute("create sequence a START WITH -7320917853639540658");
        stat.execute("create sequence b START WITH 7320917853639540658 INCREMENT -1");
        conn.close();
    }

    private void testTwo() throws SQLException {
        deleteDb("sequence");
        Connection conn = getConnection("sequence");
        Statement stat = conn.createStatement();
        stat.execute("create sequence s");
        conn.setAutoCommit(false);

        Connection conn2 = getConnection("sequence");
        Statement stat2 = conn2.createStatement();
        conn2.setAutoCommit(false);

        long last = 0;
        for (int i = 0; i < 100; i++) {
            long v1 = getNext(stat);
            assertTrue(v1 > last);
            last = v1;
            for (int j = 0; j < 100; j++) {
                long v2 = getNext(stat2);
                assertTrue(v2 > last);
                last = v2;
            }
        }

        conn2.close();
        conn.close();
    }

    private void test(String setupSql, String finalError, long... values)
            throws SQLException {

        deleteDb("sequence");

        Connection conn = getConnection("sequence");
        Statement stat = conn.createStatement();
        stat.execute(setupSql);

        if (!config.memory) {
            conn.close();
            conn = getConnection("sequence");
        }

        stat = conn.createStatement();
        for (long value : values) {
            assertEquals(value, getNext(stat));
        }

        if (finalError != null) {
            try {
                getNext(stat);
                fail("Expected error: " + finalError);
            } catch (SQLException e) {
                assertContains(e.getMessage(), finalError);
            }
        }

        conn.close();
    }

    private void expectError(Statement stat, String sql, String error) {
        try {
            stat.execute(sql);
            fail("Expected error: " + error);
        } catch (SQLException e) {
            assertContains(e.getMessage(), error);
        }
    }

    private static long getNext(Statement stat) throws SQLException {
        ResultSet rs = stat.executeQuery("call next value for s");
        rs.next();
        long value = rs.getLong(1);
        return value;
    }

    /**
     * A test trigger.
     */
    public static class TriggerTest implements Trigger {

        @Override
        public void init(Connection conn, String schemaName,
                String triggerName, String tableName, boolean before, int type)
                throws SQLException {
            conn.createStatement().executeQuery("call next value for test_seq");
        }

        @Override
        public void fire(Connection conn, Object[] oldRow, Object[] newRow)
                throws SQLException {
            // ignore
        }

        @Override
        public void close() throws SQLException {
            // ignore
        }

        @Override
        public void remove() throws SQLException {
            // ignore
        }

    }

}
