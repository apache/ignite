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
import java.util.Arrays;
import java.util.HashSet;

import org.h2.api.ErrorCode;
import org.h2.api.Trigger;
import org.h2.engine.Session;
import org.h2.jdbc.JdbcConnection;
import org.h2.test.TestBase;
import org.h2.tools.TriggerAdapter;
import org.h2.util.Task;
import org.h2.value.ValueLong;

/**
 * Tests for trigger and constraints.
 */
public class TestTriggersConstraints extends TestBase implements Trigger {

    private static boolean mustNotCallTrigger;
    private String triggerName;

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
        deleteDb("trigger");
        testTriggerDeadlock();
        testDeleteInTrigger();
        testTriggerAdapter();
        testTriggerSelectEachRow();
        testViewTrigger();
        testViewTriggerGeneratedKeys();
        testTriggerBeforeSelect();
        testTriggerAlterTable();
        testTriggerAsSource();
        testTriggerAsJavascript();
        testTriggers();
        testConstraints();
        testCheckConstraintErrorMessage();
        testMultiPartForeignKeys();
        deleteDb("trigger");
    }

    /**
     * A trigger that deletes all rows in the test table.
     */
    public static class DeleteTrigger extends TriggerAdapter {
        @Override
        public void fire(Connection conn, ResultSet oldRow, ResultSet newRow)
                throws SQLException {
            conn.createStatement().execute("delete from test");
        }
    }

    private void testTriggerDeadlock() throws Exception {
        final Connection conn, conn2;
        final Statement stat, stat2;
        conn = getConnection("trigger");
        conn2 = getConnection("trigger");
        stat = conn.createStatement();
        stat2 = conn2.createStatement();
        stat.execute("create table test(id int) as select 1");
        stat.execute("create table test2(id int) as select 1");
        stat.execute("create trigger test_u before update on test2 " +
                "for each row call \"" + DeleteTrigger.class.getName() + "\"");
        conn.setAutoCommit(false);
        conn2.setAutoCommit(false);
        stat2.execute("update test set id = 2");
        Task task = new Task() {
            @Override
            public void call() throws Exception {
                Thread.sleep(300);
                stat2.execute("update test2 set id = 4");
            }
        };
        task.execute();
        Thread.sleep(100);
        try {
            stat.execute("update test2 set id = 3");
            task.get();
        } catch (SQLException e) {
            assertEquals(ErrorCode.LOCK_TIMEOUT_1, e.getErrorCode());
        }
        conn2.rollback();
        conn.rollback();
        stat.execute("drop table test");
        stat.execute("drop table test2");
        conn.close();
        conn2.close();
    }

    private void testDeleteInTrigger() throws SQLException {
        if (config.mvcc || config.mvStore) {
            return;
        }
        Connection conn;
        Statement stat;
        conn = getConnection("trigger");
        stat = conn.createStatement();
        stat.execute("create table test(id int) as select 1");
        stat.execute("create trigger test_u before update on test " +
                "for each row call \"" + DeleteTrigger.class.getName() + "\"");
        // this threw a NullPointerException
        assertThrows(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1, stat).
                execute("update test set id = 2");
        stat.execute("drop table test");
        conn.close();
    }

    private void testTriggerAdapter() throws SQLException {
        Connection conn;
        Statement stat;
        conn = getConnection("trigger");
        stat = conn.createStatement();
        stat.execute("drop table if exists test");
        stat.execute("create table test(id int, c clob, b blob)");
        stat.execute("create table message(name varchar)");
        stat.execute(
                "create trigger test_insert before insert, update, delete on test " +
                "for each row call \"" + TestTriggerAdapter.class.getName() + "\"");
        stat.execute("insert into test values(1, 'hello', 'abcd')");
        ResultSet rs;
        rs = stat.executeQuery("select * from test");
        rs.next();
        assertEquals(10, rs.getInt(1));
        stat.execute("update test set id = 2");
        rs = stat.executeQuery("select * from test");
        rs.next();
        assertEquals(20, rs.getInt(1));
        stat.execute("delete from test");
        rs = stat.executeQuery("select * from message");
        assertTrue(rs.next());
        assertEquals("+1;", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("-10;+2;", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("-20;", rs.getString(1));
        assertFalse(rs.next());
        stat.execute("drop table test, message");
        conn.close();
    }

    private void testTriggerSelectEachRow() throws SQLException {
        Connection conn;
        Statement stat;
        conn = getConnection("trigger");
        stat = conn.createStatement();
        stat.execute("drop table if exists test");
        stat.execute("create table test(id int)");
        assertThrows(ErrorCode.TRIGGER_SELECT_AND_ROW_BASED_NOT_SUPPORTED, stat)
                .execute("create trigger test_insert before select on test " +
                    "for each row call \"" + TestTriggerAdapter.class.getName() + "\"");
        conn.close();
    }

    private void testViewTrigger() throws SQLException {
        Connection conn;
        Statement stat;
        conn = getConnection("trigger");
        stat = conn.createStatement();
        stat.execute("drop table if exists test");
        stat.execute("create table test(id int)");
        stat.execute("create view test_view as select * from test");
        stat.execute("create trigger test_view_insert " +
                "instead of insert on test_view for each row call \"" +
                TestView.class.getName() + "\"");
        stat.execute("create trigger test_view_delete " +
                "instead of delete on test_view for each row call \"" +
                TestView.class.getName() + "\"");
        if (!config.memory) {
            conn.close();
            conn = getConnection("trigger");
            stat = conn.createStatement();
        }
        int count = stat.executeUpdate("insert into test_view values(1)");
        assertEquals(1, count);
        ResultSet rs;
        rs = stat.executeQuery("select * from test");
        assertTrue(rs.next());
        assertFalse(rs.next());
        count = stat.executeUpdate("delete from test_view");
        assertEquals(1, count);
        stat.execute("drop view test_view");
        stat.execute("drop table test");
        conn.close();
    }

    private void testViewTriggerGeneratedKeys() throws SQLException {
        Connection conn;
        Statement stat;
        conn = getConnection("trigger");
        stat = conn.createStatement();
        stat.execute("drop table if exists test");
        stat.execute("create table test(id int identity)");
        stat.execute("create view test_view as select * from test");
        stat.execute("create trigger test_view_insert " +
                "instead of insert on test_view for each row call \"" +
                TestViewGeneratedKeys.class.getName() + "\"");
        if (!config.memory) {
            conn.close();
            conn = getConnection("trigger");
            stat = conn.createStatement();
        }

        PreparedStatement pstat;
        pstat = conn.prepareStatement(
                "insert into test_view values()", Statement.RETURN_GENERATED_KEYS);
        int count = pstat.executeUpdate();
        assertEquals(1, count);

        ResultSet gkRs;
        gkRs = stat.executeQuery("select scope_identity()");

        assertTrue(gkRs.next());
        assertEquals(1, gkRs.getInt(1));
        assertFalse(gkRs.next());

        ResultSet rs;
        rs = stat.executeQuery("select * from test");
        assertTrue(rs.next());
        assertFalse(rs.next());
        stat.execute("drop view test_view");
        stat.execute("drop table test");
        conn.close();
    }

    /**
     * A test trigger adapter implementation.
     */
    public static class TestTriggerAdapter extends TriggerAdapter {

        @Override
        public void fire(Connection conn, ResultSet oldRow, ResultSet newRow)
                throws SQLException {
            StringBuilder buff = new StringBuilder();
            if (oldRow != null) {
                buff.append("-").append(oldRow.getString("id")).append(';');
            }
            if (newRow != null) {
                buff.append("+").append(newRow.getString("id")).append(';');
            }
            if (!"TEST_INSERT".equals(triggerName)) {
                throw new RuntimeException("Wrong trigger name: " + triggerName);
            }
            if (!"TEST".equals(tableName)) {
                throw new RuntimeException("Wrong table name: " + tableName);
            }
            if (!"PUBLIC".equals(schemaName)) {
                throw new RuntimeException("Wrong schema name: " + schemaName);
            }
            if (type != (Trigger.INSERT | Trigger.UPDATE | Trigger.DELETE)) {
                throw new RuntimeException("Wrong type: " + type);
            }
            if (newRow != null) {
                if (oldRow == null) {
                    if (newRow.getInt(1) != 1) {
                        throw new RuntimeException("Expected: 1 got: " +
                                newRow.getString(1));
                    }
                } else {
                    if (newRow.getInt(1) != 2) {
                        throw new RuntimeException("Expected: 2 got: " +
                                newRow.getString(1));
                    }
                }
                newRow.getCharacterStream(2);
                newRow.getBinaryStream(3);
                newRow.updateInt(1, newRow.getInt(1) * 10);
            }
            conn.createStatement().execute("insert into message values('" +
                    buff.toString() + "')");
        }

    }

    /**
     * A test trigger implementation.
     */
    public static class TestView implements Trigger {

        PreparedStatement prepInsert;

        @Override
        public void init(Connection conn, String schemaName,
                String triggerName, String tableName, boolean before, int type)
                throws SQLException {
            prepInsert = conn.prepareStatement("insert into test values(?)");
        }

        @Override
        public void fire(Connection conn, Object[] oldRow, Object[] newRow)
                throws SQLException {
            if (newRow != null) {
                prepInsert.setInt(1, (Integer) newRow[0]);
                prepInsert.execute();
            }
        }

        @Override
        public void close() {
            // ignore
        }

        @Override
        public void remove() {
            // ignore
        }

    }

    /**
     *
     */
    public static class TestViewGeneratedKeys implements Trigger {

        PreparedStatement prepInsert;

        @Override
        public void init(Connection conn, String schemaName,
                String triggerName, String tableName, boolean before, int type)
                throws SQLException {
            prepInsert = conn.prepareStatement(
                    "insert into test values()", Statement.RETURN_GENERATED_KEYS);
        }

        @Override
        public void fire(Connection conn, Object[] oldRow, Object[] newRow)
                throws SQLException {
            if (newRow != null) {
                prepInsert.execute();
                ResultSet rs = prepInsert.getGeneratedKeys();
                if (rs.next()) {
                    JdbcConnection jconn = (JdbcConnection) conn;
                    Session session = (Session) jconn.getSession();
                    session.setLastTriggerIdentity(ValueLong.get(rs.getLong(1)));
                }
            }
        }

        @Override
        public void close() {
            // ignore
        }

        @Override
        public void remove() {
            // ignore
        }

    }

    private void testTriggerBeforeSelect() throws SQLException {
        Connection conn;
        Statement stat;
        conn = getConnection("trigger");
        stat = conn.createStatement();
        stat.execute("drop table if exists meta_tables");
        stat.execute("create table meta_tables(name varchar)");
        stat.execute("create trigger meta_tables_select " +
                "before select on meta_tables call \"" + TestSelect.class.getName() + "\"");
        ResultSet rs;
        rs = stat.executeQuery("select * from meta_tables");
        assertTrue(rs.next());
        assertFalse(rs.next());
        stat.execute("create table test(id int)");
        rs = stat.executeQuery("select * from meta_tables");
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());
        conn.close();
        if (!config.memory) {
            conn = getConnection("trigger");
            stat = conn.createStatement();
            stat.execute("create table test2(id int)");
            rs = stat.executeQuery("select * from meta_tables");
            assertTrue(rs.next());
            assertTrue(rs.next());
            assertTrue(rs.next());
            assertFalse(rs.next());
            conn.close();
        }
    }

    /**
     * A test trigger implementation.
     */
    public static class TestSelect implements Trigger {

        PreparedStatement prepMeta;

        @Override
        public void init(Connection conn, String schemaName,
                String triggerName, String tableName, boolean before, int type)
                throws SQLException {
            prepMeta = conn.prepareStatement("insert into meta_tables " +
                    "select table_name from information_schema.tables " +
                    "where table_schema='PUBLIC'");
        }

        @Override
        public void fire(Connection conn, Object[] oldRow, Object[] newRow)
                throws SQLException {
            if (oldRow != null || newRow != null) {
                throw new SQLException("old and new must be null");
            }
            conn.createStatement().execute("delete from meta_tables");
            prepMeta.execute();
        }

        @Override
        public void close() {
            // ignore
        }

        @Override
        public void remove() {
            // ignore
        }

    }

    /**
     * A test trigger implementation.
     */
    public static class TestTriggerAlterTable implements Trigger {

        @Override
        public void fire(Connection conn, Object[] oldRow, Object[] newRow)
                throws SQLException {
            conn.createStatement().execute("call seq.nextval");
        }

        @Override
        public void init(Connection conn, String schemaName,
                String triggerName, String tableName, boolean before, int type) {
            // nothing to do
        }

        @Override
        public void close() {
            // ignore
        }

        @Override
        public void remove() {
            // ignore
        }

    }

    private void testTriggerAlterTable() throws SQLException {
        deleteDb("trigger");
        testTrigger(null);
    }

    private void testTriggerAsSource() throws SQLException {
        deleteDb("trigger");
        testTrigger("java");
    }

    private void testTriggerAsJavascript() throws SQLException {
        deleteDb("trigger");
        testTrigger("javascript");
    }

    private void testTrigger(final String sourceLang) throws SQLException {
        final String callSeq = "call seq.nextval";
        Connection conn = getConnection("trigger");
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("create sequence seq");
        stat.execute("create table test(id int primary key)");
        assertSingleValue(stat, callSeq, 1);
        conn.setAutoCommit(false);
        Trigger t = new org.h2.test.db.TestTriggersConstraints.TestTriggerAlterTable();
        t.close();
        if ("java".equals(sourceLang)) {
            String triggerClassName = this.getClass().getName() + "."
                    + TestTriggerAlterTable.class.getSimpleName();
            stat.execute("create trigger test_upd before insert on test "
                    + "as $$org.h2.api.Trigger create() " + "{ return new "
                    + triggerClassName + "(); } $$");
        } else if ("javascript".equals(sourceLang)) {
            String triggerClassName = this.getClass().getName() + "."
                    + TestTriggerAlterTable.class.getSimpleName();
            final String body = "//javascript\n"
                    + "new Packages." + triggerClassName + "();";
            stat.execute("create trigger test_upd before insert on test as $$"
                    + body + " $$");
        } else {
            stat.execute("create trigger test_upd before insert on test call \""
                    + TestTriggerAlterTable.class.getName() + "\"");
        }
        stat.execute("insert into test values(1)");
        assertSingleValue(stat, callSeq, 3);
        stat.execute("alter table test add column name varchar");
        assertSingleValue(stat, callSeq, 4);
        stat.execute("drop sequence seq");
        stat.execute("drop table test");
        conn.close();
    }

    private void testConstraints() throws SQLException {
        Connection conn = getConnection("trigger");
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("create table test(id int primary key, parent int)");
        stat.execute("alter table test add constraint test_parent_id " +
                "foreign key(parent) references test (id) on delete cascade");
        stat.execute("insert into test select x, x/2 from system_range(0, 100)");
        stat.execute("delete from test");
        assertSingleValue(stat, "select count(*) from test", 0);
        stat.execute("drop table test");
        conn.close();
    }

    private void testCheckConstraintErrorMessage() throws SQLException {
        Connection conn = getConnection("trigger");
        Statement stat = conn.createStatement();

        stat.execute("create table companies(id identity)");
        stat.execute("create table departments(id identity, "
                + "company_id int not null, "
                + "foreign key(company_id) references companies(id))");
        stat.execute("create table connections (id identity, company_id int not null, "
                + "first int not null, second int not null, "
                + "foreign key (company_id) references companies(id), "
                + "foreign key (first) references departments(id), "
                + "foreign key (second) references departments(id), "
                + "check (select departments.company_id from departments, companies where "
                + "       departments.id in (first, second)) = company_id)");
        stat.execute("insert into companies(id) values(1)");
        stat.execute("insert into departments(id, company_id) "
                + "values(10, 1)");
        stat.execute("insert into departments(id, company_id) "
                + "values(20, 1)");
        assertThrows(ErrorCode.CHECK_CONSTRAINT_INVALID, stat)
            .execute("insert into connections(id, company_id, first, second) "
                + "values(100, 1, 10, 20)");

        stat.execute("drop table connections");
        stat.execute("drop table departments");
        stat.execute("drop table companies");
        conn.close();
    }

    /**
     * Regression test: we had a bug where the AlterTableAddConstraint class
     * used to sometimes pick the wrong unique index for a foreign key.
     */
    private void testMultiPartForeignKeys() throws SQLException {
        Connection conn = getConnection("trigger");
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST1");
        stat.execute("DROP TABLE IF EXISTS TEST2");

        stat.execute("create table test1(id int primary key, col1 int)");
        stat.execute("alter table test1 add constraint unique_test1 " +
                "unique (id,col1)");

        stat.execute("create table test2(id int primary key, col1 int)");
        stat.execute("alter table test2 add constraint fk_test2 " +
                "foreign key(id,col1) references test1 (id,col1)");

        stat.execute("insert into test1 values (1,1)");
        stat.execute("insert into test1 values (2,2)");
        stat.execute("insert into test1 values (3,3)");
        stat.execute("insert into test2 values (1,1)");
        assertThrows(23506, stat).execute("insert into test2 values (2,1)");
        assertSingleValue(stat, "select count(*) from test1", 3);
        assertSingleValue(stat, "select count(*) from test2", 1);

        stat.execute("drop table test1");
        stat.execute("drop table test2");
        conn.close();
    }

    private void testTriggers() throws SQLException {
        mustNotCallTrigger = false;
        Connection conn = getConnection("trigger");
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        // CREATE TRIGGER trigger {BEFORE|AFTER}
        // {INSERT|UPDATE|DELETE|ROLLBACK} ON table
        // [FOR EACH ROW] [QUEUE n] [NOWAIT] CALL triggeredClass
        stat.execute("CREATE TRIGGER IF NOT EXISTS INS_BEFORE " +
                "BEFORE INSERT ON TEST " +
                "FOR EACH ROW NOWAIT CALL \"" + getClass().getName() + "\"");
        stat.execute("CREATE TRIGGER IF NOT EXISTS INS_BEFORE " +
                "BEFORE INSERT ON TEST " +
                "FOR EACH ROW NOWAIT CALL \"" + getClass().getName() + "\"");
        stat.execute("CREATE TRIGGER INS_AFTER " + "" +
                "AFTER INSERT ON TEST " +
                "FOR EACH ROW NOWAIT CALL \"" + getClass().getName() + "\"");
        stat.execute("CREATE TRIGGER UPD_BEFORE " +
                "BEFORE UPDATE ON TEST " +
                "FOR EACH ROW NOWAIT CALL \"" + getClass().getName() + "\"");
        stat.execute("CREATE TRIGGER INS_AFTER_ROLLBACK " +
                "AFTER INSERT, ROLLBACK ON TEST " +
                "FOR EACH ROW NOWAIT CALL \"" + getClass().getName() + "\"");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        ResultSet rs;
        rs = stat.executeQuery("SCRIPT");
        checkRows(rs, new String[] {
                "CREATE FORCE TRIGGER PUBLIC.INS_BEFORE " +
                    "BEFORE INSERT ON PUBLIC.TEST " +
                    "FOR EACH ROW NOWAIT CALL \"" + getClass().getName() + "\";",
                "CREATE FORCE TRIGGER PUBLIC.INS_AFTER " +
                    "AFTER INSERT ON PUBLIC.TEST " +
                    "FOR EACH ROW NOWAIT CALL \"" + getClass().getName() + "\";",
                "CREATE FORCE TRIGGER PUBLIC.UPD_BEFORE " +
                    "BEFORE UPDATE ON PUBLIC.TEST " +
                    "FOR EACH ROW NOWAIT CALL \"" + getClass().getName() + "\";",
                "CREATE FORCE TRIGGER PUBLIC.INS_AFTER_ROLLBACK " +
                    "AFTER INSERT, ROLLBACK ON PUBLIC.TEST " +
                    "FOR EACH ROW NOWAIT CALL \"" + getClass().getName() + "\";",
                        });
        while (rs.next()) {
            String sql = rs.getString(1);
            if (sql.startsWith("CREATE TRIGGER")) {
                System.out.println(sql);
            }
        }

        rs = stat.executeQuery("SELECT * FROM TEST");
        rs.next();
        assertEquals("Hello-updated", rs.getString(2));
        assertFalse(rs.next());
        stat.execute("UPDATE TEST SET NAME=NAME||'-upd'");
        rs = stat.executeQuery("SELECT * FROM TEST");
        rs.next();
        assertEquals("Hello-updated-upd-updated2", rs.getString(2));
        assertFalse(rs.next());

        mustNotCallTrigger = true;
        stat.execute("DROP TRIGGER IF EXISTS INS_BEFORE");
        stat.execute("DROP TRIGGER IF EXISTS INS_BEFORE");
        stat.execute("DROP TRIGGER IF EXISTS INS_AFTER_ROLLBACK");
        assertThrows(ErrorCode.TRIGGER_NOT_FOUND_1, stat).
                execute("DROP TRIGGER INS_BEFORE");
        stat.execute("DROP TRIGGER  INS_AFTER");
        stat.execute("DROP TRIGGER  UPD_BEFORE");
        stat.execute("UPDATE TEST SET NAME=NAME||'-upd-no_trigger'");
        stat.execute("INSERT INTO TEST VALUES(100, 'Insert-no_trigger')");
        conn.close();

        conn = getConnection("trigger");

        mustNotCallTrigger = false;
        conn.close();
    }

    private void checkRows(ResultSet rs, String[] expected) throws SQLException {
        HashSet<String> set = new HashSet<>(Arrays.asList(expected));
        while (rs.next()) {
            set.remove(rs.getString(1));
        }
        if (set.size() > 0) {
            fail("set should be empty: " + set);
        }
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow)
            throws SQLException {
        if (mustNotCallTrigger) {
            throw new AssertionError("must not be called now");
        }
        if (conn == null) {
            throw new AssertionError("connection is null");
        }
        if (triggerName.startsWith("INS_BEFORE")) {
            newRow[1] = newRow[1] + "-updated";
        } else if (triggerName.startsWith("INS_AFTER")) {
            if (!newRow[1].toString().endsWith("-updated")) {
                throw new AssertionError("supposed to be updated");
            }
            checkCommit(conn);
        } else if (triggerName.startsWith("UPD_BEFORE")) {
            newRow[1] = newRow[1] + "-updated2";
        } else if (triggerName.startsWith("UPD_AFTER")) {
            if (!newRow[1].toString().endsWith("-updated2")) {
                throw new AssertionError("supposed to be updated2");
            }
            checkCommit(conn);
        }
    }

    @Override
    public void close() {
        // ignore
    }

    @Override
    public void remove() {
        // ignore
    }

    private void checkCommit(Connection conn) throws SQLException {
        assertThrows(ErrorCode.COMMIT_ROLLBACK_NOT_ALLOWED, conn).commit();
        assertThrows(ErrorCode.COMMIT_ROLLBACK_NOT_ALLOWED, conn.createStatement()).
                execute("CREATE TABLE X(ID INT)");
    }

    @Override
    public void init(Connection conn, String schemaName, String trigger,
            String tableName, boolean before, int type) {
        this.triggerName = trigger;
        if (!"TEST".equals(tableName)) {
            throw new AssertionError("supposed to be TEST");
        }
        if ((trigger.endsWith("AFTER") && before) ||
                (trigger.endsWith("BEFORE") && !before)) {
            throw new AssertionError("triggerName: " + trigger + " before:" + before);
        }
        if ((trigger.startsWith("UPD") && type != UPDATE) ||
                (trigger.startsWith("INS") && type != INSERT) ||
                (trigger.startsWith("DEL") && type != DELETE)) {
            throw new AssertionError("triggerName: " + trigger + " type:" + type);
        }
    }

}
