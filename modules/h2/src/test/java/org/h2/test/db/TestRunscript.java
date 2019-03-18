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
import org.h2.api.ErrorCode;
import org.h2.api.Trigger;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.tools.ChangeFileEncryption;
import org.h2.tools.Recover;
import org.h2.util.Task;

/**
 * Tests the RUNSCRIPT SQL statement.
 */
public class TestRunscript extends TestBase implements Trigger {

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
        test(false);
        test(true);
        testDropReferencedUserDefinedFunction();
        testDropCascade();
        testScriptExcludeSchema();
        testScriptExcludeTable();
        testScriptExcludeFunctionAlias();
        testScriptExcludeConstant();
        testScriptExcludeSequence();
        testScriptExcludeConstraint();
        testScriptExcludeTrigger();
        testScriptExcludeRight();
        testRunscriptFromClasspath();
        testCancelScript();
        testEncoding();
        testClobPrimaryKey();
        deleteDb("runscript");
    }

    private void testDropReferencedUserDefinedFunction() throws Exception {
        deleteDb("runscript");
        Connection conn;
        conn = getConnection("runscript");
        Statement stat = conn.createStatement();
        stat.execute("create alias int_decode for \"java.lang.Integer.decode\"");
        stat.execute("create table test(x varchar, y int as int_decode(x))");
        stat.execute("script simple drop to '" +
                getBaseDir() + "/backup.sql'");
        stat.execute("runscript from '" +
                getBaseDir() + "/backup.sql'");
        conn.close();
    }

    private void testDropCascade() throws Exception {
        deleteDb("runscript");
        Connection conn;
        conn = getConnection("runscript");
        Statement stat = conn.createStatement();
        stat.execute("create table b(x int)");
        stat.execute("create view a as select * from b");
        stat.execute("script simple drop to '" +
                getBaseDir() + "/backup.sql'");
        stat.execute("runscript from '" +
                getBaseDir() + "/backup.sql'");
        conn.close();
    }

    private void testScriptExcludeSchema() throws Exception {
        deleteDb("runscript");
        Connection conn;
        ResultSet rs;
        conn = getConnection("runscript");
        Statement stat = conn.createStatement();
        stat.execute("create schema include_schema1");
        stat.execute("create schema exclude_schema1");
        stat.execute("script schema include_schema1");
        rs = stat.getResultSet();
        while (rs.next()) {
            assertFalse("The schema 'exclude_schema1' should not be present in the script",
                    rs.getString(1).contains("exclude_schema1".toUpperCase()));
        }
        rs.close();
        stat.execute("create schema include_schema2");
        stat.execute("script nosettings schema include_schema1, include_schema2");
        rs = stat.getResultSet();
        // user and one row per schema = 3
        assertResultRowCount(3, rs);
        rs.close();
        conn.close();
    }

    private void testScriptExcludeTable() throws Exception {
        deleteDb("runscript");
        Connection conn;
        ResultSet rs;
        conn = getConnection("runscript");
        Statement stat = conn.createStatement();
        stat.execute("create schema a");
        stat.execute("create schema b");
        stat.execute("create schema c");
        stat.execute("create table a.test1(x varchar, y int)");
        stat.execute("create table a.test2(x varchar, y int)");
        stat.execute("create table b.test1(x varchar, y int)");
        stat.execute("create table b.test2(x varchar, y int)");
        stat.execute("script table a.test1");
        rs = stat.getResultSet();
        while (rs.next()) {
            assertFalse("The table 'a.test2' should not be present in the script",
                    rs.getString(1).contains("a.test2".toUpperCase()));
            assertFalse("The table 'b.test1' should not be present in the script",
                    rs.getString(1).contains("b.test1".toUpperCase()));
            assertFalse("The table 'b.test2' should not be present in the script",
                    rs.getString(1).contains("b.test2".toUpperCase()));
        }
        rs.close();
        stat.execute("set schema b");
        stat.execute("script table test1");
        rs = stat.getResultSet();
        while (rs.next()) {
            assertFalse("The table 'a.test1' should not be present in the script",
                    rs.getString(1).contains("a.test1".toUpperCase()));
            assertFalse("The table 'a.test2' should not be present in the script",
                    rs.getString(1).contains("a.test2".toUpperCase()));
            assertFalse("The table 'b.test2' should not be present in the script",
                    rs.getString(1).contains("b.test2".toUpperCase()));
        }
        stat.execute("script nosettings table a.test1, test2");
        rs = stat.getResultSet();
        // user, schemas 'a' & 'b' and 2 rows per table = 7
        assertResultRowCount(7, rs);
        rs.close();
        conn.close();
    }

    private void testScriptExcludeFunctionAlias() throws Exception {
        deleteDb("runscript");
        Connection conn;
        ResultSet rs;
        conn = getConnection("runscript");
        Statement stat = conn.createStatement();
        stat.execute("create schema a");
        stat.execute("create schema b");
        stat.execute("create schema c");
        stat.execute("create alias a.int_decode for \"java.lang.Integer.decode\"");
        stat.execute("create table a.test(x varchar, y int as a.int_decode(x))");
        stat.execute("script schema b");
        rs = stat.getResultSet();
        while (rs.next()) {
            assertFalse("The function alias 'int_decode' " +
                    "should not be present in the script",
                    rs.getString(1).contains("int_decode".toUpperCase()));
        }
        rs.close();
        conn.close();
    }

    private void testScriptExcludeConstant() throws Exception {
        deleteDb("runscript");
        Connection conn;
        ResultSet rs;
        conn = getConnection("runscript");
        Statement stat = conn.createStatement();
        stat.execute("create schema a");
        stat.execute("create schema b");
        stat.execute("create schema c");
        stat.execute("create constant a.default_email value 'no@thanks.org'");
        stat.execute("create table a.test1(x varchar, " +
                "email varchar default a.default_email)");
        stat.execute("script schema b");
        rs = stat.getResultSet();
        while (rs.next()) {
            assertFalse("The constant 'default_email' " +
                    "should not be present in the script",
                    rs.getString(1).contains("default_email".toUpperCase()));
        }
        rs.close();
        conn.close();
    }

    private void testScriptExcludeSequence() throws Exception {
        deleteDb("runscript");
        Connection conn;
        ResultSet rs;
        conn = getConnection("runscript");
        Statement stat = conn.createStatement();
        stat.execute("create schema a");
        stat.execute("create schema b");
        stat.execute("create schema c");
        stat.execute("create sequence a.seq_id");
        stat.execute("script schema b");
        rs = stat.getResultSet();
        while (rs.next()) {
            assertFalse("The sequence 'seq_id' should not be present in the script",
                    rs.getString(1).contains("seq_id".toUpperCase()));
        }
        rs.close();
        conn.close();
    }

    private void testScriptExcludeConstraint() throws Exception {
        deleteDb("runscript");
        Connection conn;
        ResultSet rs;
        conn = getConnection("runscript");
        Statement stat = conn.createStatement();
        stat.execute("create schema a");
        stat.execute("create schema b");
        stat.execute("create schema c");
        stat.execute("create table a.test1(x varchar, y int)");
        stat.execute("alter table a.test1 add constraint " +
                "unique_constraint unique (x, y) ");
        stat.execute("script schema b");
        rs = stat.getResultSet();
        while (rs.next()) {
            assertFalse("The sequence 'unique_constraint' " +
                    "should not be present in the script",
                    rs.getString(1).contains("unique_constraint".toUpperCase()));
        }
        rs.close();
        stat.execute("create table a.test2(x varchar, y int)");
        stat.execute("script table a.test2");
        rs = stat.getResultSet();
        while (rs.next()) {
            assertFalse("The sequence 'unique_constraint' " +
                    "should not be present in the script",
                    rs.getString(1).contains("unique_constraint".toUpperCase()));
        }
        rs.close();
        conn.close();
    }

    private void testScriptExcludeTrigger() throws Exception {
        deleteDb("runscript");
        Connection conn;
        ResultSet rs;
        conn = getConnection("runscript");
        Statement stat = conn.createStatement();
        stat.execute("create schema a");
        stat.execute("create schema b");
        stat.execute("create schema c");
        stat.execute("create table a.test1(x varchar, y int)");
        stat.execute("create trigger trigger_insert before insert on a.test1 " +
                "for each row call \"org.h2.test.db.TestRunscript\"");
        stat.execute("script schema b");
        rs = stat.getResultSet();
        while (rs.next()) {
            assertFalse("The trigger 'trigger_insert' should not be present in the script",
                    rs.getString(1).contains("trigger_insert".toUpperCase()));
        }
        rs.close();
        stat.execute("create table a.test2(x varchar, y int)");
        stat.execute("script table a.test2");
        rs = stat.getResultSet();
        while (rs.next()) {
            assertFalse("The trigger 'trigger_insert' should not be present in the script",
                    rs.getString(1).contains("trigger_insert".toUpperCase()));
        }
        rs.close();
        conn.close();
    }

    private void testScriptExcludeRight() throws Exception {
        deleteDb("runscript");
        Connection conn;
        ResultSet rs;
        conn = getConnection("runscript");
        Statement stat = conn.createStatement();
        stat.execute("create user USER_A1 password 'test'");
        stat.execute("create user USER_B1 password 'test'");
        stat.execute("create schema a");
        stat.execute("create schema b");
        stat.execute("create schema c");
        stat.execute("create table a.test1(x varchar, y int)");
        stat.execute("create table b.test1(x varchar, y int)");
        stat.execute("grant select on a.test1 to USER_A1");
        stat.execute("grant select on b.test1 to USER_B1");
        stat.execute("script schema b");
        rs = stat.getResultSet();
        while (rs.next()) {
            assertFalse("The grant to 'USER_A1' should not be present in the script",
                    rs.getString(1).contains("to USER_A1".toUpperCase()));
        }
        rs.close();
        stat.execute("create user USER_A2 password 'test'");
        stat.execute("create table a.test2(x varchar, y int)");
        stat.execute("grant select on a.test2 to USER_A2");
        stat.execute("script table a.test2");
        rs = stat.getResultSet();
        while (rs.next()) {
            assertFalse("The grant to 'USER_A1' should not be present in the script",
                    rs.getString(1).contains("to USER_A1".toUpperCase()));
            assertFalse("The grant to 'USER_B1' should not be present in the script",
                    rs.getString(1).contains("to USER_B1".toUpperCase()));
        }
        rs.close();
        conn.close();
    }

    private void testRunscriptFromClasspath() throws Exception {
        deleteDb("runscript");
        Connection conn;
        conn = getConnection("runscript");
        Statement stat = conn.createStatement();
        stat.execute("runscript from 'classpath:/org/h2/samples/newsfeed.sql'");
        stat.execute("select * from version");
        conn.close();
    }

    private void testCancelScript() throws Exception {
        if (config.travis) {
            // fails regularly under Travis, not sure why
            return;
        }
        deleteDb("runscript");
        Connection conn;
        conn = getConnection("runscript");
        final Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key) as " +
                "select x from system_range(1, 20000)");
        stat.execute("script simple drop to '"+
                getBaseDir()+"/backup.sql'");
        stat.execute("set throttle 1000");
        // need to wait a bit (throttle is only used every 50 ms)
        Thread.sleep(200);
        final String dir = getBaseDir();
        Task task;
        task = new Task() {
            @Override
            public void call() throws SQLException {
                stat.execute("script simple drop to '"+dir+"/backup2.sql'");
            }
        };
        task.execute();
        Thread.sleep(200);
        stat.cancel();
        SQLException e = (SQLException) task.getException();
        assertTrue(e != null);
        assertEquals(ErrorCode.STATEMENT_WAS_CANCELED, e.getErrorCode());

        stat.execute("set throttle 1000");
        // need to wait a bit (throttle is only used every 50 ms)
        Thread.sleep(100);

        task = new Task() {
            @Override
            public void call() throws SQLException {
                stat.execute("runscript from '"+dir+"/backup.sql'");
            }
        };
        task.execute();
        Thread.sleep(200);
        stat.cancel();
        e = (SQLException) task.getException();
        assertTrue(e != null);
        assertEquals(ErrorCode.STATEMENT_WAS_CANCELED, e.getErrorCode());

        conn.close();
        FileUtils.delete(getBaseDir() + "/backup.sql");
        FileUtils.delete(getBaseDir() + "/backup2.sql");
    }

    private void testEncoding() throws SQLException {
        deleteDb("runscript");
        Connection conn;
        Statement stat;
        conn = getConnection("runscript");
        stat = conn.createStatement();
        stat.execute("create table \"t\u00f6\"(id int)");
        stat.execute("script to '"+
                getBaseDir()+"/backup.sql'");
        stat.execute("drop all objects");
        stat.execute("runscript from '"+
                getBaseDir()+"/backup.sql'");
        stat.execute("select * from \"t\u00f6\"");
        stat.execute("script to '"+
                getBaseDir()+"/backup.sql' charset 'UTF-8'");
        stat.execute("drop all objects");
        stat.execute("runscript from '"+
                getBaseDir()+"/backup.sql' charset 'UTF-8'");
        stat.execute("select * from \"t\u00f6\"");
        conn.close();
        FileUtils.delete(getBaseDir() + "/backup.sql");
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param a the value
     * @return the absolute value
     */
    public static int test(int a) {
        return Math.abs(a);
    }

    private void testClobPrimaryKey() throws SQLException {
        deleteDb("runscript");
        Connection conn;
        Statement stat;
        conn = getConnection("runscript");
        stat = conn.createStatement();
        stat.execute("create table test(id int not null, data clob) " +
                "as select 1, space(4100)");
        // the primary key for SYSTEM_LOB_STREAM used to be named like this
        stat.execute("create primary key primary_key_e on test(id)");
        stat.execute("script to '" + getBaseDir() + "/backup.sql'");
        conn.close();
        deleteDb("runscript");
        conn = getConnection("runscript");
        stat = conn.createStatement();
        stat.execute("runscript from '" + getBaseDir() + "/backup.sql'");
        conn.close();
        deleteDb("runscriptRestore");
        FileUtils.delete(getBaseDir() + "/backup.sql");
    }

    private void test(boolean password) throws SQLException {
        deleteDb("runscript");
        Connection conn1, conn2;
        Statement stat1, stat2;
        conn1 = getConnection("runscript");
        stat1 = conn1.createStatement();
        stat1.execute("create table test (id identity, name varchar(12))");
        stat1.execute("insert into test (name) values ('first'), ('second')");
        stat1.execute("create table test2(id int primary key) as " +
                "select x from system_range(1, 5000)");
        stat1.execute("create sequence testSeq start with 100 increment by 10");
        stat1.execute("create alias myTest for \"" +
                getClass().getName() + ".test\"");
        stat1.execute("create trigger myTrigger before insert " +
                "on test nowait call \"" + getClass().getName() + "\"");
        stat1.execute("create view testView as select * " +
                "from test where 1=0 union all " +
                "select * from test where 0=1");
        stat1.execute("create user testAdmin salt '00' hash '01' admin");
        stat1.execute("create schema testSchema authorization testAdmin");
        stat1.execute("create table testSchema.parent" +
                "(id int primary key, name varchar)");
        stat1.execute("create index idxname on testSchema.parent(name)");
        stat1.execute("create table testSchema.child(id int primary key, " +
                "parentId int, name varchar, foreign key(parentId) " +
                "references parent(id))");
        stat1.execute("create user testUser salt '02' hash '03'");
        stat1.execute("create role testRole");
        stat1.execute("grant all on testSchema.child to testUser");
        stat1.execute("grant select, insert on testSchema.parent to testRole");
        stat1.execute("grant testRole to testUser");
        stat1.execute("create table blob (value blob)");
        PreparedStatement prep = conn1.prepareStatement(
                "insert into blob values (?)");
        prep.setBytes(1, new byte[65536]);
        prep.execute();
        String sql = "script to ?";
        if (password) {
            sql += " CIPHER AES PASSWORD ?";
        }
        prep = conn1.prepareStatement(sql);
        prep.setString(1, getBaseDir() + "/backup.2.sql");
        if (password) {
            prep.setString(2, "t1e2s3t4");
        }
        prep.execute();

        deleteDb("runscriptRestore");
        conn2 = getConnection("runscriptRestore");
        stat2 = conn2.createStatement();
        sql = "runscript from '" + getBaseDir() + "/backup.2.sql'";
        if (password) {
            sql += " CIPHER AES PASSWORD 'wrongPassword'";
        }
        if (password) {
            assertThrows(ErrorCode.FILE_ENCRYPTION_ERROR_1, stat2).
                    execute(sql);
        }
        sql = "runscript from '" + getBaseDir() + "/backup.2.sql'";
        if (password) {
            sql += " CIPHER AES PASSWORD 't1e2s3t4'";
        }
        stat2.execute(sql);
        stat2.execute("script to '" + getBaseDir() + "/backup.3.sql'");

        assertEqualDatabases(stat1, stat2);

        if (!config.memory && !config.reopen) {
            conn1.close();

            if (config.cipher != null) {
                ChangeFileEncryption.execute(getBaseDir(), "runscript",
                        config.cipher, getFilePassword().toCharArray(), null, true);
            }
            Recover.execute(getBaseDir(), "runscript");

            deleteDb("runscriptRestoreRecover");
            Connection conn3 = getConnection("runscriptRestoreRecover");
            Statement stat3 = conn3.createStatement();
            stat3.execute("runscript from '" + getBaseDir() + "/runscript.h2.sql'");
            conn3.close();
            conn3 = getConnection("runscriptRestoreRecover");
            stat3 = conn3.createStatement();

            if (config.cipher != null) {
                ChangeFileEncryption.execute(getBaseDir(),
                        "runscript", config.cipher, null, getFilePassword().toCharArray(), true);
            }

            conn1 = getConnection("runscript");
            stat1 = conn1.createStatement();

            assertEqualDatabases(stat1, stat3);
            conn3.close();
        }

        assertEqualDatabases(stat1, stat2);

        conn1.close();
        conn2.close();
        deleteDb("runscriptRestore");
        deleteDb("runscriptRestoreRecover");
        FileUtils.delete(getBaseDir() + "/backup.2.sql");
        FileUtils.delete(getBaseDir() + "/backup.3.sql");

    }

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
            String tableName, boolean before, int type) {
        if (!before) {
            throw new InternalError("before:" + before);
        }
        if (type != INSERT) {
            throw new InternalError("type:" + type);
        }
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) {
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
