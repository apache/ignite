/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.api.ErrorCode;
import org.h2.test.TestBase;

/**
 * Test ALTER statements.
 */
public class TestAlter extends TestBase {

    private Connection conn;
    private Statement stat;

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
        deleteDb(getTestName());
        conn = getConnection(getTestName());
        stat = conn.createStatement();
        testAlterTableRenameConstraint();
        testAlterTableAlterColumnAsSelfColumn();
        testAlterTableDropColumnWithReferences();
        testAlterTableDropMultipleColumns();
        testAlterTableAlterColumnWithConstraint();
        testAlterTableAlterColumn();
        testAlterTableAddColumnIdentity();
        testAlterTableDropIdentityColumn();
        testAlterTableAddColumnIfNotExists();
        testAlterTableAddMultipleColumns();
        testAlterTableAlterColumn2();
        testAlterTableAddColumnBefore();
        testAlterTableAddColumnAfter();
        testAlterTableAddMultipleColumnsBefore();
        testAlterTableAddMultipleColumnsAfter();
        testAlterTableModifyColumn();
        testAlterTableModifyColumnSetNull();
        testAlterTableModifyColumnNotNullOracle();
        conn.close();
        deleteDb(getTestName());
    }

    private void testAlterTableAlterColumnAsSelfColumn() throws SQLException {
        stat.execute("create table test(id int, name varchar)");
        stat.execute("alter table test alter column id int as id+1");
        stat.execute("insert into test values(1, 'Hello')");
        stat.execute("update test set name='World'");
        ResultSet rs = stat.executeQuery("select * from test");
        rs.next();
        assertEquals(3, rs.getInt(1));
        stat.execute("drop table test");
    }

    private void testAlterTableDropColumnWithReferences() throws SQLException {
        stat.execute("create table parent(id int, b int)");
        stat.execute("create table child(p int primary key)");
        stat.execute("alter table child add foreign key(p) references parent(id)");
        stat.execute("alter table parent drop column id");
        stat.execute("drop table parent");
        stat.execute("drop table child");

        stat.execute("create table test(id int, name varchar(255))");
        stat.execute("alter table test add constraint x check (id > name)");

        // the constraint references multiple columns
        assertThrows(ErrorCode.COLUMN_IS_REFERENCED_1, stat).
                execute("alter table test drop column id");

        stat.execute("drop table test");

        stat.execute("create table test(id int, name varchar(255))");
        stat.execute("alter table test add constraint x unique(id, name)");

        // the constraint references multiple columns
        assertThrows(ErrorCode.COLUMN_IS_REFERENCED_1, stat).
                execute("alter table test drop column id");

        stat.execute("drop table test");

        stat.execute("create table test(id int, name varchar(255))");
        stat.execute("alter table test add constraint x check (id > 1)");
        stat.execute("alter table test drop column id");
        stat.execute("drop table test");

        stat.execute("create table test(id int, name varchar(255))");
        stat.execute("alter table test add constraint x check (name > 'TEST.ID')");
        // previous versions of H2 used sql.indexOf(columnName)
        // to check if the column is referenced
        stat.execute("alter table test drop column id");
        stat.execute("drop table test");

        stat.execute("create table test(id int, name varchar(255))");
        stat.execute("alter table test add constraint x unique(id)");
        stat.execute("alter table test drop column id");
        stat.execute("drop table test");

    }

    private void testAlterTableDropMultipleColumns() throws SQLException {
        stat.execute("create table test(id int, b varchar, c int, d int)");
        stat.execute("alter table test drop column b, c");
        stat.execute("alter table test drop d");
        stat.execute("drop table test");
        // Test-Case: Same as above but using brackets (Oracle style)
        stat.execute("create table test(id int, b varchar, c int, d int)");
        stat.execute("alter table test drop column (b, c)");
        assertThrows(ErrorCode.COLUMN_NOT_FOUND_1, stat).
            execute("alter table test drop column b");
        stat.execute("alter table test drop (d)");
        stat.execute("drop table test");
        // Test-Case: Error if dropping all columns
        stat.execute("create table test(id int, name varchar, name2 varchar)");
        assertThrows(ErrorCode.CANNOT_DROP_LAST_COLUMN, stat).
            execute("alter table test drop column id, name, name2");
        stat.execute("drop table test");
    }

    /**
     * Tests a bug we used to have where altering the name of a column that had
     * a check constraint that referenced itself would result in not being able
     * to re-open the DB.
     */
    private void testAlterTableAlterColumnWithConstraint() throws SQLException {
        if (config.memory) {
            return;
        }
        stat.execute("create table test(id int check(id in (1,2)) )");
        stat.execute("alter table test alter id rename to id2");
        // disconnect and reconnect
        conn.close();
        conn = getConnection(getTestName());
        stat = conn.createStatement();
        stat.execute("insert into test values(1)");
        assertThrows(ErrorCode.CHECK_CONSTRAINT_VIOLATED_1, stat).
            execute("insert into test values(3)");
        stat.execute("drop table test");
    }

    private void testAlterTableRenameConstraint() throws SQLException {
        stat.execute("create table test(id int, name varchar(255))");
        stat.execute("alter table test add constraint x check (id > name)");
        stat.execute("alter table test rename constraint x to x2");
        stat.execute("drop table test");
    }

    private void testAlterTableDropIdentityColumn() throws SQLException {
        stat.execute("create table test(id int auto_increment, name varchar)");
        stat.execute("alter table test drop column id");
        ResultSet rs = stat.executeQuery("select * from INFORMATION_SCHEMA.SEQUENCES");
        assertFalse(rs.next());
        stat.execute("drop table test");

        stat.execute("create table test(id int auto_increment, name varchar)");
        stat.execute("alter table test drop column name");
        rs = stat.executeQuery("select * from INFORMATION_SCHEMA.SEQUENCES");
        assertTrue(rs.next());
        stat.execute("drop table test");
    }

    private void testAlterTableAlterColumn() throws SQLException {
        stat.execute("create table t(x varchar) as select 'x'");
        assertThrows(ErrorCode.DATA_CONVERSION_ERROR_1, stat).
                execute("alter table t alter column x int");
        stat.execute("drop table t");
        stat.execute("create table t(id identity, x varchar) as select null, 'x'");
        assertThrows(ErrorCode.DATA_CONVERSION_ERROR_1, stat).
                execute("alter table t alter column x int");
        stat.execute("drop table t");
    }

    private void testAlterTableAddColumnIdentity() throws SQLException {
        stat.execute("create table t(x varchar)");
        stat.execute("alter table t add id bigint identity(5, 5) not null");
        stat.execute("insert into t values (null, null)");
        stat.execute("insert into t values (null, null)");
        ResultSet rs = stat.executeQuery("select id from t order by id");
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(10, rs.getInt(1));
        assertFalse(rs.next());
        stat.execute("drop table t");
    }

    private void testAlterTableAddColumnIfNotExists() throws SQLException {
        stat.execute("create table t(x varchar) as select 'x'");
        stat.execute("alter table t add if not exists x int");
        stat.execute("drop table t");
        stat.execute("create table t(x varchar) as select 'x'");
        stat.execute("alter table t add if not exists y int");
        stat.execute("select x, y from t");
        stat.execute("drop table t");
    }

    private void testAlterTableAddMultipleColumns() throws SQLException {
        stat.execute("create table t(x varchar) as select 'x'");
        stat.execute("alter table t add (y int, z varchar)");
        stat.execute("drop table t");
        stat.execute("create table t(x varchar) as select 'x'");
        stat.execute("alter table t add (y int)");
        stat.execute("drop table t");
    }



    // column and field names must be upper-case due to getMetaData sensitivity
    private void testAlterTableAddMultipleColumnsBefore() throws SQLException {
        stat.execute("create table T(X varchar)");
        stat.execute("alter table T add (Y int, Z int) before X");
        DatabaseMetaData dbMeta = conn.getMetaData();
        ResultSet rs = dbMeta.getColumns(null, null, "T", null);
        assertTrue(rs.next());
        assertEquals("Y", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("Z", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("X", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());
        stat.execute("drop table T");
    }

    // column and field names must be upper-case due to getMetaData sensitivity
    private void testAlterTableAddMultipleColumnsAfter() throws SQLException {
        stat.execute("create table T(X varchar)");
        stat.execute("alter table T add (Y int, Z int) after X");
        DatabaseMetaData dbMeta = conn.getMetaData();
        ResultSet rs = dbMeta.getColumns(null, null, "T", null);
        assertTrue(rs.next());
        assertEquals("X", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("Y", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("Z", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());
        stat.execute("drop table T");
    }

    // column and field names must be upper-case due to getMetaData sensitivity
    private void testAlterTableAddColumnBefore() throws SQLException {
        stat.execute("create table T(X varchar)");
        stat.execute("alter table T add Y int before X");
        DatabaseMetaData dbMeta = conn.getMetaData();
        ResultSet rs = dbMeta.getColumns(null, null, "T", null);
        assertTrue(rs.next());
        assertEquals("Y", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("X", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());
        stat.execute("drop table T");
    }

    // column and field names must be upper-case due to getMetaData sensitivity
    private void testAlterTableAddColumnAfter() throws SQLException {
        stat.execute("create table T(X varchar)");
        stat.execute("alter table T add Y int after X");
        DatabaseMetaData dbMeta = conn.getMetaData();
        ResultSet rs = dbMeta.getColumns(null, null, "T", null);
        assertTrue(rs.next());
        assertEquals("X", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("Y", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());
        stat.execute("drop table T");
    }

    private void testAlterTableAlterColumn2() throws SQLException {
        // ensure that increasing a VARCHAR columns length takes effect because
        // we optimize this case
        stat.execute("create table t(x varchar(2)) as select 'x'");
        stat.execute("alter table t alter column x varchar(20)");
        stat.execute("insert into t values('Hello')");
        stat.execute("drop table t");
    }

    private void testAlterTableModifyColumn() throws SQLException {
        stat.execute("create table t(x int)");
        stat.execute("alter table t modify column x varchar(20)");
        stat.execute("insert into t values('Hello')");
        stat.execute("drop table t");
    }

    /**
     * Test for fix "Change not-null / null -constraint to existing column"
     * (MySql/ORACLE - SQL style) that failed silently corrupting the changed
     * column.<br/>
     * Before the change (added after v1.4.196) following was observed:
     * <pre>
     *  alter table T modify C int null; -- Worked as expected
     *  alter table T modify C null;     -- Silently corrupted column C
     * </pre>
     */
    private void testAlterTableModifyColumnSetNull() throws SQLException {
        // This worked in v1.4.196
        stat.execute("create table T (C varchar not null)");
        stat.execute("alter table T modify C int null");
        stat.execute("insert into T values(null)");
        stat.execute("drop table T");
        // This failed in v1.4.196
        stat.execute("create table T (C int not null)");
        stat.execute("alter table T modify C null"); // Silently corrupted column C
        stat.execute("insert into T values(null)"); // <- Fixed in v1.4.196 - NULL is allowed
        stat.execute("drop table T");
    }

    private void testAlterTableModifyColumnNotNullOracle() throws SQLException {
        stat.execute("create table foo (bar varchar(255))");
        stat.execute("alter table foo modify (bar varchar(255) not null)");
        try {
            stat.execute("insert into foo values(null)");
            fail("Null should not be allowed after modification.");
        }
        catch(SQLException e) {
            // This is what we expect, fails to insert null.
        }
    }
}
