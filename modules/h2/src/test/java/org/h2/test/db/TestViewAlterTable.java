/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.test.TestBase;
import org.h2.api.ErrorCode;

/**
 * Test the impact of ALTER TABLE statements on views.
 */
public class TestViewAlterTable extends TestBase {

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

        testDropColumnWithoutViews();
        testViewsAreWorking();
        testAlterTableDropColumnNotInView();
        testAlterTableDropColumnInView();
        testAlterTableAddColumnWithView();
        testAlterTableAlterColumnDataTypeWithView();
        testSelectStar();
        testJoinAndAlias();
        testSubSelect();
        testForeignKey();

        conn.close();
        deleteDb(getTestName());
    }

    private void testDropColumnWithoutViews() throws SQLException {
        stat.execute("create table test(a int, b int, c int)");
        stat.execute("alter table test drop column c");
        stat.execute("drop table test");
    }

    private void testViewsAreWorking() throws SQLException {
        createTestData();
        checkViewRemainsValid();
    }

    private void testAlterTableDropColumnNotInView() throws SQLException {
        createTestData();
        stat.execute("alter table test drop column c");
        checkViewRemainsValid();
    }

    private void testAlterTableDropColumnInView() throws SQLException {
        // simple
        stat.execute("create table test(id identity, name varchar) " +
                "as select x, 'Hello'");
        stat.execute("create view test_view as select * from test");
        assertThrows(ErrorCode.VIEW_IS_INVALID_2, stat).
                execute("alter table test drop name");
        ResultSet rs = stat.executeQuery("select * from test_view");
        assertTrue(rs.next());
        stat.execute("drop view test_view");
        stat.execute("drop table test");

        // nested
        createTestData();
        // should throw exception because V1 uses column A
        assertThrows(ErrorCode.VIEW_IS_INVALID_2, stat).
                execute("alter table test drop column a");
        stat.execute("drop table test cascade");
    }

    private void testAlterTableAddColumnWithView() throws SQLException {
        createTestData();
        stat.execute("alter table test add column d int");
        checkViewRemainsValid();
    }

    private void testAlterTableAlterColumnDataTypeWithView()
            throws SQLException {
        createTestData();
        stat.execute("alter table test alter b char(1)");
        checkViewRemainsValid();
    }

    private void testSelectStar() throws SQLException {
        createTestData();
        stat.execute("create view v4 as select * from test");
        stat.execute("alter table test add d int default 6");
        // H2 doesn't remember v4 as 'select * from test', it instead remembers
        // each individual column that was in 'test' when the view was
        // originally created. This is consistent with PostgreSQL.
        assertThrows(ErrorCode.COLUMN_NOT_FOUND_1, stat).
            executeQuery("select d from v4");
        checkViewRemainsValid();
    }

    private void testJoinAndAlias() throws SQLException {
        createTestData();
        stat.execute("create view v4 as select v1.a dog, v3.a cat " +
                "from v1 join v3 on v1.b = v3.a");
        // should make no difference
        stat.execute("alter table test add d int default 6");
        ResultSet rs = stat.executeQuery("select cat, dog from v4");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        assertFalse(rs.next());
        checkViewRemainsValid();
    }

    private void testSubSelect() throws SQLException {
        createTestData();
        stat.execute("create view v4 as select * from v3 " +
                "where a in (select b from v2)");
        // should make no difference
        stat.execute("alter table test add d int default 6");
        ResultSet rs = stat.executeQuery("select a from v4");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());
        checkViewRemainsValid();
    }

    private void testForeignKey() throws SQLException {
        createTestData();
        stat.execute("create table test2(z int, a int, primary key(z), " +
                "foreign key (a) references TEST(a))");
        stat.execute("insert into test2(z, a) values (99, 1)");
        // should make no difference
        stat.execute("alter table test add d int default 6");
        ResultSet rs = stat.executeQuery("select z from test2");
        assertTrue(rs.next());
        assertEquals(99, rs.getInt(1));
        assertFalse(rs.next());
        stat.execute("drop table test2");
        checkViewRemainsValid();
    }

    private void createTestData() throws SQLException {
        stat.execute("create table test(a int, b int, c int)");
        stat.execute("insert into test(a, b, c) values (1, 2, 3)");
        stat.execute("create view v1 as select a as b, b as a from test");
        // child of v1
        stat.execute("create view v2 as select * from v1");
        stat.execute("create user if not exists test_user password 'x'");
        stat.execute("grant select on v2 to test_user");
        // sibling of v1
        stat.execute("create view v3 as select a from test");
    }

    private void checkViewRemainsValid() throws SQLException {
        ResultSet rs = stat.executeQuery("select b from v1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());

        rs = stat.executeQuery("select b from v2");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());

        rs = stat.executeQuery("select * from information_schema.rights");
        assertTrue(rs.next());
        assertEquals("TEST_USER", rs.getString("GRANTEE"));
        assertEquals("V2", rs.getString("TABLE_NAME"));
        rs = stat.executeQuery("select b from test");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());

        stat.execute("drop table test cascade");

        rs = conn.getMetaData().getTables(null, null, null, null);
        while (rs.next()) {
            // should have no tables left in the database
            assertEquals(rs.getString(2) + "." + rs.getString(3),
                    "INFORMATION_SCHEMA", rs.getString(2));
        }

    }
}
