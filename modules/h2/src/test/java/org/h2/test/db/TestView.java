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
import org.h2.engine.Session;
import org.h2.jdbc.JdbcConnection;
import org.h2.test.TestBase;

/**
 * Test for views.
 */
public class TestView extends TestBase {

    private static int x;

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
        deleteDb("view");
        testSubSubQuery();
        testSubQueryViewIndexCache();
        testInnerSelectWithRownum();
        testInnerSelectWithRange();
        testEmptyColumn();
        testChangeSchemaSearchPath();
        testParameterizedView();
        testCache();
        testCacheFunction(true);
        testCacheFunction(false);
        testInSelect();
        testUnionReconnect();
        testManyViews();
        testReferenceView();
        testViewAlterAndCommandCache();
        testViewConstraintFromColumnExpression();
        deleteDb("view");
    }

    private void testSubSubQuery() throws SQLException {
        Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        stat.execute("drop table test if exists");
        stat.execute("create table test(a int, b int, c int)");
        stat.execute("insert into test values(1, 1, 1)");
        ResultSet rs = stat.executeQuery("select 1 x from (select a, b, c from " +
                "(select * from test) bbb where bbb.a >=1 and bbb.a <= 1) sp " +
                "where sp.a = 1 and sp.b = 1 and sp.c = 1");
        assertTrue(rs.next());
        conn.close();
    }

    private void testSubQueryViewIndexCache() throws SQLException {
        if (config.networked) {
            return;
        }
        Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        stat.execute("drop table test if exists");
        stat.execute("create table test(id int primary key, " +
                "name varchar(25) unique, age int unique)");

        // check that initial cache size is empty
        Session s = (Session) ((JdbcConnection) conn).getSession();
        s.clearViewIndexCache();
        assertTrue(s.getViewIndexCache(true).isEmpty());
        assertTrue(s.getViewIndexCache(false).isEmpty());

        // create view command should not affect caches
        stat.execute("create view v as select * from test");
        assertTrue(s.getViewIndexCache(true).isEmpty());
        assertTrue(s.getViewIndexCache(false).isEmpty());

        // check view index cache
        stat.executeQuery("select * from v where id > 0").next();
        int size1 = s.getViewIndexCache(false).size();
        assertTrue(size1 > 0);
        assertTrue(s.getViewIndexCache(true).isEmpty());
        stat.executeQuery("select * from v where name = 'xyz'").next();
        int size2 = s.getViewIndexCache(false).size();
        assertTrue(size2 > size1);
        assertTrue(s.getViewIndexCache(true).isEmpty());

        // check we did not add anything to view cache if we run a sub-query
        stat.executeQuery("select * from (select * from test) where age = 17").next();
        int size3 = s.getViewIndexCache(false).size();
        assertEquals(size2, size3);
        assertTrue(s.getViewIndexCache(true).isEmpty());

        // check clear works
        s.clearViewIndexCache();
        assertTrue(s.getViewIndexCache(false).isEmpty());
        assertTrue(s.getViewIndexCache(true).isEmpty());

        // drop everything
        stat.execute("drop view v");
        stat.execute("drop table test");
        conn.close();
    }

    private void testInnerSelectWithRownum() throws SQLException {
        Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        stat.execute("drop table test if exists");
        stat.execute("create table test(id int primary key, name varchar(1))");
        stat.execute("insert into test(id, name) values(1, 'b'), (3, 'a')");
        ResultSet rs = stat.executeQuery(
                "select nr from (select row_number() over() as nr, " +
                "a.id as id from (select id from test order by name) as a) as b " +
                "where b.id = 1;");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());
        stat.execute("drop table test");
        conn.close();
    }

    private void testInnerSelectWithRange() throws SQLException {
        Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(
                "select x from (select x from (" +
                "select x from system_range(1, 5)) " +
                "where x > 2 and x < 4) where x = 3");
        assertTrue(rs.next());
        assertFalse(rs.next());
        rs = stat.executeQuery(
                "select x from (select x from (" +
                "select x from system_range(1, 5)) " +
                "where x = 3) where x > 2 and x < 4");
        assertTrue(rs.next());
        assertFalse(rs.next());
        conn.close();
    }

    private void testEmptyColumn() throws SQLException {
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        stat.execute("create table test(a int, b int)");
        stat.execute("create view test_view as select a, b from test");
        stat.execute("select * from test_view where a between 1 and 2 and b = 2");
        conn.close();
    }

    private void testChangeSchemaSearchPath() throws SQLException {
        deleteDb("view");
        Connection conn = getConnection("view;FUNCTIONS_IN_SCHEMA=TRUE");
        Statement stat = conn.createStatement();
        stat.execute("CREATE ALIAS X AS $$ int x() { return 1; } $$;");
        stat.execute("CREATE SCHEMA S");
        stat.execute("CREATE VIEW S.TEST AS SELECT X() FROM DUAL");
        stat.execute("SET SCHEMA=S");
        stat.execute("SET SCHEMA_SEARCH_PATH=S");
        stat.execute("SELECT * FROM TEST");
        conn.close();
    }

    private void testParameterizedView() throws SQLException {
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE Test(id INT AUTO_INCREMENT NOT NULL, " +
                "f1 VARCHAR NOT NULL, f2 VARCHAR NOT NULL)");
        stat.execute("INSERT INTO Test(f1, f2) VALUES ('value1','value2')");
        stat.execute("INSERT INTO Test(f1, f2) VALUES ('value1','value3')");
        PreparedStatement ps = conn.prepareStatement(
                "CREATE VIEW Test_View AS SELECT f2 FROM Test WHERE f1=?");
        ps.setString(1, "value1");
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, ps).
                executeUpdate();
        // ResultSet rs;
        // rs = stat.executeQuery("SELECT * FROM Test_View");
        // assertTrue(rs.next());
        // assertFalse(rs.next());
        // rs = stat.executeQuery("select VIEW_DEFINITION " +
        // "from information_schema.views " +
        // "where TABLE_NAME='TEST_VIEW'");
        // rs.next();
        // assertEquals("...", rs.getString(1));
        conn.close();
    }

    private void testCacheFunction(boolean deterministic) throws SQLException {
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        x = 8;
        stat.execute("CREATE ALIAS GET_X " +
                (deterministic ? "DETERMINISTIC" : "") +
                " FOR \"" + getClass().getName() + ".getX\"");
        stat.execute("CREATE VIEW V AS SELECT * FROM (SELECT GET_X())");
        ResultSet rs;
        rs = stat.executeQuery("SELECT * FROM V");
        rs.next();
        assertEquals(8, rs.getInt(1));
        x = 5;
        rs = stat.executeQuery("SELECT * FROM V");
        rs.next();
        assertEquals(deterministic ? 8 : 5, rs.getInt(1));
        conn.close();
    }

    /**
     * This method is called via reflection from the database.
     *
     * @return the static value x
     */
    public static int getX() {
        return x;
    }

    private void testCache() throws SQLException {
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        stat.execute("SET @X 8");
        stat.execute("CREATE VIEW V AS SELECT * FROM (SELECT @X)");
        ResultSet rs;
        rs = stat.executeQuery("SELECT * FROM V");
        rs.next();
        assertEquals(8, rs.getInt(1));
        stat.execute("SET @X 5");
        rs = stat.executeQuery("SELECT * FROM V");
        rs.next();
        assertEquals(5, rs.getInt(1));
        conn.close();
    }

    private void testInSelect() throws SQLException {
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key) as select 1");
        PreparedStatement prep = conn.prepareStatement(
                "select * from test t where t.id in " +
                "(select t2.id from test t2 where t2.id in (?, ?))");
        prep.setInt(1, 1);
        prep.setInt(2, 2);
        prep.execute();
        conn.close();
    }

    private void testUnionReconnect() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        stat.execute("create table t1(k smallint, ts timestamp(6))");
        stat.execute("create table t2(k smallint, ts timestamp(6))");
        stat.execute("create table t3(k smallint, ts timestamp(6))");
        stat.execute("create view v_max_ts as select " +
                "max(ts) from (select max(ts) as ts from t1 " +
                "union select max(ts) as ts from t2 " +
                "union select max(ts) as ts from t3)");
        stat.execute("create view v_test as select max(ts) as ts from t1 " +
                "union select max(ts) as ts from t2 " +
                "union select max(ts) as ts from t3");
        conn.close();
        conn = getConnection("view");
        stat = conn.createStatement();
        stat.execute("select * from v_max_ts");
        conn.close();
        deleteDb("view");
    }

    private void testManyViews() throws SQLException {
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement s = conn.createStatement();
        s.execute("create table t0(id int primary key)");
        s.execute("insert into t0 values(1), (2), (3)");
        for (int i = 0; i < 30; i++) {
            s.execute("create view t" + (i + 1) + " as select * from t" + i);
            s.execute("select * from t" + (i + 1));
            ResultSet rs = s.executeQuery(
                    "select count(*) from t" + (i + 1) + " where id=2");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
        conn.close();
        conn = getConnection("view");
        conn.close();
        deleteDb("view");
    }

    private void testReferenceView() throws SQLException {
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement s = conn.createStatement();
        s.execute("create table t0(id int primary key)");
        s.execute("create view t1 as select * from t0");
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, s).execute(
                "create table t2(id int primary key, " +
                "col1 int not null, foreign key (col1) references t1(id))");
        conn.close();
        deleteDb("view");
    }

    /**
     * Make sure that when we change a view, that change in reflected in other
     * sessions command cache.
     */
    private void testViewAlterAndCommandCache() throws SQLException {
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        stat.execute("create table t0(id int primary key)");
        stat.execute("create table t1(id int primary key)");
        stat.execute("insert into t0 values(0)");
        stat.execute("insert into t1 values(1)");
        stat.execute("create view v1 as select * from t0");
        ResultSet rs = stat.executeQuery("select * from v1");
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        stat.execute("create or replace view v1 as select * from t1");
        rs = stat.executeQuery("select * from v1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        conn.close();
        deleteDb("view");
    }

    /**
     * Make sure that the table constraint is still available when create a view
     * of other table.
     */
    private void testViewConstraintFromColumnExpression() throws SQLException {
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        stat.execute("create table t0(id1 int primary key CHECK ((ID1 % 2) = 0))");
        stat.execute("create table t1(id2 int primary key CHECK ((ID2 % 1) = 0))");
        stat.execute("insert into t0 values(0)");
        stat.execute("insert into t1 values(1)");
        stat.execute("create view v1 as select * from t0,t1");
        // Check with ColumnExpression
        ResultSet rs = stat.executeQuery(
                "select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = 'V1'");
        assertTrue(rs.next());
        assertEquals("ID1", rs.getString("COLUMN_NAME"));
        assertEquals("((ID1 % 2) = 0)", rs.getString("CHECK_CONSTRAINT"));
        assertTrue(rs.next());
        assertEquals("ID2", rs.getString("COLUMN_NAME"));
        assertEquals("((ID2 % 1) = 0)", rs.getString("CHECK_CONSTRAINT"));
        // Check with AliasExpression
        stat.execute("create view v2 as select ID1 key1,ID2 key2 from t0,t1");
        rs = stat.executeQuery("select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = 'V2'");
        assertTrue(rs.next());
        assertEquals("KEY1", rs.getString("COLUMN_NAME"));
        assertEquals("((KEY1 % 2) = 0)", rs.getString("CHECK_CONSTRAINT"));
        assertTrue(rs.next());
        assertEquals("KEY2", rs.getString("COLUMN_NAME"));
        assertEquals("((KEY2 % 1) = 0)", rs.getString("CHECK_CONSTRAINT"));
        // Check hide of constraint if column is an Operation
        stat.execute("create view v3 as select ID1 + 1 ID1, ID2 + 1 ID2 from t0,t1");
        rs = stat.executeQuery("select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = 'V3'");
        assertTrue(rs.next());
        assertEquals("ID1", rs.getString("COLUMN_NAME"));
        assertEquals("", rs.getString("CHECK_CONSTRAINT"));
        assertTrue(rs.next());
        assertEquals("ID2", rs.getString("COLUMN_NAME"));
        assertEquals("", rs.getString("CHECK_CONSTRAINT"));
        conn.close();
        deleteDb("view");
    }
}
