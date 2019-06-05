/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.h2.test.TestBase;

/**
 * Test for check compatibility with PostgreSQL function string_agg()
 */
public class TestStringAggCompatibility extends TestBase {

    private Connection conn;

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
        prepareDb();
        testWhenOrderByMissing();
        testWithOrderBy();
        conn.close();
    }

    private void testWithOrderBy() throws SQLException {
        ResultSet result = query(
                "select string_agg(b, ', ' order by b desc) from stringAgg group by a; ");

        assertTrue(result.next());
        assertEquals("3, 2, 1", result.getString(1));
    }

    private void testWhenOrderByMissing() throws SQLException {
        ResultSet result = query("select string_agg(b, ', ') from stringAgg group by a; ");

        assertTrue(result.next());
        assertEquals("1, 2, 3", result.getString(1));
    }

    private ResultSet query(String q) throws SQLException {
        PreparedStatement st = conn.prepareStatement(q);

        st.execute();

        return st.getResultSet();
    }

    private void prepareDb() throws SQLException {
        exec("create table stringAgg(\n" +
                " a int not null,\n" +
                " b varchar(50) not null\n" +
                ");");

        exec("insert into stringAgg values(1, '1')");
        exec("insert into stringAgg values(1, '2')");
        exec("insert into stringAgg values(1, '3')");

    }

    private void exec(String sql) throws SQLException {
        conn.prepareStatement(sql).execute();
    }
}
