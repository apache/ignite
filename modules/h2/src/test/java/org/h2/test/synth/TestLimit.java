/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.test.TestBase;

/**
 * The LIMIT, OFFSET, maxRows.
 */
public class TestLimit extends TestBase {

    private Statement stat;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        // test.config.traceTest = true;
        test.test();
    }

    @Override
    public void test() throws Exception {
        deleteDb("limit");
        Connection conn = getConnection("limit");
        stat = conn.createStatement();
        stat.execute("create table test(id int) as " +
                "select x from system_range(1, 10)");
        for (int maxRows = 0; maxRows < 12; maxRows++) {
            stat.setMaxRows(maxRows);
            for (int limit = -2; limit < 12; limit++) {
                for (int offset = -2; offset < 12; offset++) {
                    int l = limit < 0 ? 10 : Math.min(10, limit);
                    for (int d = 0; d < 2; d++) {
                        int m = maxRows <= 0 ? 10 : Math.min(10, maxRows);
                        int expected = Math.min(m, l);
                        if (offset > 0) {
                            expected = Math.max(0, Math.min(10 - offset, expected));
                        }
                        String s = "select " + (d == 1 ? "distinct " : "") +
                                " * from test limit " + (limit == -2 ? "null" : limit) +
                                " offset " + (offset == -2 ? "null" : offset);
                        assertRow(expected, s);
                        String union = "(" + s + ") union (" + s + ")";
                        assertRow(expected, union);
                        m = maxRows <= 0 ? 20 : Math.min(20, maxRows);
                        if (offset > 0) {
                            l = Math.max(0, Math.min(10 - offset, l));
                        }
                        expected = Math.min(m, l * 2);
                        union = "(" + s + ") union all (" + s + ")";
                        assertRow(expected, union);
                        for (int unionLimit = -2; unionLimit < 5; unionLimit++) {
                            int e = unionLimit < 0 ? 20 : Math.min(20, unionLimit);
                            e = Math.min(expected, e);
                            String u = union + " limit " +
                                    (unionLimit == -2 ? "null" : unionLimit);
                            assertRow(e, u);
                        }
                    }
                }
            }
        }
        assertEquals(0, stat.executeUpdate("delete from test limit 0"));
        assertEquals(1, stat.executeUpdate("delete from test limit 1"));
        assertEquals(2, stat.executeUpdate("delete from test limit 2"));
        assertEquals(7, stat.executeUpdate("delete from test limit null"));
        stat.execute("insert into test select x from system_range(1, 10)");
        assertEquals(10, stat.executeUpdate("delete from test limit -1"));
        conn.close();
        deleteDb("limit");
    }

    private void assertRow(int expected, String sql) throws SQLException {
        try {
            assertResultRowCount(expected, stat.executeQuery(sql));
        } catch (AssertionError e) {
            stat.executeQuery(sql + " -- cache killer");
            throw e;
        }
    }

}
