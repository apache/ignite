/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.server;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;

/**
 * Tests remote JDBC access with nested loops.
 * This is not allowed in some databases.
 */
public class TestNestedLoop extends TestBase {

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
        deleteDb("nestedLoop");
        Connection conn = getConnection("nestedLoop");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int identity, name varchar)");
        int len = getSize(1010, 10000);
        for (int i = 0; i < len; i++) {
            stat.execute("insert into test(name) values('Hello World')");
        }
        ResultSet rs = stat.executeQuery("select id from test");
        stat.executeQuery("select id from test");
        assertThrows(ErrorCode.OBJECT_CLOSED, rs).next();
        rs = stat.executeQuery("select id from test");
        stat.close();
        assertThrows(ErrorCode.OBJECT_CLOSED, rs).next();
        stat = conn.createStatement();
        rs = stat.executeQuery("select id from test");
        Statement stat2 = conn.createStatement();
        while (rs.next()) {
            int id = rs.getInt(1);
            ResultSet rs2 = stat2.executeQuery("select * from test where id=" + id);
            while (rs2.next()) {
                assertEquals(id, rs2.getInt(1));
                assertEquals("Hello World", rs2.getString(2));
            }
            rs2 = stat2.executeQuery("select * from test where id=" + id);
            while (rs2.next()) {
                assertEquals(id, rs2.getInt(1));
                assertEquals("Hello World", rs2.getString(2));
            }
        }
        conn.close();
        deleteDb("nestedLoop");
    }

}
