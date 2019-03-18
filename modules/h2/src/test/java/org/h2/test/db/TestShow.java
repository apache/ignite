/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import org.h2.test.TestBase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Test of compatibility for the SHOW statement.
 */
public class TestShow extends TestBase {

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
        testPgCompatibility();
        testMysqlCompatibility();
    }

    private void testPgCompatibility() throws SQLException {
        try (Connection conn = getConnection("mem:pg")) {
            Statement stat = conn.createStatement();

            assertResult("UNICODE", stat, "SHOW CLIENT_ENCODING");
            assertResult("read committed", stat, "SHOW DEFAULT_TRANSACTION_ISOLATION");
            assertResult("read committed", stat, "SHOW TRANSACTION ISOLATION LEVEL");
            assertResult("ISO", stat, "SHOW DATESTYLE");
            assertResult("8.2.23", stat, "SHOW SERVER_VERSION");
            assertResult("UTF8", stat, "SHOW SERVER_ENCODING");
        }
    }

    private void testMysqlCompatibility() throws SQLException {
        try (Connection conn = getConnection("mem:pg")) {
            Statement stat = conn.createStatement();
            ResultSet rs;

            // show tables without a schema
            stat.execute("create table person(id int, name varchar)");
            rs = stat.executeQuery("SHOW TABLES");
            assertTrue(rs.next());
            assertEquals("PERSON", rs.getString(1));
            assertEquals("PUBLIC", rs.getString(2));
            assertFalse(rs.next());

            // show tables with a schema
            assertResultRowCount(1, stat.executeQuery("SHOW TABLES FROM PUBLIC"));

            // columns
            assertResultRowCount(2, stat.executeQuery("SHOW COLUMNS FROM person"));
        }
    }
}
