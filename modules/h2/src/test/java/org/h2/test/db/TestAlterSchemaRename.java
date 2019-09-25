/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Test ALTER SCHEMA RENAME statements.
 */
public class TestAlterSchemaRename extends TestBase {

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
        testTryToRenameSystemSchemas();
        testSimpleRename();
        testRenameToExistingSchema();
        testCrossSchemaViews();
        testAlias();
        conn.close();
        deleteDb(getTestName());
    }

    private void testTryToRenameSystemSchemas() throws SQLException {
        assertThrows(ErrorCode.SCHEMA_CAN_NOT_BE_DROPPED_1, stat).
                execute("alter schema information_schema rename to test_info");
        stat.execute("create sequence test_sequence");
        assertThrows(ErrorCode.SCHEMA_CAN_NOT_BE_DROPPED_1, stat).
                execute("alter schema public rename to test_schema");
    }

    private void testSimpleRename() throws SQLException {
        stat.execute("create schema s1");
        stat.execute("create table s1.tab(val int)");
        stat.execute("insert into s1.tab(val) values (3)");
        ResultSet rs = stat.executeQuery("select * from s1.tab");
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        stat.execute("alter schema s1 rename to s2");
        rs = stat.executeQuery("select * from s2.tab");
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        stat.execute("drop schema s2 cascade");
    }


    private void testRenameToExistingSchema() throws SQLException {
        stat.execute("create schema s1");
        stat.execute("create schema s2");
        assertThrows(ErrorCode.SCHEMA_ALREADY_EXISTS_1, stat).
                execute("alter schema s1 rename to s2");
        stat.execute("drop schema s1");
        stat.execute("drop schema s2");
    }


    private void testCrossSchemaViews() throws SQLException {
        stat.execute("create schema s1");
        stat.execute("create schema s2");
        stat.execute("create table s1.tab(val int)");
        stat.execute("insert into s1.tab(val) values (3)");
        stat.execute("create view s1.v1 as select * from s1.tab");
        stat.execute("create view s2.v1 as select val * 2 from s1.tab");
        stat.execute("alter schema s2 rename to s2_new");
        ResultSet rs = stat.executeQuery("select * from s1.v1");
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        rs = stat.executeQuery("select * from s2_new.v1");
        assertTrue(rs.next());
        assertEquals(6, rs.getInt(1));
        if (!config.memory) {
            conn.close();
            conn = getConnection(getTestName());
            stat = conn.createStatement();
            stat.executeQuery("select * from s2_new.v1");
        }
        stat.execute("drop schema s1 cascade");
        stat.execute("drop schema s2_new cascade");
    }

    /**
     * Check that aliases in the schema got moved
     */
    private void testAlias() throws SQLException {
        stat.execute("create schema s1");
        stat.execute("CREATE ALIAS S1.REVERSE AS $$ " +
                "String reverse(String s) {" +
                "   return new StringBuilder(s).reverse().toString();" +
                "} $$;");
        stat.execute("alter schema s1 rename to s2");
        ResultSet rs = stat.executeQuery("CALL S2.REVERSE('1234')");
        assertTrue(rs.next());
        assertEquals("4321", rs.getString(1));
        if (!config.memory) {
            conn.close();
            conn = getConnection(getTestName());
            stat = conn.createStatement();
            stat.executeQuery("CALL S2.REVERSE('1234')");
        }
        stat.execute("drop schema s2 cascade");
    }

}