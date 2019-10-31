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
import org.h2.test.TestBase;

/**
 * Tests the ALLOW_LITERALS feature (protection against SQL injection).
 */
public class TestSQLInjection extends TestBase {

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
    public void test() throws SQLException {
        if (config.reopen) {
            return;
        }
        deleteDb("sqlInjection");
        reconnect("sqlInjection");
        stat.execute("DROP TABLE IF EXISTS USERS");
        stat.execute("CREATE TABLE USERS(NAME VARCHAR PRIMARY KEY, " +
                "PASSWORD VARCHAR, TYPE VARCHAR)");
        stat.execute("CREATE SCHEMA CONST");
        stat.execute("CREATE CONSTANT CONST.ACTIVE VALUE 'Active'");
        stat.execute("INSERT INTO USERS VALUES('James', '123456', CONST.ACTIVE)");
        assertTrue(checkPasswordInsecure("123456"));
        assertFalse(checkPasswordInsecure("abcdef"));
        assertTrue(checkPasswordInsecure("' OR ''='"));
        assertTrue(checkPasswordSecure("123456"));
        assertFalse(checkPasswordSecure("abcdef"));
        assertFalse(checkPasswordSecure("' OR ''='"));
        stat.execute("CALL 123");
        stat.execute("CALL 'Hello'");
        stat.execute("CALL $$Hello World$$");
        stat.execute("SET ALLOW_LITERALS NUMBERS");
        stat.execute("CALL 123");
        assertThrows(ErrorCode.LITERALS_ARE_NOT_ALLOWED, stat).
                execute("CALL 'Hello'");
        assertThrows(ErrorCode.LITERALS_ARE_NOT_ALLOWED, stat).
                execute("CALL $$Hello World$$");
        stat.execute("SET ALLOW_LITERALS NONE");
        try {
            checkPasswordInsecure("123456");
            fail();
        } catch (SQLException e) {
            assertKnownException(e);
        }
        assertTrue(checkPasswordSecure("123456"));
        assertFalse(checkPasswordSecure("' OR ''='"));
        conn.close();

        if (config.memory) {
            return;
        }

        reconnect("sqlInjection");

        try {
            checkPasswordInsecure("123456");
            fail();
        } catch (SQLException e) {
            assertKnownException(e);
        }
        assertTrue(checkPasswordSecure("123456"));
        assertFalse(checkPasswordSecure("' OR ''='"));
        conn.close();
        deleteDb("sqlInjection");
    }

    private boolean checkPasswordInsecure(String pwd) throws SQLException {
        String sql = "SELECT * FROM USERS WHERE PASSWORD='" + pwd + "'";
        ResultSet rs = conn.createStatement().executeQuery(sql);
        return rs.next();
    }

    private boolean checkPasswordSecure(String pwd) throws SQLException {
        String sql = "SELECT * FROM USERS WHERE PASSWORD=?";
        PreparedStatement prep = conn.prepareStatement(sql);
        prep.setString(1, pwd);
        ResultSet rs = prep.executeQuery();
        return rs.next();
    }

    private void reconnect(String name) throws SQLException {
        if (!config.memory) {
            if (conn != null) {
                conn.close();
                conn = null;
            }
        }
        if (conn == null) {
            conn = getConnection(name);
            stat = conn.createStatement();
        }
    }
}
