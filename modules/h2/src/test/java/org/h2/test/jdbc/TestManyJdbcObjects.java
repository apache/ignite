/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;

/**
 * Tests the server by creating many JDBC objects (result sets and so on).
 */
public class TestManyJdbcObjects extends TestBase {

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
        testNestedResultSets();
        testManyConnections();
        testOneConnectionPrepare();
        deleteDb("manyObjects");
    }

    private void testNestedResultSets() throws SQLException {
        if (!config.networked) {
            return;
        }
        deleteDb("manyObjects");
        Connection conn = getConnection("manyObjects");
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rsTables = meta.getColumns(null, null, null, null);
        while (rsTables.next()) {
            meta.getExportedKeys(null, null, null);
            meta.getImportedKeys(null, null, null);
        }
        conn.close();
    }

    private void testManyConnections() throws SQLException {
        if (!config.networked || config.memory) {
            return;
        }
        // SERVER_CACHED_OBJECTS = 1000: connections = 20 (1250)
        // SERVER_CACHED_OBJECTS = 500: connections = 40
        // SERVER_CACHED_OBJECTS = 50: connections = 120
        deleteDb("manyObjects");
        int connCount = getSize(4, 40);
        Connection[] conn = new Connection[connCount];
        for (int i = 0; i < connCount; i++) {
            conn[i] = getConnection("manyObjects");
        }
        int len = getSize(50, 500);
        for (int j = 0; j < len; j++) {
            if ((j % 10) == 0) {
                trace("j=" + j);
            }
            for (int i = 0; i < connCount; i++) {
                conn[i].getMetaData().getSchemas().close();
            }
        }
        for (int i = 0; i < connCount; i++) {
            conn[i].close();
        }
    }

    private void testOneConnectionPrepare() throws SQLException {
        deleteDb("manyObjects");
        Connection conn = getConnection("manyObjects");
        PreparedStatement prep;
        Statement stat;
        int size = getSize(10, 1000);
        for (int i = 0; i < size; i++) {
            conn.getMetaData();
        }
        for (int i = 0; i < size; i++) {
            conn.createStatement();
        }
        stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        for (int i = 0; i < size; i++) {
            stat.executeQuery("SELECT * FROM TEST WHERE 1=0");
        }
        for (int i = 0; i < size; i++) {
            stat.executeQuery("SELECT * FROM TEST");
        }
        for (int i = 0; i < size; i++) {
            conn.prepareStatement("SELECT * FROM TEST");
        }
        prep = conn.prepareStatement("SELECT * FROM TEST WHERE 1=0");
        for (int i = 0; i < size; i++) {
            prep.executeQuery();
        }
        prep = conn.prepareStatement("SELECT * FROM TEST");
        for (int i = 0; i < size; i++) {
            prep.executeQuery();
        }
        conn.close();
    }

}
