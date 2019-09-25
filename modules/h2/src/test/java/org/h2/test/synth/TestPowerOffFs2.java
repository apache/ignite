/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;
import org.h2.test.utils.FilePathDebug;
import org.h2.util.New;

/**
 * Tests that use the debug file system to simulate power failure.
 * This test runs many random operations and stops after some time.
 */
public class TestPowerOffFs2 extends TestBase {

    private static final String USER = "sa";
    private static final String PASSWORD = "sa";

    private FilePathDebug fs;

    private String url;
    private final ArrayList<Connection> connections = New.arrayList();
    private final ArrayList<String> tables = New.arrayList();

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
        fs = FilePathDebug.register();
        url = "jdbc:h2:debug:memFS:powerOffFs;FILE_LOCK=NO;" +
                "TRACE_LEVEL_FILE=0;WRITE_DELAY=0;CACHE_SIZE=32";
        for (int i = 0;; i++) {
            test(i);
        }
    }

    private void test(int x) throws SQLException {
        System.out.println("x:" + x);
        deleteDb("memFS:", null);
        try {
            testCrash(x);
            fail();
        } catch (SQLException e) {
            if (e.toString().indexOf("Simulated") < 0) {
                throw e;
            }
            for (Connection c : connections) {
                try {
                    Statement stat = c.createStatement();
                    stat.execute("shutdown immediately");
                } catch (Exception e2) {
                    // ignore
                }
                try {
                    c.close();
                } catch (Exception e2) {
                    // ignore
                }
            }
        }
        fs.setPowerOffCount(0);
        Connection conn;
        conn = openConnection();
        testConsistent(conn);
        conn.close();
    }

    private void testCrash(int x) throws SQLException {
        connections.clear();
        tables.clear();
        Random random = new Random(x);
        for (int i = 0;; i++) {
            if (i > 200 && connections.size() > 1 && tables.size() > 1) {
                fs.setPowerOffCount(100);
            }
            if (connections.size() < 1) {
                openConnection();
            }
            if (tables.size() < 1) {
                createTable(random);
            }
            int p = random.nextInt(100);
            if ((p -= 2) <= 0) {
                // 2%: open new connection
                if (connections.size() < 5) {
                    openConnection();
                }
            } else if ((p -= 1) <= 0) {
                // 1%: close connection
                if (connections.size() > 1) {
                    Connection conn = connections.remove(
                            random.nextInt(connections.size()));
                    conn.close();
                }
            } else if ((p -= 10) <= 0) {
                // 10% create table
                createTable(random);
            } else if ((p -= 20) <= 0) {
                // 20% large insert, delete, or update
                if (tables.size() > 0) {
                    Connection conn = connections.get(
                            random.nextInt(connections.size()));
                    Statement stat = conn.createStatement();
                    String table = tables.get(random.nextInt(tables.size()));
                    if (random.nextBoolean()) {
                        // 10% insert
                        stat.execute("INSERT INTO " + table +
                                "(NAME) SELECT 'Hello ' || X FROM SYSTEM_RANGE(0, 20)");
                    } else if (random.nextBoolean()) {
                        // 5% update
                        stat.execute("UPDATE " + table + " SET NAME='Hallo Welt'");
                    } else {
                        // 5% delete
                        stat.execute("DELETE FROM " + table);
                    }
                }
            } else if ((p -= 5) < 0) {
                // 5% truncate or drop table
                if (tables.size() > 0) {
                    Connection conn = connections.get(
                            random.nextInt(connections.size()));
                    Statement stat = conn.createStatement();
                    String table = tables.get(random.nextInt(tables.size()));
                    if (random.nextBoolean()) {
                        stat.execute("TRUNCATE TABLE " + table);
                    } else {
                        stat.execute("DROP TABLE " + table);
                        tables.remove(table);
                    }
                }
            } else if ((p -= 30) <= 0) {
                // 30% insert
                if (tables.size() > 0) {
                    Connection conn = connections.get(
                            random.nextInt(connections.size()));
                    Statement stat = conn.createStatement();
                    String table = tables.get(random.nextInt(tables.size()));
                    int spaces = random.nextInt(4) * 30;
                    if (random.nextInt(15) == 2) {
                        spaces *= 100;
                    }
                    int name = random.nextInt(20);
                    stat.execute("INSERT INTO " + table +
                            "(NAME) VALUES('" + name + "' || space( " + spaces + " ))");
                }
            } else {
                // 32% delete
                if (tables.size() > 0) {
                    Connection conn = connections.get(random.nextInt(connections.size()));
                    Statement stat = conn.createStatement();
                    String table = tables.get(random.nextInt(tables.size()));
                    stat.execute("DELETE FROM " + table +
                            " WHERE ID = SELECT MIN(ID) FROM " + table);
                }
            }
        }
    }

    private Connection openConnection() throws SQLException {
        Connection conn = DriverManager.getConnection(url, USER, PASSWORD);
        connections.add(conn);
        return conn;
    }

    private void createTable(Random random) throws SQLException {
        Connection conn = connections.get(random.nextInt(connections.size()));
        Statement stat = conn.createStatement();
        String table = "TEST" + random.nextInt(10);
        try {
            stat.execute("CREATE TABLE " + table + "(ID IDENTITY, NAME VARCHAR)");
            if (random.nextBoolean()) {
                stat.execute("CREATE INDEX IDX_" + table + " ON " + table + "(NAME)");
            }
            tables.add(table);
        } catch (SQLException e) {
            if (e.getErrorCode() == ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1) {
                if (!tables.contains(table)) {
                    tables.add(table);
                }
                // ok
            } else {
                throw e;
            }
        }
    }

    private static void testConsistent(Connection conn) throws SQLException {
        for (int i = 0; i < 20; i++) {
            Statement stat = conn.createStatement();
            try {
                ResultSet rs = stat.executeQuery("SELECT * FROM TEST" + i);
                while (rs.next()) {
                    rs.getLong("ID");
                    rs.getString("NAME");
                }
                rs = stat.executeQuery("SELECT * FROM TEST" + i + " ORDER BY ID");
                while (rs.next()) {
                    rs.getLong("ID");
                    rs.getString("NAME");
                }
            } catch (SQLException e) {
                if (e.getErrorCode() == ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1) {
                    // ok
                } else {
                    throw e;
                }
            }
        }
    }

}
