/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.api.DatabaseEventListener;
import org.h2.api.ErrorCode;
import org.h2.test.TestBase;
import org.h2.tools.Server;

/**
 * Tests automatic embedded/server mode.
 */
public class TestAutoReconnect extends TestBase {

    private String url;
    private boolean autoServer;
    private Server server;
    private Connection connServer;
    private Connection conn;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    private void restart() throws SQLException, InterruptedException {
        if (autoServer) {
            if (connServer != null) {
                connServer.createStatement().execute("SHUTDOWN");
                connServer.close();
            }
            org.h2.Driver.load();
            connServer = getConnection(url);
        } else {
            server.stop();
            Thread.sleep(100); // try to prevent "port may be in use" error
            server.start();
        }
    }

    @Override
    public void test() throws Exception {
        testWrongUrl();
        autoServer = true;
        testReconnect();
        autoServer = false;
        testReconnect();
        deleteDb(getTestName());
    }

    private void testWrongUrl() throws Exception {
        deleteDb(getTestName());
        Server tcp = Server.createTcpServer().start();
        try {
            conn = getConnection("jdbc:h2:" + getBaseDir() +
                    "/" + getTestName() + ";AUTO_SERVER=TRUE");
            assertThrows(ErrorCode.DATABASE_ALREADY_OPEN_1, this).
                    getConnection("jdbc:h2:" + getBaseDir() +
                            "/" + getTestName() + ";OPEN_NEW=TRUE");
            assertThrows(ErrorCode.DATABASE_ALREADY_OPEN_1, this).
                    getConnection("jdbc:h2:" + getBaseDir() +
                            "/" + getTestName() + ";OPEN_NEW=TRUE");
            conn.close();

            conn = getConnection("jdbc:h2:tcp://localhost/" + getBaseDir() +
                    "/" + getTestName());
            assertThrows(ErrorCode.DATABASE_ALREADY_OPEN_1, this).
                    getConnection("jdbc:h2:" + getBaseDir() +
                            "/" + getTestName() + ";AUTO_SERVER=TRUE;OPEN_NEW=TRUE");
            conn.close();
        } finally {
            tcp.stop();
        }
    }

    private void testReconnect() throws Exception {
        deleteDb(getTestName());
        if (autoServer) {
            url = "jdbc:h2:" + getBaseDir() + "/" + getTestName() + ";" +
                "FILE_LOCK=SOCKET;" +
                "AUTO_SERVER=TRUE;OPEN_NEW=TRUE";
            restart();
        } else {
            server = Server.createTcpServer().start();
            int port = server.getPort();
            url = "jdbc:h2:tcp://localhost:" + port + "/" + getBaseDir() + "/" + getTestName() + ";" +
                "FILE_LOCK=SOCKET;AUTO_RECONNECT=TRUE";
        }

        // test the database event listener
        conn = getConnection(url + ";DATABASE_EVENT_LISTENER='" +
        MyDatabaseEventListener.class.getName() + "'");
        conn.close();

        Statement stat;

        conn = getConnection(url);
        restart();
        stat = conn.createStatement();
        restart();
        stat.execute("create table test(id identity, name varchar)");
        restart();
        PreparedStatement prep = conn.prepareStatement(
                "insert into test values(null, ?)");
        restart();
        prep.setString(1, "Hello");
        restart();
        prep.execute();
        restart();
        prep.setString(1, "World");
        restart();
        prep.execute();
        restart();
        ResultSet rs = stat.executeQuery("select * from test order by id");
        restart();
        assertTrue(rs.next());
        restart();
        assertEquals(1, rs.getInt(1));
        restart();
        assertEquals("Hello", rs.getString(2));
        restart();
        assertTrue(rs.next());
        restart();
        assertEquals(2, rs.getInt(1));
        restart();
        assertEquals("World", rs.getString(2));
        restart();
        assertFalse(rs.next());
        restart();
        stat.execute("SET @TEST 10");
        restart();
        rs = stat.executeQuery("CALL @TEST");
        rs.next();
        assertEquals(10, rs.getInt(1));
        stat.setFetchSize(10);
        restart();
        rs = stat.executeQuery("select * from system_range(1, 20)");
        restart();
        for (int i = 0;; i++) {
            try {
                boolean more = rs.next();
                if (!more) {
                    assertEquals(i, 20);
                    break;
                }
                restart();
                int x = rs.getInt(1);
                assertEquals(x, i + 1);
                if (i > 10) {
                    fail();
                }
            } catch (SQLException e) {
                if (i < 10) {
                    throw e;
                }
            }
        }
        restart();
        rs.close();

        conn.setAutoCommit(false);
        restart();
        assertThrows(ErrorCode.CONNECTION_BROKEN_1, conn.createStatement()).
                execute("select * from test");

        conn.close();
        if (autoServer) {
            connServer.close();
        } else {
            server.stop();
        }
    }

    /**
     * A database event listener used in this test.
     */
    public static final class MyDatabaseEventListener implements
            DatabaseEventListener {

        @Override
        public void closingDatabase() {
            // ignore
        }

        @Override
        public void exceptionThrown(SQLException e, String sql) {
            // ignore
        }

        @Override
        public void init(String u) {
            // ignore
        }

        @Override
        public void opened() {
            // ignore
        }

        @Override
        public void setProgress(int state, String name, int x, int max) {
            // ignore
        }
    }
}
