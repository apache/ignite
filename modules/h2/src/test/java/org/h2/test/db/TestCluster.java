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
import java.util.Properties;

import org.h2.api.ErrorCode;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.tools.CreateCluster;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Server;
import org.h2.util.JdbcUtils;

/**
 * Test the cluster feature.
 */
public class TestCluster extends TestBase {

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
        testClob();
        testRecover();
        testRollback();
        testCase();
        testClientInfo();
        testCreateClusterAtRuntime();
        testStartStopCluster();
    }

    private void testClob() throws SQLException {
        if (config.memory || config.networked || config.cipher != null) {
            return;
        }
        deleteFiles();

        org.h2.Driver.load();
        String user = getUser(), password = getPassword();
        Connection conn;
        Statement stat;

        Server n1 = org.h2.tools.Server.createTcpServer("-baseDir", getBaseDir() + "/node1").start();
        int port1 = n1.getPort();
        String url1 = getURL("jdbc:h2:tcp://localhost:" + port1 + "/test", false);

        conn = getConnection(url1, user, password);
        stat = conn.createStatement();
        stat.execute("create table t1(id int, name clob)");
        stat.execute("insert into t1 values(1, repeat('Hello', 50))");
        conn.close();

        Server n2 = org.h2.tools.Server.createTcpServer("-baseDir", getBaseDir() + "/node2").start();
        int port2 = n2.getPort();
        String url2 = getURL("jdbc:h2:tcp://localhost:" + port2 + "/test", false);

        String serverList = "localhost:" + port1 + ",localhost:" + port2;
        String urlCluster = getURL("jdbc:h2:tcp://" + serverList + "/test", true);
        CreateCluster.main("-urlSource", url1, "-urlTarget", url2,
                "-user", user, "-password", password, "-serverList",
                serverList);

        conn = getConnection(urlCluster, user, password);
        conn.close();

        n1.stop();
        n2.stop();
        deleteFiles();
    }

    private void testRecover() throws SQLException {
        if (config.memory || config.networked || config.cipher != null) {
            return;
        }
        deleteFiles();

        org.h2.Driver.load();
        String user = getUser(), password = getPassword();
        Connection conn;
        Statement stat;
        ResultSet rs;


        Server server1 = org.h2.tools.Server.createTcpServer("-baseDir", getBaseDir() + "/node1").start();
        int port1 = server1.getPort();
        Server server2 = org.h2.tools.Server.createTcpServer("-baseDir", getBaseDir() + "/node2").start();
        int port2 = server2.getPort();

        String url1 = getURL("jdbc:h2:tcp://localhost:" + port1 + "/test", true);
        String url2 = getURL("jdbc:h2:tcp://localhost:" + port2 + "/test", true);
        String serverList = "localhost:" + port1 + ",localhost:" + port2;
        String urlCluster = getURL("jdbc:h2:tcp://" + serverList + "/test", true);

        CreateCluster.main("-urlSource", url1, "-urlTarget", url2,
                "-user", user, "-password", password, "-serverList",
                serverList);

        conn = getConnection(urlCluster, user, password);
        stat = conn.createStatement();
        stat.execute("create table t1(id int, name varchar(30))");
        stat.execute("insert into t1 values(1, 'a'), (2, 'b'), (3, 'c')");
        rs = stat.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals(3, rs.getInt(1));

        server2.stop();
        DeleteDbFiles.main("-dir", getBaseDir() + "/node2", "-quiet");

        stat.execute("insert into t1 values(4, 'd'), (5, 'e')");
        rs = stat.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals(5, rs.getInt(1));

        server2 = org.h2.tools.Server.createTcpServer("-tcpPort",
                "" + port2 , "-baseDir", getBaseDir() + "/node2").start();
        CreateCluster.main("-urlSource", url1, "-urlTarget", url2,
                "-user", user, "-password", password, "-serverList",
                serverList);

        conn.close();

        conn = getConnection(urlCluster, user, password);
        stat = conn.createStatement();
        rs = stat.executeQuery("select count(*) from t1");
        rs.next();
        assertEquals(5, rs.getInt(1));
        conn.close();

        server1.stop();
        server2.stop();
        deleteFiles();
    }

    private void testRollback() throws SQLException {
        if (config.memory || config.networked || config.cipher != null) {
            return;
        }
        deleteFiles();

        org.h2.Driver.load();
        String user = getUser(), password = getPassword();
        Connection conn;
        Statement stat;
        ResultSet rs;

        Server n1 = org.h2.tools.Server.createTcpServer("-baseDir", getBaseDir() + "/node1").start();
        int port1 = n1.getPort();
        Server n2 = org.h2.tools.Server.createTcpServer("-baseDir", getBaseDir() + "/node2").start();
        int port2 = n2.getPort();

        String url1 = getURL("jdbc:h2:tcp://localhost:" + port1 + "/test", true);
        String url2 = getURL("jdbc:h2:tcp://localhost:" + port2 + "/test", true);
        String serverList = "localhost:" + port1 + ",localhost:" + port2;
        String urlCluster = getURL("jdbc:h2:tcp://" + serverList + "/test", true);

        CreateCluster.main("-urlSource", url1, "-urlTarget", url2,
                "-user", user, "-password", password, "-serverList",
                serverList);

        conn = getConnection(urlCluster, user, password);
        stat = conn.createStatement();
        assertTrue(conn.getAutoCommit());
        stat.execute("create table test(id int, name varchar)");
        assertTrue(conn.getAutoCommit());
        stat.execute("set autocommit false");
        // issue 259
        // assertFalse(conn.getAutoCommit());
        conn.setAutoCommit(false);
        assertFalse(conn.getAutoCommit());
        stat.execute("insert into test values(1, 'Hello')");
        stat.execute("rollback");
        rs = stat.executeQuery("select * from test order by id");
        assertFalse(rs.next());
        conn.close();

        n1.stop();
        n2.stop();
        deleteFiles();
    }

    private void testCase() throws SQLException {
        if (config.memory || config.networked || config.cipher != null) {
            return;
        }
        deleteFiles();

        org.h2.Driver.load();
        String user = getUser(), password = getPassword();
        Connection conn;
        Statement stat;
        ResultSet rs;


        Server n1 = org.h2.tools.Server.createTcpServer("-baseDir", getBaseDir() + "/node1").start();
        int port1 = n1.getPort();
        Server n2 = org.h2.tools.Server.createTcpServer("-baseDir", getBaseDir() + "/node2").start();
        int port2 = n2.getPort();
        String serverList = "localhost:" + port1 + ",localhost:" + port2;
        String url1 = getURL("jdbc:h2:tcp://localhost:" + port1 + "/test", true);
        String url2 = getURL("jdbc:h2:tcp://localhost:" + port2 + "/test", true);
        String urlCluster = getURL("jdbc:h2:tcp://" + serverList + "/test", true);

        CreateCluster.main("-urlSource", url1, "-urlTarget", url2,
                "-user", user, "-password", password, "-serverList",
                serverList);

        conn = getConnection(urlCluster, user, password);
        stat = conn.createStatement();
        assertTrue(conn.getAutoCommit());
        stat.execute("create table test(name int)");
        assertTrue(conn.getAutoCommit());
        stat.execute("insert into test values(1)");
        conn.setAutoCommit(false);
        assertFalse(conn.getAutoCommit());
        stat.execute("insert into test values(2)");
        assertFalse(conn.getAutoCommit());
        conn.rollback();
        rs = stat.executeQuery("select * from test order by name");
        assertTrue(rs.next());
        assertFalse(rs.next());
        conn.close();

        // stop server 2, and test if only one server is available
        n2.stop();

        conn = getConnection(urlCluster, user, password);
        stat = conn.createStatement();
        rs = stat.executeQuery("select * from test");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        conn.close();

        n1.stop();
        deleteFiles();
    }

    private void testClientInfo() throws SQLException {
        if (config.memory || config.networked || config.cipher != null) {
            return;
        }
        deleteFiles();

        org.h2.Driver.load();
        String user = getUser(), password = getPassword();
        Connection conn;


        Server n1 = org.h2.tools.Server.createTcpServer("-baseDir", getBaseDir() + "/node1").start();
        int port1 = n1.getPort();
        Server n2 = org.h2.tools.Server.createTcpServer("-baseDir", getBaseDir() + "/node2").start();
        int port2 = n2.getPort();

        String serverList = "localhost:" + port1 + ",localhost:" + port2;
        String url1 = getURL("jdbc:h2:tcp://localhost:" + port1 + "/test", true);
        String url2 = getURL("jdbc:h2:tcp://localhost:" + port2 + "/test", true);
        String urlCluster = getURL("jdbc:h2:tcp://" + serverList + "/test", true);

        CreateCluster.main("-urlSource", url1, "-urlTarget", url2,
                "-user", user, "-password", password, "-serverList",
                serverList);

        conn = getConnection(urlCluster, user, password);
        Properties p = conn.getClientInfo();
        assertEquals("2", p.getProperty("numServers"));
        assertEquals("127.0.0.1:" + port1, p.getProperty("server0"));
        assertEquals("127.0.0.1:" + port2, p.getProperty("server1"));

        assertEquals("2", conn.getClientInfo("numServers"));
        assertEquals("127.0.0.1:" + port1, conn.getClientInfo("server0"));
        assertEquals("127.0.0.1:" + port2, conn.getClientInfo("server1"));
        conn.close();

        // stop server 2, and test if only one server is available
        n2.stop();

        conn = getConnection(urlCluster, user, password);
        p = conn.getClientInfo();

        assertEquals("1", p.getProperty("numServers"));
        assertEquals("127.0.0.1:" + port1, p.getProperty("server0"));
        assertEquals("1", conn.getClientInfo("numServers"));
        assertEquals("127.0.0.1:" + port1, conn.getClientInfo("server0"));
        conn.close();

        n1.stop();
        deleteFiles();
    }

    private void testCreateClusterAtRuntime() throws SQLException {
        if (config.memory || config.networked || config.cipher != null) {
            return;
        }
        deleteFiles();

        org.h2.Driver.load();
        String user = getUser(), password = getPassword();
        Connection conn;
        Statement stat;
        int len = 10;

        // initialize the database
        Server n1 = org.h2.tools.Server.createTcpServer("-baseDir", getBaseDir() + "/node1").start();
        int port1 = n1.getPort();
        String url1 = getURL("jdbc:h2:tcp://localhost:" + port1 + "/test", false);
        conn = getConnection(url1, user, password);
        stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar) as " +
                "select x, 'Data' || x from system_range(0, " + (len - 1) + ")");
        stat.execute("create user test password 'test'");
        stat.execute("grant all on test to test");

        // start the second server
        Server n2 = org.h2.tools.Server.createTcpServer("-baseDir", getBaseDir() + "/node2").start();
        int port2 = n2.getPort();
        String url2 = getURL("jdbc:h2:tcp://localhost:" + port2 + "/test", false);

        // copy the database and initialize the cluster
        String serverList = "localhost:" + port1 + ",localhost:" + port2;
        CreateCluster.main("-urlSource", url1, "-urlTarget", url2,
                "-user", user, "-password", password, "-serverList",
                serverList);

        // check the original connection is closed
        assertThrows(ErrorCode.CONNECTION_BROKEN_1, stat).
                execute("select * from test");
        JdbcUtils.closeSilently(conn);

        // test the cluster connection
        String urlCluster = getURL("jdbc:h2:tcp://" + serverList + "/test", false);
        Connection connApp = getConnection(urlCluster +
                ";AUTO_RECONNECT=TRUE", user, password);
        check(connApp, len, "'" + serverList + "'");

        // delete the rows, but don't commit
        connApp.setAutoCommit(false);
        connApp.createStatement().execute("delete from test");

        // stop server 2, and test if only one server is available
        n2.stop();

        // rollback the transaction
        connApp.createStatement().executeQuery("select count(*) from test");
        connApp.rollback();
        check(connApp, len, "''");
        connApp.setAutoCommit(true);

        // re-create the cluster
        n2 = org.h2.tools.Server.createTcpServer("-tcpPort", "" + port2,
                "-baseDir", getBaseDir() + "/node2").start();
        CreateCluster.main("-urlSource", url1, "-urlTarget", url2,
                "-user", user, "-password", password, "-serverList",
                serverList);

        // test the cluster connection
        check(connApp, len, "'" + serverList + "'");
        connApp.close();

        // test a non-admin user
        String user2 = "test", password2 = getPassword("test");
        connApp = getConnection(urlCluster, user2, password2);
        check(connApp, len, "'" + serverList + "'");
        connApp.close();

        n1.stop();

        // test non-admin cluster connection if only one server runs
        Connection connApp2 = getConnection(urlCluster +
                ";AUTO_RECONNECT=TRUE", user2, password2);
        check(connApp2, len, "''");
        connApp2.close();
        // test non-admin cluster connection if only one server runs
        connApp2 = getConnection(urlCluster +
                ";AUTO_RECONNECT=TRUE", user2, password2);
        check(connApp2, len, "''");
        connApp2.close();

        n2.stop();
        deleteFiles();
    }

    private void testStartStopCluster() throws SQLException {
        if (config.memory || config.networked || config.cipher != null) {
            return;
        }
        int port1 = 9193, port2 = 9194;
        String serverList = "localhost:" + port1 + ",localhost:" + port2;
        deleteFiles();

        // initialize the database
        Connection conn;
        org.h2.Driver.load();

        String urlNode1 = getURL("node1/test", true);
        String urlNode2 = getURL("node2/test", true);
        String user = getUser(), password = getPassword();
        conn = getConnection(urlNode1, user, password);
        Statement stat;
        stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?, ?)");
        int len = getSize(10, 1000);
        for (int i = 0; i < len; i++) {
            prep.setInt(1, i);
            prep.setString(2, "Data" + i);
            prep.executeUpdate();
        }
        check(conn, len, "''");
        conn.close();

        // copy the database and initialize the cluster
        CreateCluster.main("-urlSource", urlNode1, "-urlTarget",
                urlNode2, "-user", user, "-password", password, "-serverList",
                serverList);

        // start both servers
        Server n1 = org.h2.tools.Server.createTcpServer("-tcpPort", "" +
                port1, "-baseDir", getBaseDir() + "/node1").start();
        Server n2 = org.h2.tools.Server.createTcpServer("-tcpPort", "" +
                port2, "-baseDir", getBaseDir() + "/node2").start();

        // try to connect in standalone mode - should fail
        // should not be able to connect in standalone mode
        assertThrows(ErrorCode.CLUSTER_ERROR_DATABASE_RUNS_CLUSTERED_1, this).
                getConnection("jdbc:h2:tcp://localhost:"+port1+"/test", user, password);
        assertThrows(ErrorCode.CLUSTER_ERROR_DATABASE_RUNS_CLUSTERED_1, this).
                getConnection("jdbc:h2:tcp://localhost:"+port2+"/test", user, password);

        // test a cluster connection
        conn = getConnection("jdbc:h2:tcp://" + serverList + "/test", user, password);
        check(conn, len, "'"+serverList+"'");
        conn.close();

        // stop server 2, and test if only one server is available
        n2.stop();
        conn = getConnection("jdbc:h2:tcp://" + serverList + "/test", user, password);
        check(conn, len, "''");
        conn.close();
        conn = getConnection("jdbc:h2:tcp://" + serverList + "/test", user, password);
        check(conn, len, "''");
        conn.close();

        // disable the cluster
        conn = getConnection("jdbc:h2:tcp://localhost:"+
                port1+"/test;CLUSTER=''", user, password);
        conn.close();
        n1.stop();

        // re-create the cluster
        DeleteDbFiles.main("-dir", getBaseDir() + "/node2", "-quiet");
        CreateCluster.main("-urlSource", urlNode1, "-urlTarget",
                urlNode2, "-user", user, "-password", password, "-serverList",
                serverList);
        n1 = org.h2.tools.Server.createTcpServer("-tcpPort", "" +
                port1, "-baseDir", getBaseDir() + "/node1").start();
        n2 = org.h2.tools.Server.createTcpServer("-tcpPort", "" +
                port2, "-baseDir", getBaseDir() + "/node2").start();

        conn = getConnection("jdbc:h2:tcp://" + serverList + "/test", user, password);
        stat = conn.createStatement();
        stat.execute("CREATE TABLE BOTH(ID INT)");

        n1.stop();

        stat.execute("CREATE TABLE A(ID INT)");
        conn.close();
        n2.stop();

        n1 = org.h2.tools.Server.createTcpServer("-tcpPort", "" +
                port1, "-baseDir", getBaseDir() + "/node1").start();
        conn = getConnection("jdbc:h2:tcp://localhost:"+
                port1+"/test;CLUSTER=''", user, password);
        check(conn, len, "''");
        conn.close();
        n1.stop();

        n2 = org.h2.tools.Server.createTcpServer("-tcpPort", "" +
                port2, "-baseDir", getBaseDir() + "/node2").start();
        conn = getConnection("jdbc:h2:tcp://localhost:" +
                port2 + "/test;CLUSTER=''", user, password);
        check(conn, len, "''");
        conn.createStatement().execute("SELECT * FROM A");
        conn.close();
        n2.stop();
        deleteFiles();
    }

    private void deleteFiles() throws SQLException {
        DeleteDbFiles.main("-dir", getBaseDir() + "/node1", "-quiet");
        DeleteDbFiles.main("-dir", getBaseDir() + "/node2", "-quiet");
        FileUtils.delete(getBaseDir() + "/node1");
        FileUtils.delete(getBaseDir() + "/node2");
    }

    private void check(Connection conn, int len, String expectedCluster)
            throws SQLException {
        PreparedStatement prep = conn.prepareStatement("SELECT * FROM TEST WHERE ID=?");
        for (int i = 0; i < len; i++) {
            prep.setInt(1, i);
            ResultSet rs = prep.executeQuery();
            rs.next();
            assertEquals("Data" + i, rs.getString(2));
            assertFalse(rs.next());
        }
        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT VALUE FROM INFORMATION_SCHEMA.SETTINGS WHERE NAME='CLUSTER'");
        String cluster = rs.next() ? rs.getString(1) : "''";
        assertEquals(expectedCluster, cluster);
    }

}
