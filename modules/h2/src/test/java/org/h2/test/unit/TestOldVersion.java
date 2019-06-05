/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Properties;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;
import org.h2.tools.Server;

/**
 * Tests the compatibility with older versions
 */
public class TestOldVersion extends TestBase {

    private ClassLoader cl;
    private Driver driver;

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
        if (config.mvStore) {
            return;
        }
        cl = getClassLoader("file:ext/h2-1.2.127.jar");
        driver = getDriver(cl);
        if (driver == null) {
            println("not found: ext/h2-1.2.127.jar - test skipped");
            return;
        }
        Connection conn = driver.connect("jdbc:h2:mem:", null);
        assertEquals("1.2.127 (2010-01-15)", conn.getMetaData()
                .getDatabaseProductVersion());
        conn.close();
        testLobInFiles();
        testOldClientNewServer();
    }

    private void testLobInFiles() throws Exception {
        deleteDb("oldVersion");
        Connection conn;
        Statement stat;
        conn = driver.connect("jdbc:h2:" + getBaseDir() + "/oldVersion", null);
        stat = conn.createStatement();
        stat.execute("create table test(id int primary key, b blob, c clob)");
        PreparedStatement prep = conn
                .prepareStatement("insert into test values(?, ?, ?)");
        prep.setInt(1, 0);
        prep.setNull(2, Types.BLOB);
        prep.setNull(3, Types.CLOB);
        prep.execute();
        prep.setInt(1, 1);
        prep.setBytes(2, new byte[0]);
        prep.setString(3, "");
        prep.execute();
        prep.setInt(1, 2);
        prep.setBytes(2, new byte[5]);
        prep.setString(3, "\u1234\u1234\u1234\u1234\u1234");
        prep.execute();
        prep.setInt(1, 3);
        prep.setBytes(2, new byte[100000]);
        prep.setString(3, new String(new char[100000]));
        prep.execute();
        conn.close();
        conn = DriverManager.getConnection("jdbc:h2:" + getBaseDir() +
                "/oldVersion", new Properties());
        stat = conn.createStatement();
        checkResult(stat.executeQuery("select * from test order by id"));
        stat.execute("create table test2 as select * from test");
        checkResult(stat.executeQuery("select * from test2 order by id"));
        stat.execute("delete from test");
        conn.close();
    }

    private void checkResult(ResultSet rs) throws SQLException {
        rs.next();
        assertEquals(0, rs.getInt(1));
        assertEquals(null, rs.getBytes(2));
        assertEquals(null, rs.getString(3));
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals(new byte[0], rs.getBytes(2));
        assertEquals("", rs.getString(3));
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals(new byte[5], rs.getBytes(2));
        assertEquals("\u1234\u1234\u1234\u1234\u1234", rs.getString(3));
        rs.next();
        assertEquals(3, rs.getInt(1));
        assertEquals(new byte[100000], rs.getBytes(2));
        assertEquals(new String(new char[100000]), rs.getString(3));
    }

    private void testOldClientNewServer() throws Exception {
        Server server = org.h2.tools.Server.createTcpServer();
        server.start();
        int port = server.getPort();
        assertThrows(ErrorCode.DRIVER_VERSION_ERROR_2, driver).connect(
                "jdbc:h2:tcp://localhost:" + port + "/mem:test", null);
        server.stop();

        Class<?> serverClass = cl.loadClass("org.h2.tools.Server");
        Method m;
        m = serverClass.getMethod("createTcpServer", String[].class);
        Object serverOld = m.invoke(null, new Object[] { new String[] {
                "-tcpPort", "" + port } });
        m = serverOld.getClass().getMethod("start");
        m.invoke(serverOld);
        Connection conn;
        conn = org.h2.Driver.load().connect("jdbc:h2:mem:", null);
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("call 1");
        rs.next();
        assertEquals(1, rs.getInt(1));
        conn.close();
        m = serverOld.getClass().getMethod("stop");
        m.invoke(serverOld);
    }

    private static ClassLoader getClassLoader(String jarFile) throws Exception {
        URL[] urls = { new URL(jarFile) };
        return new URLClassLoader(urls, null);
    }

    private static Driver getDriver(ClassLoader cl) throws Exception {
        Class<?> driverClass;
        try {
            driverClass = cl.loadClass("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            return null;
        }
        Method m = driverClass.getMethod("load");
        Driver driver = (Driver) m.invoke(null);
        return driver;
    }

}
