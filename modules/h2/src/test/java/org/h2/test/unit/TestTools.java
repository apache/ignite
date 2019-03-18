/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.awt.Button;
import java.awt.HeadlessException;
import java.awt.event.ActionEvent;
import java.awt.event.MouseEvent;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.h2.api.ErrorCode;
import org.h2.engine.SysProperties;
import org.h2.store.FileLister;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.test.trace.Player;
import org.h2.test.utils.AssertThrows;
import org.h2.tools.Backup;
import org.h2.tools.ChangeFileEncryption;
import org.h2.tools.Console;
import org.h2.tools.ConvertTraceFile;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Recover;
import org.h2.tools.Restore;
import org.h2.tools.RunScript;
import org.h2.tools.Script;
import org.h2.tools.Server;
import org.h2.tools.SimpleResultSet;
import org.h2.tools.SimpleResultSet.SimpleArray;
import org.h2.util.JdbcUtils;
import org.h2.util.Task;
import org.h2.value.ValueUuid;

/**
 * Tests the database tools.
 */
public class TestTools extends TestBase {

    private static String lastUrl;
    private Server server;
    private List<Server> remainingServers = new ArrayList<>(3);

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
        if (config.networked) {
            return;
        }
        DeleteDbFiles.execute(getBaseDir(), null, true);
        org.h2.Driver.load();
        testSimpleResultSet();
        testTcpServerWithoutPort();
        testConsole();
        testJdbcDriverUtils();
        testWrongServer();
        testDeleteFiles();
        testScriptRunscriptLob();
        testServerMain();
        testRemove();
        testConvertTraceFile();
        testManagementDb();
        testChangeFileEncryption(false);
        if (!config.splitFileSystem) {
            testChangeFileEncryption(true);
        }
        testChangeFileEncryptionWithWrongPassword();
        testServer();
        testScriptRunscript();
        testBackupRestore();
        testRecover();
        FileUtils.delete(getBaseDir() + "/b2.sql");
        FileUtils.delete(getBaseDir() + "/b2.sql.txt");
        FileUtils.delete(getBaseDir() + "/b2.zip");
    }

    private void testTcpServerWithoutPort() throws Exception {
        Server s1 = Server.createTcpServer().start();
        Server s2 = Server.createTcpServer().start();
        assertTrue(s1.getPort() != s2.getPort());
        s1.stop();
        s2.stop();
        s1 = Server.createTcpServer("-tcpPort", "9123").start();
        assertEquals(9123, s1.getPort());
        createClassProxy(Server.class);
        assertThrows(ErrorCode.EXCEPTION_OPENING_PORT_2,
                Server.createTcpServer("-tcpPort", "9123")).start();
        s1.stop();
    }

    private void testConsole() throws Exception {
        String old = System.getProperty(SysProperties.H2_BROWSER);
        Console c = new Console();
        c.setOut(new PrintStream(new ByteArrayOutputStream()));
        try {

            // start including browser
            lastUrl = "-";
            System.setProperty(SysProperties.H2_BROWSER, "call:" +
                    TestTools.class.getName() + ".openBrowser");
            c.runTool("-web", "-webPort", "9002", "-tool", "-browser", "-tcp",
                    "-tcpPort", "9003", "-pg", "-pgPort", "9004");
            assertContains(lastUrl, ":9002");
            shutdownConsole(c);

            // check if starting the browser works
            c.runTool("-web", "-webPort", "9002", "-tool");
            lastUrl = "-";
            c.actionPerformed(new ActionEvent(this, 0, "console"));
            assertContains(lastUrl, ":9002");
            lastUrl = "-";
            // double-click prevention is 100 ms
            Thread.sleep(200);
            try {
                MouseEvent me = new MouseEvent(new Button(), 0, 0, 0, 0, 0, 0,
                        false, MouseEvent.BUTTON1);
                c.mouseClicked(me);
                assertContains(lastUrl, ":9002");
                lastUrl = "-";
                // no delay - ignore because it looks like a double click
                c.mouseClicked(me);
                assertEquals("-", lastUrl);
                // open the window
                c.actionPerformed(new ActionEvent(this, 0, "status"));
                c.actionPerformed(new ActionEvent(this, 0, "exit"));

                // check if the service was stopped
                c.runTool("-webPort", "9002");

            } catch (HeadlessException e) {
                // ignore
            }

            shutdownConsole(c);

            // trying to use the same port for two services should fail,
            // but also stop the first service
            createClassProxy(c.getClass());
            assertThrows(ErrorCode.EXCEPTION_OPENING_PORT_2, c).runTool("-web",
                    "-webPort", "9002", "-tcp", "-tcpPort", "9002");
            c.runTool("-web", "-webPort", "9002");

        } finally {
            if (old != null) {
                System.setProperty(SysProperties.H2_BROWSER, old);
            } else {
                System.clearProperty(SysProperties.H2_BROWSER);
            }
            shutdownConsole(c);
        }
    }

    private static void shutdownConsole(Console c) {
        c.shutdown();
        if (Thread.currentThread().isInterrupted()) {
            // Clear interrupted state so test can continue its work safely
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                // Ignore
            }
        }
    }

    /**
     * This method is called via reflection.
     *
     * @param url the browser url
     */
    public static void openBrowser(String url) {
        lastUrl = url;
    }

    private void testSimpleResultSet() throws Exception {

        SimpleResultSet rs;
        rs = new SimpleResultSet();
        rs.addColumn(null, 0, 0, 0);
        rs.addRow(1);
        createClassProxy(rs.getClass());
        assertThrows(IllegalStateException.class, rs).
                addColumn(null, 0, 0, 0);
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());

        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("1", rs.getString(1));
        assertEquals("1", rs.getString("C1"));
        assertFalse(rs.wasNull());
        assertEquals("C1", rs.getMetaData().getColumnLabel(1));
        assertEquals("C1", rs.getColumnName(1));
        assertEquals(
                ResultSetMetaData.columnNullableUnknown,
                rs.getMetaData().isNullable(1));
        assertFalse(rs.getMetaData().isAutoIncrement(1));
        assertTrue(rs.getMetaData().isCaseSensitive(1));
        assertFalse(rs.getMetaData().isCurrency(1));
        assertFalse(rs.getMetaData().isDefinitelyWritable(1));
        assertTrue(rs.getMetaData().isReadOnly(1));
        assertTrue(rs.getMetaData().isSearchable(1));
        assertTrue(rs.getMetaData().isSigned(1));
        assertFalse(rs.getMetaData().isWritable(1));
        assertEquals(null, rs.getMetaData().getCatalogName(1));
        assertEquals(null, rs.getMetaData().getColumnClassName(1));
        assertEquals("NULL", rs.getMetaData().getColumnTypeName(1));
        assertEquals(null, rs.getMetaData().getSchemaName(1));
        assertEquals(null, rs.getMetaData().getTableName(1));
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, rs.getHoldability());
        assertEquals(1, rs.getColumnCount());

        rs = new SimpleResultSet();
        rs.setAutoClose(false);

        rs.addColumn("a", Types.BIGINT, 0, 0);
        rs.addColumn("b", Types.BINARY, 0, 0);
        rs.addColumn("c", Types.BOOLEAN, 0, 0);
        rs.addColumn("d", Types.DATE, 0, 0);
        rs.addColumn("e", Types.DECIMAL, 0, 0);
        rs.addColumn("f", Types.FLOAT, 0, 0);
        rs.addColumn("g", Types.VARCHAR, 0, 0);
        rs.addColumn("h", Types.ARRAY, 0, 0);
        rs.addColumn("i", Types.TIME, 0, 0);
        rs.addColumn("j", Types.TIMESTAMP, 0, 0);
        rs.addColumn("k", Types.CLOB, 0, 0);
        rs.addColumn("l", Types.BLOB, 0, 0);

        Date d = Date.valueOf("2001-02-03");
        byte[] b = {(byte) 0xab};
        Object[] a = {1, 2};
        Time t = Time.valueOf("10:20:30");
        Timestamp ts = Timestamp.valueOf("2002-03-04 10:20:30");
        Clob clob = new SimpleClob("Hello World");
        Blob blob = new SimpleBlob(new byte[]{(byte) 1, (byte) 2});
        rs.addRow(1, b, true, d, "10.3", Math.PI, "-3", a, t, ts, clob, blob);
        rs.addRow(BigInteger.ONE, null, true, null, BigDecimal.ONE, 1d, null, null, null, null, null);
        rs.addRow(BigInteger.ZERO, null, false, null, BigDecimal.ZERO, 0d, null, null, null, null, null);
        rs.addRow(null, null, null, null, null, null, null, null, null, null, null);

        rs.next();

        assertEquals(1, rs.getLong(1));
        assertEquals((byte) 1, rs.getByte(1));
        assertEquals((short) 1, rs.getShort(1));
        assertEquals(1, rs.getLong("a"));
        assertEquals((byte) 1, rs.getByte("a"));
        assertEquals(1, rs.getInt("a"));
        assertEquals((short) 1, rs.getShort("a"));
        assertTrue(rs.getObject(1).getClass() == Integer.class);
        assertTrue(rs.getObject("a").getClass() == Integer.class);
        assertTrue(rs.getBoolean(1));

        assertEquals(b, rs.getBytes(2));
        assertEquals(b, rs.getBytes("b"));

        assertTrue(rs.getBoolean(3));
        assertTrue(rs.getBoolean("c"));
        assertEquals(d.getTime(), rs.getDate(4).getTime());
        assertEquals(d.getTime(), rs.getDate("d").getTime());

        assertTrue(new BigDecimal("10.3").equals(rs.getBigDecimal(5)));
        assertTrue(new BigDecimal("10.3").equals(rs.getBigDecimal("e")));
        assertEquals(10.3, rs.getDouble(5));
        assertEquals((float) 10.3, rs.getFloat(5));

        assertTrue(Math.PI == rs.getDouble(6));
        assertTrue(Math.PI == rs.getDouble("f"));
        assertTrue((float) Math.PI == rs.getFloat(6));
        assertTrue((float) Math.PI == rs.getFloat("f"));
        assertTrue(rs.getBoolean(6));

        assertEquals(-3, rs.getInt(7));
        assertEquals(-3, rs.getByte(7));
        assertEquals(-3, rs.getShort(7));
        assertEquals(-3, rs.getLong(7));

        Object[] a2 = (Object[]) rs.getArray(8).getArray();
        assertEquals(2, a2.length);
        assertTrue(a == a2);
        SimpleArray array = (SimpleArray) rs.getArray("h");
        assertEquals(Types.NULL, array.getBaseType());
        assertEquals("NULL", array.getBaseTypeName());
        a2 = (Object[]) array.getArray();
        array.free();
        assertEquals(2, a2.length);
        assertTrue(a == a2);

        assertTrue(t == rs.getTime("i"));
        assertTrue(t == rs.getTime(9));

        assertTrue(ts == rs.getTimestamp("j"));
        assertTrue(ts == rs.getTimestamp(10));

        assertTrue(clob == rs.getClob("k"));
        assertTrue(clob == rs.getClob(11));
        assertEquals("Hello World", rs.getString("k"));
        assertEquals("Hello World", rs.getString(11));

        assertTrue(blob == rs.getBlob("l"));
        assertTrue(blob == rs.getBlob(12));

        assertThrows(ErrorCode.INVALID_VALUE_2, (ResultSet) rs).
                getString(13);
        assertThrows(ErrorCode.COLUMN_NOT_FOUND_1, (ResultSet) rs).
                getString("NOT_FOUND");

        rs.next();

        assertTrue(rs.getBoolean(1));
        assertTrue(rs.getBoolean(3));
        assertTrue(rs.getBoolean(5));
        assertTrue(rs.getBoolean(6));

        rs.next();

        assertFalse(rs.getBoolean(1));
        assertFalse(rs.getBoolean(3));
        assertFalse(rs.getBoolean(5));
        assertFalse(rs.getBoolean(6));

        rs.next();

        assertEquals(0, rs.getLong(1));
        assertTrue(rs.wasNull());
        assertEquals(null, rs.getBytes(2));
        assertTrue(rs.wasNull());
        assertFalse(rs.getBoolean(3));
        assertTrue(rs.wasNull());
        assertNull(rs.getDate(4));
        assertTrue(rs.wasNull());
        assertNull(rs.getBigDecimal(5));
        assertTrue(rs.wasNull());
        assertEquals(0.0, rs.getDouble(5));
        assertTrue(rs.wasNull());
        assertEquals(0.0, rs.getDouble(6));
        assertTrue(rs.wasNull());
        assertEquals(0.0, rs.getFloat(6));
        assertTrue(rs.wasNull());
        assertEquals(0, rs.getInt(7));
        assertTrue(rs.wasNull());
        assertNull(rs.getArray(8));
        assertTrue(rs.wasNull());
        assertNull(rs.getTime(9));
        assertTrue(rs.wasNull());
        assertNull(rs.getTimestamp(10));
        assertTrue(rs.wasNull());
        assertNull(rs.getClob(11));
        assertTrue(rs.wasNull());
        assertNull(rs.getCharacterStream(11));
        assertTrue(rs.wasNull());
        assertNull(rs.getBlob(12));
        assertTrue(rs.wasNull());
        assertNull(rs.getBinaryStream(12));
        assertTrue(rs.wasNull());

        // all updateX methods
        for (Method m: rs.getClass().getMethods()) {
            if (m.getName().startsWith("update")) {
                if (m.getName().equals("updateRow")) {
                    continue;
                }
                int len = m.getParameterTypes().length;
                if (m.getName().equals("updateObject") && m.getParameterTypes().length > 2) {
                    Class<?> p3 = m.getParameterTypes()[2];
                    if (p3.toString().indexOf("SQLType") >= 0) {
                        continue;
                    }
                }
                Object[] params = new Object[len];
                int i = 0;
                String expectedValue = null;
                for (Class<?> type : m.getParameterTypes()) {
                    Object o;
                    String e = null;
                    if (type == int.class) {
                        o = 1;
                        e = "1";
                    } else if (type == byte.class) {
                        o = (byte) 2;
                        e = "2";
                    } else if (type == double.class) {
                        o = (double) 3;
                        e = "3.0";
                    } else if (type == float.class) {
                        o = (float) 4;
                        e = "4.0";
                    } else if (type == long.class) {
                        o = (long) 5;
                        e = "5";
                    } else if (type == short.class) {
                        o = (short) 6;
                        e = "6";
                    } else if (type == boolean.class) {
                        o = false;
                        e = "false";
                    } else if (type == String.class) {
                        // columnName or value
                        o = "a";
                        e = "a";
                    } else {
                        o = null;
                    }
                    if (i == 1) {
                        expectedValue = e;
                    }
                    params[i] = o;
                    i++;
                }
                m.invoke(rs, params);
                if (params.length == 1) {
                    // updateNull
                    assertEquals(null, rs.getString(1));
                } else {
                    assertEquals(expectedValue, rs.getString(1));
                }
                // invalid column name / index
                Object invalidColumn;
                if (m.getParameterTypes()[0] == String.class) {
                    invalidColumn = "x";
                } else {
                    invalidColumn = 0;
                }
                params[0] = invalidColumn;
                try {
                    m.invoke(rs, params);
                    fail();
                } catch (InvocationTargetException e) {
                    SQLException e2 = (SQLException) e.getTargetException();
                    if (invalidColumn instanceof String) {
                        assertEquals(ErrorCode.COLUMN_NOT_FOUND_1,
                                e2.getErrorCode());
                    } else {
                        assertEquals(ErrorCode.INVALID_VALUE_2,
                                e2.getErrorCode());
                    }
                }
            }
        }
        assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
        assertEquals(0, rs.getFetchSize());
        assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType());
        assertTrue(rs.getStatement() == null);
        assertFalse(rs.isClosed());

        rs.beforeFirst();
        assertEquals(0, rs.getRow());
        assertTrue(rs.next());
        assertFalse(rs.isClosed());
        assertEquals(1, rs.getRow());
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());
        assertThrows(ErrorCode.NO_DATA_AVAILABLE, (ResultSet) rs).
                getInt(1);
        assertEquals(0, rs.getRow());
        assertFalse(rs.isClosed());
        rs.close();
        assertTrue(rs.isClosed());
        rs = new SimpleResultSet();
        rs.addColumn("TEST", Types.BINARY, 0, 0);
        UUID uuid = UUID.randomUUID();
        rs.addRow(uuid);
        rs.next();
        assertEquals(uuid, rs.getObject(1));
        assertEquals(uuid, ValueUuid.get(rs.getBytes(1)).getObject());
    }

    private void testJdbcDriverUtils() {
        assertEquals("org.h2.Driver",
                JdbcUtils.getDriver("jdbc:h2:~/test"));
        assertEquals("org.postgresql.Driver",
                JdbcUtils.getDriver("jdbc:postgresql:test"));
        assertEquals(null,
                JdbcUtils.getDriver("jdbc:unknown:test"));
    }

    private void testWrongServer() throws Exception {
        // try to connect when the server is not running
        assertThrows(ErrorCode.CONNECTION_BROKEN_1, this).
                getConnection("jdbc:h2:tcp://localhost:9001/test");
        final ServerSocket serverSocket = new ServerSocket(9001);
        Task task = new Task() {
            @Override
            public void call() throws Exception {
                while (!stop) {
                    Socket socket = serverSocket.accept();
                    byte[] data = new byte[1024];
                    data[0] = 'x';
                    OutputStream out = socket.getOutputStream();
                    out.write(data);
                    out.close();
                    socket.close();
                }
            }
        };
        try {
            task.execute();
            Thread.sleep(100);
            try {
                getConnection("jdbc:h2:tcp://localhost:9001/test");
                fail();
            } catch (SQLException e) {
                assertEquals(ErrorCode.CONNECTION_BROKEN_1, e.getErrorCode());
            }
        } finally {
            serverSocket.close();
            task.getException();
        }
    }

    private void testDeleteFiles() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("testDeleteFiles");
        Connection conn = getConnection("testDeleteFiles");
        Statement stat = conn.createStatement();
        stat.execute("create table test(c clob) as select space(10000) from dual");
        conn.close();
        // the name starts with the same string, but does not match it
        DeleteDbFiles.execute(getBaseDir(), "testDelete", true);
        conn = getConnection("testDeleteFiles");
        stat = conn.createStatement();
        ResultSet rs;
        rs = stat.executeQuery("select * from test");
        rs.next();
        rs.getString(1);
        conn.close();
        deleteDb("testDeleteFiles");
    }

    private void testServerMain() throws SQLException {
        testNonSSL();
        if (!config.travis) {
            testSSL();
        }
    }

    private void testNonSSL() throws SQLException {
        String result;
        Connection conn;

        try {
            result = runServer(0, new String[]{"-?"});
            assertContains(result, "Starts the H2 Console");
            assertTrue(result.indexOf("Unknown option") < 0);

            result = runServer(1, new String[]{"-xy"});
            assertContains(result, "Starts the H2 Console");
            assertContains(result, "Feature not supported");
            result = runServer(0, new String[]{"-tcp",
                    "-tcpPort", "9001", "-tcpPassword", "abc"});
            assertContains(result, "tcp://");
            assertContains(result, ":9001");
            assertContains(result, "only local");
            assertTrue(result.indexOf("Starts the H2 Console") < 0);
            conn = getConnection("jdbc:h2:tcp://localhost:9001/mem:", "sa", "sa");
            conn.close();
            result = runServer(0, new String[]{"-tcpShutdown",
                    "tcp://localhost:9001", "-tcpPassword", "abc", "-tcpShutdownForce"});
            assertContains(result, "Shutting down");
        } finally {
            shutdownServers();
        }
    }

    private void testSSL() throws SQLException {
        String result;
        Connection conn;

        try {
            result = runServer(0, new String[]{"-tcp",
                    "-tcpAllowOthers", "-tcpPort", "9001", "-tcpPassword", "abcdef", "-tcpSSL"});
            assertContains(result, "ssl://");
            assertContains(result, ":9001");
            assertContains(result, "others can");
            assertTrue(result.indexOf("Starts the H2 Console") < 0);
            conn = getConnection("jdbc:h2:ssl://localhost:9001/mem:", "sa", "sa");
            conn.close();

            result = runServer(0, new String[]{"-tcpShutdown",
                    "ssl://localhost:9001", "-tcpPassword", "abcdef"});
            assertContains(result, "Shutting down");
            assertThrows(ErrorCode.CONNECTION_BROKEN_1, this).
            getConnection("jdbc:h2:ssl://localhost:9001/mem:", "sa", "sa");

            result = runServer(0, new String[]{
                    "-web", "-webPort", "9002", "-webAllowOthers", "-webSSL",
                    "-pg", "-pgAllowOthers", "-pgPort", "9003",
                    "-tcp", "-tcpAllowOthers", "-tcpPort", "9006", "-tcpPassword", "abc"});
            Server stop = server;
            assertContains(result, "https://");
            assertContains(result, ":9002");
            assertContains(result, "pg://");
            assertContains(result, ":9003");
            assertContains(result, "others can");
            assertTrue(result.indexOf("only local") < 0);
            assertContains(result, "tcp://");
            assertContains(result, ":9006");

            conn = getConnection("jdbc:h2:tcp://localhost:9006/mem:", "sa", "sa");
            conn.close();

            result = runServer(0, new String[]{"-tcpShutdown",
                    "tcp://localhost:9006", "-tcpPassword", "abc", "-tcpShutdownForce"});
            assertContains(result, "Shutting down");
            stop.shutdown();
            assertThrows(ErrorCode.CONNECTION_BROKEN_1, this).
            getConnection("jdbc:h2:tcp://localhost:9006/mem:", "sa", "sa");
        } finally {
            shutdownServers();
        }
    }

    private String runServer(int exitCode, String... args) {
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(buff);
        if (server != null) {
            remainingServers.add(server);
        }
        server = new Server();
        server.setOut(ps);
        int result = 0;
        try {
            server.runTool(args);
        } catch (SQLException e) {
            result = 1;
            e.printStackTrace(ps);
        }
        assertEquals(exitCode, result);
        ps.flush();
        String s = new String(buff.toByteArray());
        return s;
    }

    private void shutdownServers() {
        for (Server remainingServer : remainingServers) {
            if (remainingServer != null) {
                remainingServer.shutdown();
            }
        }
        remainingServers.clear();
        if (server != null) {
            server.shutdown();
        }
    }

    private void testConvertTraceFile() throws Exception {
        deleteDb("toolsConvertTraceFile");
        org.h2.Driver.load();
        String url = "jdbc:h2:" + getBaseDir() + "/toolsConvertTraceFile";
        url = getURL(url, true);
        Connection conn = getConnection(url + ";TRACE_LEVEL_FILE=3", "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute(
                "create table test(id int primary key, name varchar, amount decimal)");
        PreparedStatement prep = conn.prepareStatement(
                "insert into test values(?, ?, ?)");
        prep.setInt(1, 1);
        prep.setString(2, "Hello \\'Joe\n\\'");
        prep.setBigDecimal(3, new BigDecimal("10.20"));
        prep.executeUpdate();
        stat.execute("create table test2(id int primary key,\n" +
                "a real, b double, c bigint,\n" +
                "d smallint, e boolean, f binary, g date, h time, i timestamp)",
                Statement.NO_GENERATED_KEYS);
        prep = conn.prepareStatement(
                "insert into test2 values(1, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        prep.setFloat(1, Float.MIN_VALUE);
        prep.setDouble(2, Double.MIN_VALUE);
        prep.setLong(3, Long.MIN_VALUE);
        prep.setShort(4, Short.MIN_VALUE);
        prep.setBoolean(5, false);
        prep.setBytes(6, new byte[] { (byte) 10, (byte) 20 });
        prep.setDate(7, java.sql.Date.valueOf("2007-12-31"));
        prep.setTime(8, java.sql.Time.valueOf("23:59:59"));
        prep.setTimestamp(9, java.sql.Timestamp.valueOf("2007-12-31 23:59:59"));
        prep.executeUpdate();
        conn.close();

        ConvertTraceFile.main("-traceFile", getBaseDir() +
                "/toolsConvertTraceFile.trace.db", "-javaClass", getBaseDir() +
                "/Test", "-script", getBaseDir() + "/test.sql");
        FileUtils.delete(getBaseDir() + "/Test.java");

        String trace = getBaseDir() + "/toolsConvertTraceFile.trace.db";
        assertTrue(FileUtils.exists(trace));
        String newTrace = getBaseDir() + "/test.trace.db";
        FileUtils.delete(newTrace);
        assertFalse(FileUtils.exists(newTrace));
        FileUtils.move(trace, newTrace);
        deleteDb("toolsConvertTraceFile");
        Player.main(getBaseDir() + "/test.trace.db");
        testTraceFile(url);

        deleteDb("toolsConvertTraceFile");
        RunScript.main("-url", url, "-user", "sa", "-script", getBaseDir() +
                "/test.sql");
        testTraceFile(url);

        deleteDb("toolsConvertTraceFile");
        FileUtils.delete(getBaseDir() + "/toolsConvertTraceFile.h2.sql");
        FileUtils.delete(getBaseDir() + "/test.sql");
    }

    private void testTraceFile(String url) throws SQLException {
        Connection conn;
        Recover.main("-removePassword", "-dir", getBaseDir(), "-db",
                "toolsConvertTraceFile");
        conn = getConnection(url, "sa", "");
        Statement stat = conn.createStatement();
        ResultSet rs;
        rs = stat.executeQuery("select * from test");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello \\'Joe\n\\'", rs.getString(2));
        assertEquals("10.20", rs.getBigDecimal(3).toString());
        assertFalse(rs.next());
        rs = stat.executeQuery("select * from test2");
        rs.next();
        assertEquals(Float.MIN_VALUE, rs.getFloat("a"));
        assertEquals(Double.MIN_VALUE, rs.getDouble("b"));
        assertEquals(Long.MIN_VALUE, rs.getLong("c"));
        assertEquals(Short.MIN_VALUE, rs.getShort("d"));
        assertTrue(!rs.getBoolean("e"));
        assertEquals(new byte[] { (byte) 10, (byte) 20 }, rs.getBytes("f"));
        assertEquals("2007-12-31", rs.getString("g"));
        assertEquals("23:59:59", rs.getString("h"));
        assertEquals("2007-12-31 23:59:59", rs.getString("i"));
        assertFalse(rs.next());
        conn.close();
    }

    private void testRemove() throws SQLException {
        if (config.mvStore) {
            return;
        }
        deleteDb("toolsRemove");
        org.h2.Driver.load();
        String url = "jdbc:h2:" + getBaseDir() + "/toolsRemove";
        Connection conn = getConnection(url, "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("insert into test values(1, 'Hello')");
        conn.close();
        Recover.main("-dir", getBaseDir(), "-db", "toolsRemove",
                "-removePassword");
        conn = getConnection(url, "sa", "");
        stat = conn.createStatement();
        ResultSet rs;
        rs = stat.executeQuery("select * from test");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        conn.close();
        deleteDb("toolsRemove");
        FileUtils.delete(getBaseDir() + "/toolsRemove.h2.sql");
    }

    private void testRecover() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("toolsRecover");
        org.h2.Driver.load();
        String url = getURL("toolsRecover", true);
        Connection conn = getConnection(url, "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, " +
                "name varchar, b blob, c clob)");
        stat.execute("create table \"test 2\"(id int primary key, name varchar)");
        stat.execute("comment on table test is ';-)'");
        stat.execute("insert into test values" +
                "(1, 'Hello', SECURE_RAND(4100), '\u00e4' || space(4100))");
        ResultSet rs;
        rs = stat.executeQuery("select * from test");
        rs.next();
        byte[] b1 = rs.getBytes(3);
        String s1 = rs.getString(4);

        conn.close();
        Recover.main("-dir", getBaseDir(), "-db", "toolsRecover");

        // deleteDb would delete the .lob.db directory as well
        // deleteDb("toolsRecover");
        ArrayList<String> list = FileLister.getDatabaseFiles(getBaseDir(),
                "toolsRecover", true);
        for (String fileName : list) {
            if (!FileUtils.isDirectory(fileName)) {
                FileUtils.delete(fileName);
            }
        }

        conn = getConnection(url);
        stat = conn.createStatement();
        String suffix = ".h2.sql";
        stat.execute("runscript from '" + getBaseDir() + "/toolsRecover" +
                suffix + "'");
        rs = stat.executeQuery("select * from \"test 2\"");
        assertFalse(rs.next());
        rs = stat.executeQuery("select * from test");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        byte[] b2 = rs.getBytes(3);
        String s2 = rs.getString(4);
        assertEquals("\u00e4 ", s2.substring(0, 2));
        assertEquals(4100, b2.length);
        assertEquals(4101, s2.length());
        assertEquals(b1, b2);
        assertEquals(s1, s2);
        assertFalse(rs.next());
        conn.close();
        deleteDb("toolsRecover");
        FileUtils.delete(getBaseDir() + "/toolsRecover.h2.sql");
        String dir = getBaseDir() + "/toolsRecover.lobs.db";
        FileUtils.deleteRecursive(dir, false);
    }

    private void testManagementDb() throws SQLException {
        int count = getSize(2, 10);
        for (int i = 0; i < count; i++) {
            Server tcpServer = Server.
                    createTcpServer().start();
            tcpServer.stop();
            tcpServer = Server.createTcpServer("-tcpPassword", "abc").start();
            tcpServer.stop();
        }
    }

    private void testScriptRunscriptLob() throws Exception {
        org.h2.Driver.load();
        String url = getURL("jdbc:h2:" + getBaseDir() +
                "/testScriptRunscriptLob", true);
        String user = "sa", password = "abc";
        String fileName = getBaseDir() + "/b2.sql";
        Connection conn = getConnection(url, user, password);
        conn.createStatement().execute(
                "CREATE TABLE TEST(ID INT PRIMARY KEY, BDATA BLOB, CDATA CLOB)");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?, ?, ?)");

        prep.setInt(1, 1);
        prep.setNull(2, Types.BLOB);
        prep.setNull(3, Types.CLOB);
        prep.execute();

        prep.setInt(1, 2);
        prep.setString(2, "face");
        prep.setString(3, "face");
        prep.execute();

        Random random = new Random(1);
        prep.setInt(1, 3);
        byte[] large = new byte[getSize(10 * 1024, 100 * 1024)];
        random.nextBytes(large);
        prep.setBytes(2, large);
        String largeText = new String(large, StandardCharsets.ISO_8859_1);
        prep.setString(3, largeText);
        prep.execute();

        for (int i = 0; i < 2; i++) {
            ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT * FROM TEST ORDER BY ID");
            rs.next();
            assertEquals(1, rs.getInt(1));
            assertTrue(rs.getString(2) == null);
            assertTrue(rs.getString(3) == null);
            rs.next();
            assertEquals(2, rs.getInt(1));
            assertEquals("face", rs.getString(2));
            assertEquals("face", rs.getString(3));
            rs.next();
            assertEquals(3, rs.getInt(1));
            assertEquals(large, rs.getBytes(2));
            assertEquals(largeText, rs.getString(3));
            assertFalse(rs.next());

            conn.close();
            Script.main("-url", url, "-user", user, "-password", password,
                    "-script", fileName);
            DeleteDbFiles.main("-dir", getBaseDir(), "-db",
                    "testScriptRunscriptLob", "-quiet");
            RunScript.main("-url", url, "-user", user, "-password", password,
                    "-script", fileName);
            conn = getConnection("jdbc:h2:" + getBaseDir() +
                    "/testScriptRunscriptLob", "sa", "abc");
        }
        conn.close();

    }

    private void testScriptRunscript() throws SQLException {
        org.h2.Driver.load();
        String url = getURL("jdbc:h2:" + getBaseDir() + "/testScriptRunscript",
                true);
        String user = "sa", password = "abc";
        String fileName = getBaseDir() + "/b2.sql";
        DeleteDbFiles.main("-dir", getBaseDir(), "-db", "testScriptRunscript",
                "-quiet");
        Connection conn = getConnection(url, user, password);
        conn.createStatement().execute("CREATE TABLE \u00f6()");
        conn.createStatement().execute(
                "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        conn.createStatement().execute("INSERT INTO TEST VALUES(1, 'Hello')");
        conn.close();
        Script.main("-url", url, "-user", user, "-password", password,
                "-script", fileName, "-options", "nodata", "compression",
                "lzf", "cipher", "aes", "password", "'123'", "charset",
                "'utf-8'");
        Script.main("-url", url, "-user", user, "-password", password,
                "-script", fileName + ".txt");
        DeleteDbFiles.main("-dir", getBaseDir(), "-db", "testScriptRunscript",
                "-quiet");
        RunScript.main("-url", url, "-user", user, "-password", password,
                "-script", fileName, "-options", "compression", "lzf",
                "cipher", "aes", "password", "'123'", "charset", "'utf-8'");
        conn = getConnection(
                "jdbc:h2:" + getBaseDir() + "/testScriptRunscript", "sa", "abc");
        ResultSet rs = conn.createStatement()
                .executeQuery("SELECT * FROM TEST");
        assertFalse(rs.next());
        rs = conn.createStatement().executeQuery("SELECT * FROM \u00f6");
        assertFalse(rs.next());
        conn.close();

        DeleteDbFiles.main("-dir", getBaseDir(), "-db", "testScriptRunscript",
                "-quiet");
        RunScript tool = new RunScript();
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        tool.setOut(new PrintStream(buff));
        tool.runTool("-url", url, "-user", user, "-password", password,
                "-script", fileName + ".txt", "-showResults");
        assertContains(buff.toString(), "Hello");


        // test parsing of BLOCKSIZE option
        DeleteDbFiles.main("-dir", getBaseDir(), "-db", "testScriptRunscript",
                "-quiet");
        conn = getConnection(url, user, password);
        conn.createStatement().execute(
                "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        conn.close();
        Script.main("-url", url, "-user", user, "-password", password,
                "-script", fileName, "-options", "simple", "blocksize",
                "8192");
    }

    private void testBackupRestore() throws SQLException {
        org.h2.Driver.load();
        String url = "jdbc:h2:" + getBaseDir() + "/testBackupRestore";
        String user = "sa", password = "abc";
        final String fileName = getBaseDir() + "/b2.zip";
        DeleteDbFiles.main("-dir", getBaseDir(), "-db", "testBackupRestore",
                "-quiet");
        Connection conn = getConnection(url, user, password);
        conn.createStatement().execute(
                "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        conn.createStatement().execute("INSERT INTO TEST VALUES(1, 'Hello')");
        conn.close();
        Backup.main("-file", fileName, "-dir", getBaseDir(), "-db",
                "testBackupRestore", "-quiet");
        DeleteDbFiles.main("-dir", getBaseDir(), "-db", "testBackupRestore",
                "-quiet");
        Restore.main("-file", fileName, "-dir", getBaseDir(), "-db",
                "testBackupRestore", "-quiet");
        conn = getConnection("jdbc:h2:" + getBaseDir() + "/testBackupRestore",
                "sa", "abc");
        ResultSet rs = conn.createStatement()
                .executeQuery("SELECT * FROM TEST");
        assertTrue(rs.next());
        assertFalse(rs.next());
        new AssertThrows(ErrorCode.CANNOT_CHANGE_SETTING_WHEN_OPEN_1) {
            @Override
            public void test() throws SQLException {
                // must fail when the database is in use
                Backup.main("-file", fileName, "-dir", getBaseDir(), "-db",
                        "testBackupRestore");
            }
        };
        conn.close();
        DeleteDbFiles.main("-dir", getBaseDir(), "-db", "testBackupRestore",
                "-quiet");
    }

    private void testChangeFileEncryption(boolean split) throws SQLException {
        org.h2.Driver.load();
        final String dir = (split ? "split:19:" : "") + getBaseDir();
        String url = "jdbc:h2:" + dir + "/testChangeFileEncryption;CIPHER=AES";
        DeleteDbFiles.execute(dir, "testChangeFileEncryption", true);
        Connection conn = getConnection(url, "sa", "abc 123");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, DATA CLOB) "
                + "AS SELECT X, SPACE(3000) FROM SYSTEM_RANGE(1, 300)");
        conn.close();
        String[] args = { "-dir", dir, "-db", "testChangeFileEncryption",
                "-cipher", "AES", "-decrypt", "abc", "-quiet" };
        ChangeFileEncryption.main(args);
        args = new String[] { "-dir", dir, "-db", "testChangeFileEncryption",
                "-cipher", "AES", "-encrypt", "def", "-quiet" };
        ChangeFileEncryption.main(args);
        conn = getConnection(url, "sa", "def 123");
        stat = conn.createStatement();
        stat.execute("SELECT * FROM TEST");
        new AssertThrows(ErrorCode.CANNOT_CHANGE_SETTING_WHEN_OPEN_1) {
            @Override
            public void test() throws SQLException {
                ChangeFileEncryption.main(new String[] { "-dir", dir, "-db",
                        "testChangeFileEncryption", "-cipher", "AES",
                        "-decrypt", "def", "-quiet" });
            }
        };
        conn.close();
        args = new String[] { "-dir", dir, "-db", "testChangeFileEncryption",
                "-quiet" };
        DeleteDbFiles.main(args);
    }

    private void testChangeFileEncryptionWithWrongPassword() throws SQLException {
        if (config.mvStore) {
            // the file system encryption abstraction used by the MVStore
            // doesn't detect wrong passwords
            return;
        }
        org.h2.Driver.load();
        final String dir = getBaseDir();
        // TODO: this doesn't seem to work in MVSTORE mode yet
        String url = "jdbc:h2:" + dir + "/testChangeFileEncryption;CIPHER=AES";
        DeleteDbFiles.execute(dir, "testChangeFileEncryption", true);
        Connection conn = getConnection(url, "sa", "abc 123");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, DATA CLOB) "
                + "AS SELECT X, SPACE(3000) FROM SYSTEM_RANGE(1, 300)");
        conn.close();
        // try with wrong password, this used to have a bug where it kept the
        // file handle open
        new AssertThrows(SQLException.class) {
            @Override
            public void test() throws SQLException {
                ChangeFileEncryption.execute(dir, "testChangeFileEncryption",
                        "AES", "wrong".toCharArray(),
                        "def".toCharArray(), true);
            }
        };
        ChangeFileEncryption.execute(dir, "testChangeFileEncryption",
                "AES", "abc".toCharArray(), "def".toCharArray(),
                true);

        conn = getConnection(url, "sa", "def 123");
        stat = conn.createStatement();
        stat.execute("SELECT * FROM TEST");
        conn.close();
        String[] args = new String[] { "-dir", dir, "-db", "testChangeFileEncryption", "-quiet" };
        DeleteDbFiles.main(args);
    }

    private void testServer() throws SQLException {
        Connection conn;
        try {
            deleteDb("test");
            Server tcpServer = Server.createTcpServer(
                            "-baseDir", getBaseDir(),
                            "-tcpAllowOthers").start();
            remainingServers.add(tcpServer);
            final int port = tcpServer.getPort();
            conn = getConnection("jdbc:h2:tcp://localhost:"+ port +"/test", "sa", "");
            conn.close();
            // must not be able to use a different base dir
            new AssertThrows(ErrorCode.IO_EXCEPTION_1) {
                @Override
                public void test() throws SQLException {
                    getConnection("jdbc:h2:tcp://localhost:"+ port +"/../test", "sa", "");
            }};
            new AssertThrows(ErrorCode.IO_EXCEPTION_1) {
                @Override
                public void test() throws SQLException {
                    getConnection("jdbc:h2:tcp://localhost:"+port+"/../test2/test", "sa", "");
            }};
            tcpServer.stop();
            Server tcpServerWithPassword = Server.createTcpServer(
                            "-ifExists",
                            "-tcpPassword", "abc",
                            "-baseDir", getBaseDir()).start();
            final int prt = tcpServerWithPassword.getPort();
            remainingServers.add(tcpServerWithPassword);
            // must not be able to create new db
            new AssertThrows(ErrorCode.DATABASE_NOT_FOUND_1) {
                @Override
                public void test() throws SQLException {
                    getConnection("jdbc:h2:tcp://localhost:"+prt+"/test2", "sa", "");
            }};
            new AssertThrows(ErrorCode.DATABASE_NOT_FOUND_1) {
                @Override
                public void test() throws SQLException {
                    getConnection("jdbc:h2:tcp://localhost:"+prt+"/test2;ifexists=false", "sa", "");
            }};
            conn = getConnection("jdbc:h2:tcp://localhost:"+prt+"/test", "sa", "");
            conn.close();
            new AssertThrows(ErrorCode.WRONG_USER_OR_PASSWORD) {
                @Override
                public void test() throws SQLException {
                    Server.shutdownTcpServer("tcp://localhost:"+prt, "", true, false);
            }};
            conn = getConnection("jdbc:h2:tcp://localhost:"+prt+"/test", "sa", "");
            // conn.close();
            Server.shutdownTcpServer("tcp://localhost:"+prt, "abc", true, false);
            // check that the database is closed
            deleteDb("test");
            // server must have been closed
            assertThrows(ErrorCode.CONNECTION_BROKEN_1, this).
                    getConnection("jdbc:h2:tcp://localhost:"+prt+"/test", "sa", "");
            JdbcUtils.closeSilently(conn);
            // Test filesystem prefix and escape from baseDir
            deleteDb("testSplit");
            server = Server.createTcpServer(
                            "-baseDir", getBaseDir(),
                            "-tcpAllowOthers").start();
            final int p = server.getPort();
            conn = getConnection("jdbc:h2:tcp://localhost:"+p+"/split:testSplit", "sa", "");
            conn.close();

            assertThrows(ErrorCode.IO_EXCEPTION_1, this).
                    getConnection("jdbc:h2:tcp://localhost:"+p+"/../test", "sa", "");

            server.stop();
            deleteDb("testSplit");
        } finally {
            shutdownServers();
        }
    }

    /**
     * A simple Clob implementation.
     */
    class SimpleClob implements Clob {

        private final String data;

        SimpleClob(String data) {
            this.data = data;
        }

        /**
         * Free the clob.
         */
        @Override
        public void free() throws SQLException {
            // ignore
        }

        @Override
        public InputStream getAsciiStream() throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Reader getCharacterStream() throws SQLException {
            throw new UnsupportedOperationException();
        }

        /**
         * Get the reader.
         *
         * @param pos the position
         * @param length the length
         * @return the reader
         */
        @Override
        public Reader getCharacterStream(long pos, long length)
                throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getSubString(long pos, int length) throws SQLException {
            return data;
        }

        @Override
        public long length() throws SQLException {
            return data.length();
        }

        @Override
        public long position(String search, long start) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long position(Clob search, long start) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public OutputStream setAsciiStream(long pos) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Writer setCharacterStream(long pos) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int setString(long pos, String str) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int setString(long pos, String str, int offset, int len)
                throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void truncate(long len) throws SQLException {
            throw new UnsupportedOperationException();
        }

    }

    /**
     * A simple Blob implementation.
     */
    class SimpleBlob implements Blob {

        private final byte[] data;

        SimpleBlob(byte[] data) {
            this.data = data;
        }

        /**
         * Free the blob.
         */
        @Override
        public void free() throws SQLException {
            // ignore
        }

        @Override
        public InputStream getBinaryStream() throws SQLException {
            throw new UnsupportedOperationException();
        }

        /**
         * Get the binary stream.
         *
         * @param pos the position
         * @param length the length
         * @return the input stream
         */
        @Override
        public InputStream getBinaryStream(long pos, long length)
                throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] getBytes(long pos, int length) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long length() throws SQLException {
            return data.length;
        }

        @Override
        public long position(byte[] pattern, long start) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long position(Blob pattern, long start) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public OutputStream setBinaryStream(long pos) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int setBytes(long pos, byte[] bytes) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int setBytes(long pos, byte[] bytes, int offset, int len)
                throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void truncate(long len) throws SQLException {
            throw new UnsupportedOperationException();
        }

    }

}
