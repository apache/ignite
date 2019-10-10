/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import org.h2.api.ErrorCode;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.value.DataType;

/**
 * Tests the linked table feature (CREATE LINKED TABLE).
 */
public class TestLinkedTable extends TestBase {

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
        testLinkedServerMode();
        testDefaultValues();
        testHiddenSQL();
        // testLinkAutoAdd();
        testNestedQueriesToSameTable();
        testSharedConnection();
        testMultipleSchemas();
        testReadOnlyLinkedTable();
        testLinkOtherSchema();
        testLinkDrop();
        testLinkSchema();
        testLinkEmitUpdates();
        testLinkTable();
        testLinkTwoTables();
        testCachingResults();
        testLinkedTableInReadOnlyDb();
        testGeometry();
        deleteDb("linkedTable");
    }

    private void testLinkedServerMode() throws SQLException {
        if (config.memory) {
            return;
        }
        // the network mode will result in a deadlock
        if (config.networked) {
            return;
        }
        deleteDb("linkedTable1");
        deleteDb("linkedTable2");
        String url2 = getURL("linkedTable2", true);
        String user = getUser(), password = getPassword();
        Connection conn = getConnection("linkedTable2");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int)");
        conn.close();
        conn = getConnection("linkedTable1");
        stat = conn.createStatement();
        stat.execute("create linked table link(null, '"+url2+
                "', '"+user+"', '"+password+"', 'TEST')");
        conn.close();
        conn = getConnection("linkedTable1");
        conn.close();
    }

    private void testDefaultValues() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("linkedTable");
        Connection connMain = DriverManager.getConnection("jdbc:h2:mem:linkedTable");
        Statement statMain = connMain.createStatement();
        statMain.execute("create table test(id identity, name varchar default 'test')");

        Connection conn = getConnection("linkedTable");
        Statement stat = conn.createStatement();
        stat.execute("create linked table test1('', " +
                "'jdbc:h2:mem:linkedTable', '', '', 'TEST') emit updates");
        stat.execute("create linked table test2('', " +
                "'jdbc:h2:mem:linkedTable', '', '', 'TEST')");
        stat.execute("insert into test1 values(default, default)");
        stat.execute("insert into test2 values(default, default)");
        stat.execute("merge into test2 values(3, default)");
        stat.execute("update test1 set name=default where id=1");
        stat.execute("update test2 set name=default where id=2");

        ResultSet rs = statMain.executeQuery("select * from test order by id");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("test", rs.getString(2));
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals("test", rs.getString(2));
        rs.next();
        assertEquals(3, rs.getInt(1));
        assertEquals("test", rs.getString(2));
        assertFalse(rs.next());

        stat.execute("delete from test1 where id=1");
        stat.execute("delete from test2 where id=2");
        stat.execute("delete from test2 where id=3");
        conn.close();
        rs = statMain.executeQuery("select * from test order by id");
        assertFalse(rs.next());

        connMain.close();
    }

    private void testHiddenSQL() throws SQLException {
        if (config.memory) {
            return;
        }
        org.h2.Driver.load();
        deleteDb("linkedTable");
        Connection conn = getConnection(
                "linkedTable;SHARE_LINKED_CONNECTIONS=TRUE");
        try {
            conn.createStatement().execute("create linked table test" +
                    "(null, 'jdbc:h2:mem:', 'sa', 'pwd', 'DUAL2')");
            fail();
        } catch (SQLException e) {
            assertContains(e.toString(), "pwd");
        }
        try {
            conn.createStatement().execute("create linked table test" +
                    "(null, 'jdbc:h2:mem:', 'sa', 'pwd', 'DUAL2') --hide--");
            fail();
        } catch (SQLException e) {
            assertTrue(e.toString().indexOf("pwd") < 0);
        }
        conn.close();
    }

    // this is not a bug, it is the documented behavior
//    private void testLinkAutoAdd() throws SQLException {
//        Class.forName("org.h2.Driver");
//        Connection ca =
//            DriverManager.getConnection("jdbc:h2:mem:one", "sa", "sa");
//        Connection cb =
//            DriverManager.getConnection("jdbc:h2:mem:two", "sa", "sa");
//        Statement sa = ca.createStatement();
//        Statement sb = cb.createStatement();
//        sa.execute("CREATE TABLE ONE (X NUMBER)");
//        sb.execute(
//            "CALL LINK_SCHEMA('GOOD', '', " +
//            "'jdbc:h2:mem:one', 'sa', 'sa', 'PUBLIC'); ");
//        sb.executeQuery("SELECT * FROM GOOD.ONE");
//        sa.execute("CREATE TABLE TWO (X NUMBER)");
//        sb.executeQuery("SELECT * FROM GOOD.TWO"); // FAILED
//        ca.close();
//        cb.close();
//    }

    private void testNestedQueriesToSameTable() throws SQLException {
        if (config.memory) {
            return;
        }
        org.h2.Driver.load();
        deleteDb("linkedTable");
        String url = getURL("linkedTable;SHARE_LINKED_CONNECTIONS=TRUE", true);
        String user = getUser();
        String password = getPassword();
        Connection ca = getConnection(url, user, password);
        Statement sa = ca.createStatement();
        sa.execute("CREATE TABLE TEST(ID INT) AS SELECT 1");
        ca.close();
        Connection cb = DriverManager.getConnection("jdbc:h2:mem:two", "sa", "sa");
        Statement sb = cb.createStatement();
        sb.execute("CREATE LINKED TABLE T1(NULL, '" +
                url + "', '"+user+"', '"+password+"', 'TEST')");
        sb.executeQuery("SELECT * FROM DUAL A " +
                "LEFT OUTER JOIN T1 A ON A.ID=1 LEFT OUTER JOIN T1 B ON B.ID=1");
        sb.execute("DROP ALL OBJECTS");
        cb.close();
    }

    private void testSharedConnection() throws SQLException {
        if (config.memory) {
            return;
        }
        org.h2.Driver.load();
        deleteDb("linkedTable");
        String url = getURL("linkedTable;SHARE_LINKED_CONNECTIONS=TRUE", true);
        String user = getUser();
        String password = getPassword();
        Connection ca = getConnection(url, user, password);
        Statement sa = ca.createStatement();
        sa.execute("CREATE TABLE TEST(ID INT)");
        ca.close();
        Connection cb = DriverManager.getConnection("jdbc:h2:mem:two", "sa", "sa");
        Statement sb = cb.createStatement();
        sb.execute("CREATE LINKED TABLE T1(NULL, '" + url +
                ";OPEN_NEW=TRUE', '"+user+"', '"+password+"', 'TEST')");
        sb.execute("CREATE LINKED TABLE T2(NULL, '" + url +
                ";OPEN_NEW=TRUE', '"+user+"', '"+password+"', 'TEST')");
        sb.execute("DROP ALL OBJECTS");
        cb.close();
    }

    private void testMultipleSchemas() throws SQLException {
        org.h2.Driver.load();
        Connection ca = DriverManager.getConnection("jdbc:h2:mem:one", "sa", "sa");
        Connection cb = DriverManager.getConnection("jdbc:h2:mem:two", "sa", "sa");
        Statement sa = ca.createStatement();
        Statement sb = cb.createStatement();
        sa.execute("CREATE TABLE TEST(ID INT)");
        sa.execute("CREATE SCHEMA P");
        sa.execute("CREATE TABLE P.TEST(X INT)");
        sa.execute("INSERT INTO TEST VALUES(1)");
        sa.execute("INSERT INTO P.TEST VALUES(2)");
        assertThrows(ErrorCode.SCHEMA_NAME_MUST_MATCH, sb).
                execute("CREATE LINKED TABLE T(NULL, " +
                        "'jdbc:h2:mem:one', 'sa', 'sa', 'TEST')");
        sb.execute("CREATE LINKED TABLE T(NULL, " +
                        "'jdbc:h2:mem:one', 'sa', 'sa', 'PUBLIC', 'TEST')");
        sb.execute("CREATE LINKED TABLE T2(NULL, " +
                        "'jdbc:h2:mem:one', 'sa', 'sa', 'P', 'TEST')");
        assertSingleValue(sb, "SELECT * FROM T", 1);
        assertSingleValue(sb, "SELECT * FROM T2", 2);
        sa.execute("DROP ALL OBJECTS");
        sb.execute("DROP ALL OBJECTS");
        assertThrows(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, sa).
                execute("SELECT * FROM TEST");
        ca.close();
        cb.close();
    }

    private void testReadOnlyLinkedTable() throws SQLException {
        org.h2.Driver.load();
        Connection ca = DriverManager.getConnection("jdbc:h2:mem:one", "sa", "sa");
        Connection cb = DriverManager.getConnection("jdbc:h2:mem:two", "sa", "sa");
        Statement sa = ca.createStatement();
        Statement sb = cb.createStatement();
        sa.execute("CREATE TABLE TEST(ID INT)");
        sa.execute("INSERT INTO TEST VALUES(1)");
        String[] suffix = {"", "READONLY", "EMIT UPDATES"};
        for (int i = 0; i < suffix.length; i++) {
            String sql = "CREATE LINKED TABLE T(NULL, " +
                    "'jdbc:h2:mem:one', 'sa', 'sa', 'TEST')" + suffix[i];
            sb.execute(sql);
            sb.executeQuery("SELECT * FROM T");
            String[] update = {"DELETE FROM T",
                    "INSERT INTO T VALUES(2)", "UPDATE T SET ID = 3"};
            for (String u : update) {
                try {
                    sb.execute(u);
                    if (i == 1) {
                        fail();
                    }
                } catch (SQLException e) {
                    if (i == 1) {
                        assertKnownException(e);
                    } else {
                        throw e;
                    }
                }
            }
            sb.execute("DROP TABLE T");
        }
        ca.close();
        cb.close();
    }

    private static void testLinkOtherSchema() throws SQLException {
        org.h2.Driver.load();
        Connection ca = DriverManager.getConnection("jdbc:h2:mem:one", "sa", "sa");
        Connection cb = DriverManager.getConnection("jdbc:h2:mem:two", "sa", "sa");
        Statement sa = ca.createStatement();
        Statement sb = cb.createStatement();
        sa.execute("CREATE TABLE GOOD (X NUMBER)");
        sa.execute("CREATE SCHEMA S");
        sa.execute("CREATE TABLE S.BAD (X NUMBER)");
        sb.execute("CALL LINK_SCHEMA('G', '', " +
                "'jdbc:h2:mem:one', 'sa', 'sa', 'PUBLIC'); ");
        sb.execute("CALL LINK_SCHEMA('B', '', " +
                "'jdbc:h2:mem:one', 'sa', 'sa', 'S'); ");
        // OK
        sb.executeQuery("SELECT * FROM G.GOOD");
        // FAILED
        sb.executeQuery("SELECT * FROM B.BAD");
        ca.close();
        cb.close();
    }

    private void testLinkTwoTables() throws SQLException {
        org.h2.Driver.load();
        Connection conn = DriverManager.getConnection(
                "jdbc:h2:mem:one", "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("CREATE SCHEMA Y");
        stat.execute("CREATE TABLE A( C INT)");
        stat.execute("INSERT INTO A VALUES(1)");
        stat.execute("CREATE TABLE Y.A (C INT)");
        stat.execute("INSERT INTO Y.A VALUES(2)");
        Connection conn2 = DriverManager.getConnection("jdbc:h2:mem:two");
        Statement stat2 = conn2.createStatement();
        stat2.execute("CREATE LINKED TABLE one('org.h2.Driver', " +
                "'jdbc:h2:mem:one', 'sa', 'sa', 'Y.A');");
        stat2.execute("CREATE LINKED TABLE two('org.h2.Driver', " +
                "'jdbc:h2:mem:one', 'sa', 'sa', 'PUBLIC.A');");
        ResultSet rs = stat2.executeQuery("SELECT * FROM one");
        rs.next();
        assertEquals(2, rs.getInt(1));
        rs = stat2.executeQuery("SELECT * FROM two");
        rs.next();
        assertEquals(1, rs.getInt(1));
        conn.close();
        conn2.close();
    }

    private static void testLinkDrop() throws SQLException {
        org.h2.Driver.load();
        Connection connA = DriverManager.getConnection("jdbc:h2:mem:a");
        Statement statA = connA.createStatement();
        statA.execute("CREATE TABLE TEST(ID INT)");
        Connection connB = DriverManager.getConnection("jdbc:h2:mem:b");
        Statement statB = connB.createStatement();
        statB.execute("CREATE LINKED TABLE " +
                "TEST_LINK('', 'jdbc:h2:mem:a', '', '', 'TEST')");
        connA.close();
        // the connection should be closed now
        // (and the table should disappear because the last connection was
        // closed)
        statB.execute("DROP TABLE TEST_LINK");
        connA = DriverManager.getConnection("jdbc:h2:mem:a");
        statA = connA.createStatement();
        // table should not exist now
        statA.execute("CREATE TABLE TEST(ID INT)");
        connA.close();
        connB.close();
    }

    private void testLinkEmitUpdates() throws SQLException {
        if (config.memory || config.networked) {
            return;
        }

        deleteDb("linked1");
        deleteDb("linked2");
        org.h2.Driver.load();

        String url1 = getURL("linked1", true);
        String url2 = getURL("linked2", true);

        Connection conn = DriverManager.getConnection(url1, "sa1", "abc abc");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");

        Connection conn2 = DriverManager.getConnection(url2, "sa2", "def def");
        Statement stat2 = conn2.createStatement();
        String link = "CREATE LINKED TABLE TEST_LINK_U('', '" + url1
                + "', 'sa1', 'abc abc', 'TEST') EMIT UPDATES";
        stat2.execute(link);
        link = "CREATE LINKED TABLE TEST_LINK_DI('', '" + url1 +
                "', 'sa1', 'abc abc', 'TEST')";
        stat2.execute(link);
        stat2.executeUpdate("INSERT INTO TEST_LINK_U VALUES(1, 'Hello')");
        stat2.executeUpdate("INSERT INTO TEST_LINK_DI VALUES(2, 'World')");
        assertThrows(ErrorCode.ERROR_ACCESSING_LINKED_TABLE_2, stat2).
                executeUpdate("UPDATE TEST_LINK_U SET ID=ID+1");
        stat2.executeUpdate("UPDATE TEST_LINK_DI SET ID=ID+1");
        stat2.executeUpdate("UPDATE TEST_LINK_U SET NAME=NAME || ID");
        ResultSet rs;

        rs = stat2.executeQuery("SELECT * FROM TEST_LINK_DI ORDER BY ID");
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals("Hello2", rs.getString(2));
        rs.next();
        assertEquals(3, rs.getInt(1));
        assertEquals("World3", rs.getString(2));
        assertFalse(rs.next());

        rs = stat2.executeQuery("SELECT * FROM TEST_LINK_U ORDER BY ID");
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals("Hello2", rs.getString(2));
        rs.next();
        assertEquals(3, rs.getInt(1));
        assertEquals("World3", rs.getString(2));
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals("Hello2", rs.getString(2));
        rs.next();
        assertEquals(3, rs.getInt(1));
        assertEquals("World3", rs.getString(2));
        assertFalse(rs.next());

        conn.close();
        conn2.close();
    }

    private void testLinkSchema() throws SQLException {
        if (config.memory || config.networked) {
            return;
        }

        deleteDb("linked1");
        deleteDb("linked2");
        org.h2.Driver.load();
        String url1 = getURL("linked1", true);
        String url2 = getURL("linked2", true);

        Connection conn = DriverManager.getConnection(url1, "sa1", "abc abc");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST1(ID INT PRIMARY KEY)");

        Connection conn2 = DriverManager.getConnection(url2, "sa2", "def def");
        Statement stat2 = conn2.createStatement();
        String link = "CALL LINK_SCHEMA('LINKED', '', '" + url1 +
                "', 'sa1', 'abc abc', 'PUBLIC')";
        stat2.execute(link);
        stat2.executeQuery("SELECT * FROM LINKED.TEST1");

        stat.execute("CREATE TABLE TEST2(ID INT PRIMARY KEY)");
        stat2.execute(link);
        stat2.executeQuery("SELECT * FROM LINKED.TEST1");
        stat2.executeQuery("SELECT * FROM LINKED.TEST2");

        conn.close();
        conn2.close();
    }

    private void testLinkTable() throws SQLException {
        if (config.memory || config.networked || config.reopen) {
            return;
        }

        deleteDb("linked1");
        deleteDb("linked2");
        org.h2.Driver.load();

        String url1 = getURL("linked1", true);
        String url2 = getURL("linked2", true);

        Connection conn = DriverManager.getConnection(url1, "sa1", "abc abc");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TEMP TABLE TEST_TEMP(ID INT PRIMARY KEY)");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, " +
                "NAME VARCHAR(200), XT TINYINT, XD DECIMAL(10,2), " +
                "XTS TIMESTAMP, XBY BINARY(255), XBO BIT, XSM SMALLINT, " +
                "XBI BIGINT, XBL BLOB, XDA DATE, XTI TIME, XCL CLOB, XDO DOUBLE)");
        stat.execute("CREATE INDEX IDXNAME ON TEST(NAME)");
        stat.execute("INSERT INTO TEST VALUES(0, NULL, NULL, NULL, NULL, " +
                "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello', -1, 10.30, " +
                "'2001-02-03 11:22:33.4455', X'FF0102', TRUE, 3000, " +
                "1234567890123456789, X'1122AA', DATE '0002-01-01', " +
                "TIME '00:00:00', 'J\u00fcrg', 2.25)");
        testRow(stat, "TEST");
        stat.execute("INSERT INTO TEST VALUES(2, 'World', 30, 100.05, " +
                "'2005-12-31 12:34:56.789', X'FFEECC33', FALSE, 1, " +
                "-1234567890123456789, X'4455FF', DATE '9999-12-31', " +
                "TIME '23:59:59', 'George', -2.5)");
        testRow(stat, "TEST");
        stat.execute("SELECT * FROM TEST_TEMP");
        conn.close();

        conn = DriverManager.getConnection(url1, "sa1", "abc abc");
        stat = conn.createStatement();
        testRow(stat, "TEST");
        assertThrows(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, stat).
                execute("SELECT * FROM TEST_TEMP");
        conn.close();

        conn = DriverManager.getConnection(url2, "sa2", "def def");
        stat = conn.createStatement();
        stat.execute("CREATE LINKED TABLE IF NOT EXISTS " +
                "LINK_TEST('org.h2.Driver', '" + url1 +
                "', 'sa1', 'abc abc', 'TEST')");
        stat.execute("CREATE LINKED TABLE IF NOT EXISTS " +
                "LINK_TEST('org.h2.Driver', '" + url1 +
                "', 'sa1', 'abc abc', 'TEST')");
        testRow(stat, "LINK_TEST");
        ResultSet rs = stat.executeQuery("SELECT * FROM LINK_TEST");
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(10, meta.getPrecision(1));
        assertEquals(200, meta.getPrecision(2));

        conn.close();
        conn = DriverManager.getConnection(url2, "sa2", "def def");
        stat = conn.createStatement();

        stat.execute("INSERT INTO LINK_TEST VALUES(3, 'Link Test', " +
                "30, 100.05, '2005-12-31 12:34:56.789', X'FFEECC33', " +
                "FALSE, 1, -1234567890123456789, X'4455FF', " +
                "DATE '9999-12-31', TIME '23:59:59', 'George', -2.5)");

        rs = stat.executeQuery("SELECT COUNT(*) FROM LINK_TEST");
        rs.next();
        assertEquals(4, rs.getInt(1));

        rs = stat.executeQuery("SELECT COUNT(*) FROM LINK_TEST WHERE NAME='Link Test'");
        rs.next();
        assertEquals(1, rs.getInt(1));

        int uc = stat.executeUpdate("DELETE FROM LINK_TEST WHERE ID=3");
        assertEquals(1, uc);

        rs = stat.executeQuery("SELECT COUNT(*) FROM LINK_TEST");
        rs.next();
        assertEquals(3, rs.getInt(1));

        rs = stat.executeQuery("SELECT * FROM " +
                "INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='LINK_TEST'");
        rs.next();
        assertEquals("TABLE LINK", rs.getString("TABLE_TYPE"));

        rs.next();
        rs = stat.executeQuery("SELECT * FROM LINK_TEST WHERE ID=0");
        rs.next();
        assertTrue(rs.getString("NAME") == null && rs.wasNull());
        assertTrue(rs.getString("XT") == null && rs.wasNull());
        assertTrue(rs.getInt("ID") == 0 && !rs.wasNull());
        assertTrue(rs.getBigDecimal("XD") == null && rs.wasNull());
        assertTrue(rs.getTimestamp("XTS") == null && rs.wasNull());
        assertTrue(rs.getBytes("XBY") == null && rs.wasNull());
        assertTrue(!rs.getBoolean("XBO") && rs.wasNull());
        assertTrue(rs.getShort("XSM") == 0 && rs.wasNull());
        assertTrue(rs.getLong("XBI") == 0 && rs.wasNull());
        assertTrue(rs.getString("XBL") == null && rs.wasNull());
        assertTrue(rs.getString("XDA") == null && rs.wasNull());
        assertTrue(rs.getString("XTI") == null && rs.wasNull());
        assertTrue(rs.getString("XCL") == null && rs.wasNull());
        assertTrue(rs.getString("XDO") == null && rs.wasNull());
        assertFalse(rs.next());

        stat.execute("DROP TABLE LINK_TEST");

        stat.execute("CREATE LINKED TABLE LINK_TEST('org.h2.Driver', '" + url1
                + "', 'sa1', 'abc abc', '(SELECT COUNT(*) FROM TEST)')");
        rs = stat.executeQuery("SELECT * FROM LINK_TEST");
        rs.next();
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());

        conn.close();

        deleteDb("linked1");
        deleteDb("linked2");
    }

    private void testRow(Statement stat, String name) throws SQLException {
        ResultSet rs = stat.executeQuery("SELECT * FROM " + name + " WHERE ID=1");
        rs.next();
        assertEquals("Hello", rs.getString("NAME"));
        assertEquals(-1, rs.getByte("XT"));
        BigDecimal bd = rs.getBigDecimal("XD");
        assertTrue(bd.equals(new BigDecimal("10.30")));
        Timestamp ts = rs.getTimestamp("XTS");
        String s = ts.toString();
        assertEquals("2001-02-03 11:22:33.4455", s);
        assertTrue(ts.equals(Timestamp.valueOf("2001-02-03 11:22:33.4455")));
        assertEquals(new byte[] { (byte) 255, (byte) 1, (byte) 2 }, rs.getBytes("XBY"));
        assertTrue(rs.getBoolean("XBO"));
        assertEquals(3000, rs.getShort("XSM"));
        assertEquals(1234567890123456789L, rs.getLong("XBI"));
        assertEquals("1122aa", rs.getString("XBL"));
        assertEquals("0002-01-01", rs.getString("XDA"));
        assertEquals("00:00:00", rs.getString("XTI"));
        assertEquals("J\u00fcrg", rs.getString("XCL"));
        assertEquals("2.25", rs.getString("XDO"));
    }

    private void testCachingResults() throws SQLException {
        org.h2.Driver.load();
        Connection ca = DriverManager.getConnection(
                "jdbc:h2:mem:one", "sa", "sa");
        Connection cb = DriverManager.getConnection(
                "jdbc:h2:mem:two", "sa", "sa");

        Statement sa = ca.createStatement();
        Statement sb = cb.createStatement();
        sa.execute("CREATE TABLE TEST(ID VARCHAR)");
        sa.execute("INSERT INTO TEST (ID) VALUES('abc')");
        sb.execute("CREATE LOCAL TEMPORARY LINKED TABLE T" +
                "(NULL, 'jdbc:h2:mem:one', 'sa', 'sa', 'TEST')");

        PreparedStatement paData = ca.prepareStatement(
                "select id from TEST where id = ?");
        PreparedStatement pbData = cb.prepareStatement(
                "select id from T where id = ?");
        PreparedStatement paCount = ca.prepareStatement(
                "select count(*) from TEST");
        PreparedStatement pbCount = cb.prepareStatement(
                "select count(*) from T");

        // Direct query => Result 1
        testCachingResultsCheckResult(paData, 1, "abc");
        testCachingResultsCheckResult(paCount, 1);

        // Via linked table => Result 1
        testCachingResultsCheckResult(pbData, 1, "abc");
        testCachingResultsCheckResult(pbCount, 1);

        sa.execute("INSERT INTO TEST (ID) VALUES('abc')");

        // Direct query => Result 2
        testCachingResultsCheckResult(paData, 2, "abc");
        testCachingResultsCheckResult(paCount, 2);

        // Via linked table => Result must be 2
        testCachingResultsCheckResult(pbData, 2, "abc");
        testCachingResultsCheckResult(pbCount, 2);

        ca.close();
        cb.close();
    }

    private void testCachingResultsCheckResult(PreparedStatement ps,
            int expected) throws SQLException {
        ResultSet rs = ps.executeQuery();
        rs.next();
        assertEquals(expected, rs.getInt(1));
    }

    private void testCachingResultsCheckResult(PreparedStatement ps,
            int expected, String value) throws SQLException {
        ps.setString(1, value);
        ResultSet rs = ps.executeQuery();
        int counter = 0;
        while (rs.next()) {
            counter++;
            String result = rs.getString(1);
            assertEquals(result, value);
        }
        assertEquals(expected, counter);
    }

    private void testLinkedTableInReadOnlyDb() throws SQLException {
        if (config.memory || config.networked || config.googleAppEngine) {
            return;
        }

        deleteDb("testLinkedTableInReadOnlyDb");
        org.h2.Driver.load();

        Connection memConn = DriverManager.getConnection(
                "jdbc:h2:mem:one", "sa", "sa");
        Statement memStat = memConn.createStatement();
        memStat.execute("CREATE TABLE TEST(ID VARCHAR)");

        String url1 = getURL("testLinkedTableInReadOnlyDb", true);
        Connection conn = DriverManager.getConnection(url1, "sa1", "abc abc");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY)");
        conn.close();

        for (String file : FileUtils.newDirectoryStream(getBaseDir())) {
            String name = FileUtils.getName(file);
            if ((name.startsWith("testLinkedTableInReadOnlyDb")) &&
                    (!name.endsWith(".trace.db"))) {
                FileUtils.setReadOnly(file);
                boolean isReadOnly = !FileUtils.canWrite(file);
                if (!isReadOnly) {
                    fail("File " + file + " is not read only. Can't test it.");
                }
            }
        }

        // Now it's read only
        conn = DriverManager.getConnection(url1, "sa1", "abc abc");
        stat = conn.createStatement();
        stat.execute("CREATE LOCAL TEMPORARY LINKED TABLE T" +
                "(NULL, 'jdbc:h2:mem:one', 'sa', 'sa', 'TEST')");
        // This is valid because it's a linked table
        stat.execute("INSERT INTO T VALUES('abc')");

        conn.close();
        memConn.close();

        deleteDb("testLinkedTableInReadOnlyDb");
    }

    private void testGeometry() throws SQLException {
        if (!config.mvStore && config.mvcc) {
            return;
        }
        if (config.memory && config.mvcc) {
            return;
        }
        if (DataType.GEOMETRY_CLASS == null) {
            return;
        }
        org.h2.Driver.load();
        Connection ca = DriverManager.getConnection("jdbc:h2:mem:one", "sa", "sa");
        Connection cb = DriverManager.getConnection("jdbc:h2:mem:two", "sa", "sa");
        Statement sa = ca.createStatement();
        Statement sb = cb.createStatement();
        sa.execute("CREATE TABLE TEST(ID SERIAL, the_geom geometry)");
        sa.execute("INSERT INTO TEST(THE_GEOM) VALUES('POINT (1 1)')");
        String sql = "CREATE LINKED TABLE T(NULL, " +
                "'jdbc:h2:mem:one', 'sa', 'sa', 'TEST') READONLY";
        sb.execute(sql);
        try (ResultSet rs = sb.executeQuery("SELECT * FROM T")) {
            assertTrue(rs.next());
            assertEquals("POINT (1 1)", rs.getString("THE_GEOM"));
        }
        sb.execute("DROP TABLE T");
        ca.close();
        cb.close();
    }
}
