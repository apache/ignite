/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;
import org.h2.tools.Server;

/**
 * Tests the PostgreSQL server protocol compliant implementation.
 */
public class TestPgServer extends TestBase {

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
        config.multiThreaded = true;
        config.memory = true;
        config.mvStore = true;
        config.mvcc = true;
        // testPgAdapter() starts server by itself without a wait so run it first
        testPgAdapter();
        testLowerCaseIdentifiers();
        testKeyAlias();
        testKeyAlias();
        testCancelQuery();
        testBinaryTypes();
        testDateTime();
        testPrepareWithUnspecifiedType();
    }

    private void testLowerCaseIdentifiers() throws SQLException {
        if (!getPgJdbcDriver()) {
            return;
        }
        deleteDb("pgserver");
        Connection conn = getConnection(
                "mem:pgserver;DATABASE_TO_UPPER=false", "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int, name varchar(255))");
        Server server = createPgServer("-baseDir", getBaseDir(),
                "-pgPort", "5535", "-pgDaemon", "-key", "pgserver",
                "mem:pgserver");
        try {
            Connection conn2;
            conn2 = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5535/pgserver", "sa", "sa");
            stat = conn2.createStatement();
            stat.execute("select * from test");
            conn2.close();
        } finally {
            server.stop();
        }
        conn.close();
        deleteDb("pgserver");
    }

    private boolean getPgJdbcDriver() {
        try {
            Class.forName("org.postgresql.Driver");
            return true;
        } catch (ClassNotFoundException e) {
            println("PostgreSQL JDBC driver not found - PgServer not tested");
            return false;
        }
    }

    private Server createPgServer(String... args) throws SQLException {
        Server server = Server.createPgServer(args);
        int failures = 0;
        for (;;) {
            try {
                server.start();
                return server;
            } catch (SQLException e) {
                // the sleeps are too mitigate "port in use" exceptions on Jenkins
                if (e.getErrorCode() != ErrorCode.EXCEPTION_OPENING_PORT_2 || ++failures > 10) {
                    throw e;
                }
                println("Sleeping");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e2) {
                    throw new RuntimeException(e2);
                }
            }
        }
    }

    private void testPgAdapter() throws SQLException {
        deleteDb("pgserver");
        Server server = Server.createPgServer(
                "-baseDir", getBaseDir(), "-pgPort", "5535", "-pgDaemon");
        assertEquals(5535, server.getPort());
        assertEquals("Not started", server.getStatus());
        server.start();
        assertStartsWith(server.getStatus(), "PG server running at pg://");
        try {
            if (getPgJdbcDriver()) {
                testPgClient();
            }
        } finally {
            server.stop();
        }
    }

    private void testCancelQuery() throws Exception {
        if (!getPgJdbcDriver()) {
            return;
        }

        Server server = createPgServer(
                "-pgPort", "5535", "-pgDaemon", "-key", "pgserver", "mem:pgserver");

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Connection conn = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5535/pgserver", "sa", "sa");
            final Statement stat = conn.createStatement();
            stat.execute("create alias sleep for \"java.lang.Thread.sleep\"");

            // create a table with 200 rows (cancel interval is 127)
            stat.execute("create table test(id int)");
            for (int i = 0; i < 200; i++) {
                stat.execute("insert into test (id) values (rand())");
            }

            Future<Boolean> future = executor.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws SQLException {
                    return stat.execute("select id, sleep(5) from test");
                }
            });

            // give it a little time to start and then cancel it
            Thread.sleep(100);
            stat.cancel();

            try {
                future.get();
                throw new IllegalStateException();
            } catch (ExecutionException e) {
                assertStartsWith(e.getCause().getMessage(),
                        "ERROR: canceling statement due to user request");
            } finally {
                conn.close();
            }
        } finally {
            server.stop();
            executor.shutdown();
        }
        deleteDb("pgserver");
    }

    private void testPgClient() throws SQLException {
        Connection conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5535/pgserver", "sa", "sa");
        Statement stat = conn.createStatement();
        assertThrows(SQLException.class, stat).
                execute("select ***");
        stat.execute("create user test password 'test'");
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("create index idx_test_name on test(name, id)");
        stat.execute("grant all on test to test");
        stat.close();
        conn.close();

        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5535/pgserver", "test", "test");
        stat = conn.createStatement();
        ResultSet rs;

        stat.execute("prepare test(int, int) as select ?1*?2");
        rs = stat.executeQuery("execute test(3, 2)");
        rs.next();
        assertEquals(6, rs.getInt(1));
        stat.execute("deallocate test");

        PreparedStatement prep;
        prep = conn.prepareStatement("select * from test where name = ?");
        prep.setNull(1, Types.VARCHAR);
        rs = prep.executeQuery();
        assertFalse(rs.next());

        prep = conn.prepareStatement("insert into test values(?, ?)");
        ParameterMetaData meta = prep.getParameterMetaData();
        assertEquals(2, meta.getParameterCount());
        prep.setInt(1, 1);
        prep.setString(2, "Hello");
        prep.execute();
        rs = stat.executeQuery("select * from test");
        rs.next();

        ResultSetMetaData rsMeta = rs.getMetaData();
        assertEquals(Types.INTEGER, rsMeta.getColumnType(1));
        assertEquals(Types.VARCHAR, rsMeta.getColumnType(2));

        prep.close();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());
        prep = conn.prepareStatement(
                "select * from test " +
                "where id = ? and name = ?");
        prep.setInt(1, 1);
        prep.setString(2, "Hello");
        rs = prep.executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());
        rs.close();
        DatabaseMetaData dbMeta = conn.getMetaData();
        rs = dbMeta.getTables(null, null, "TEST", null);
        rs.next();
        assertEquals("TEST", rs.getString("TABLE_NAME"));
        assertFalse(rs.next());
        rs = dbMeta.getColumns(null, null, "TEST", null);
        rs.next();
        assertEquals("ID", rs.getString("COLUMN_NAME"));
        rs.next();
        assertEquals("NAME", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());
        rs = dbMeta.getIndexInfo(null, null, "TEST", false, false);
        // index info is currently disabled
        // rs.next();
        // assertEquals("TEST", rs.getString("TABLE_NAME"));
        // rs.next();
        // assertEquals("TEST", rs.getString("TABLE_NAME"));
        assertFalse(rs.next());
        rs = stat.executeQuery(
                "select version(), pg_postmaster_start_time(), current_schema()");
        rs.next();
        String s = rs.getString(1);
        assertContains(s, "H2");
        assertContains(s, "PostgreSQL");
        s = rs.getString(2);
        s = rs.getString(3);
        assertEquals(s, "PUBLIC");
        assertFalse(rs.next());

        conn.setAutoCommit(false);
        stat.execute("delete from test");
        conn.rollback();
        stat.execute("update test set name = 'Hallo'");
        conn.commit();
        rs = stat.executeQuery("select * from test order by id");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hallo", rs.getString(2));
        assertFalse(rs.next());

        rs = stat.executeQuery("select id, name, pg_get_userbyid(id) " +
                "from information_schema.users order by id");
        rs.next();
        assertEquals(rs.getString(2), rs.getString(3));
        assertFalse(rs.next());
        rs.close();

        rs = stat.executeQuery("select currTid2('x', 1)");
        rs.next();
        assertEquals(1, rs.getInt(1));

        rs = stat.executeQuery("select has_table_privilege('TEST', 'READ')");
        rs.next();
        assertTrue(rs.getBoolean(1));

        rs = stat.executeQuery("select has_database_privilege(1, 'READ')");
        rs.next();
        assertTrue(rs.getBoolean(1));


        rs = stat.executeQuery("select pg_get_userbyid(-1)");
        rs.next();
        assertEquals(null, rs.getString(1));

        rs = stat.executeQuery("select pg_encoding_to_char(0)");
        rs.next();
        assertEquals("SQL_ASCII", rs.getString(1));

        rs = stat.executeQuery("select pg_encoding_to_char(6)");
        rs.next();
        assertEquals("UTF8", rs.getString(1));

        rs = stat.executeQuery("select pg_encoding_to_char(8)");
        rs.next();
        assertEquals("LATIN1", rs.getString(1));

        rs = stat.executeQuery("select pg_encoding_to_char(20)");
        rs.next();
        assertEquals("UTF8", rs.getString(1));

        rs = stat.executeQuery("select pg_encoding_to_char(40)");
        rs.next();
        assertEquals("", rs.getString(1));

        rs = stat.executeQuery("select pg_get_oid('\"WRONG\"')");
        rs.next();
        assertEquals(0, rs.getInt(1));

        rs = stat.executeQuery("select pg_get_oid('TEST')");
        rs.next();
        assertTrue(rs.getInt(1) > 0);

        rs = stat.executeQuery("select pg_get_indexdef(0, 0, false)");
        rs.next();
        assertEquals("", rs.getString(1));

        rs = stat.executeQuery("select id from information_schema.indexes " +
                "where index_name='IDX_TEST_NAME'");
        rs.next();
        int indexId = rs.getInt(1);

        rs = stat.executeQuery("select pg_get_indexdef("+indexId+", 0, false)");
        rs.next();
        assertEquals(
                "CREATE INDEX PUBLIC.IDX_TEST_NAME ON PUBLIC.TEST(NAME, ID)",
                rs.getString(1));
        rs = stat.executeQuery("select pg_get_indexdef("+indexId+", null, false)");
        rs.next();
        assertEquals(
                "CREATE INDEX PUBLIC.IDX_TEST_NAME ON PUBLIC.TEST(NAME, ID)",
                rs.getString(1));
        rs = stat.executeQuery("select pg_get_indexdef("+indexId+", 1, false)");
        rs.next();
        assertEquals("NAME", rs.getString(1));
        rs = stat.executeQuery("select pg_get_indexdef("+indexId+", 2, false)");
        rs.next();
        assertEquals("ID", rs.getString(1));

        conn.close();
    }

    private void testKeyAlias() throws SQLException {
        if (!getPgJdbcDriver()) {
            return;
        }
        Server server = createPgServer(
                "-pgPort", "5535", "-pgDaemon", "-key", "pgserver", "mem:pgserver");
        try {
            Connection conn = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5535/pgserver", "sa", "sa");
            Statement stat = conn.createStatement();

            // confirm that we've got the in memory implementation
            // by creating a table and checking flags
            stat.execute("create table test(id int primary key, name varchar)");
            ResultSet rs = stat.executeQuery(
                    "select storage_type from information_schema.tables " +
                    "where table_name = 'TEST'");
            assertTrue(rs.next());
            assertEquals("MEMORY", rs.getString(1));

            conn.close();
        } finally {
            server.stop();
        }
    }

    private void testBinaryTypes() throws SQLException {
        if (!getPgJdbcDriver()) {
            return;
        }

        Server server = createPgServer(
                "-pgPort", "5535", "-pgDaemon", "-key", "pgserver", "mem:pgserver");
        try {
            Properties props = new Properties();
            props.setProperty("user", "sa");
            props.setProperty("password", "sa");
            // force binary
            props.setProperty("prepareThreshold", "-1");

            Connection conn = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5535/pgserver", props);
            Statement stat = conn.createStatement();

            stat.execute(
                    "create table test(x1 varchar, x2 int, " +
                    "x3 smallint, x4 bigint, x5 double, x6 float, " +
                    "x7 real, x8 boolean, x9 char, x10 bytea, " +
                    "x11 date, x12 time, x13 timestamp, x14 numeric)");

            PreparedStatement ps = conn.prepareStatement(
                    "insert into test values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            ps.setString(1, "test");
            ps.setInt(2, 12345678);
            ps.setShort(3, (short) 12345);
            ps.setLong(4, 1234567890123L);
            ps.setDouble(5, 123.456);
            ps.setFloat(6, 123.456f);
            ps.setFloat(7, 123.456f);
            ps.setBoolean(8, true);
            ps.setByte(9, (byte) 0xfe);
            ps.setBytes(10, new byte[] { 'a', (byte) 0xfe, '\127' });
            ps.setDate(11, Date.valueOf("2015-01-31"));
            ps.setTime(12, Time.valueOf("20:11:15"));
            ps.setTimestamp(13, Timestamp.valueOf("2001-10-30 14:16:10.111"));
            ps.setBigDecimal(14, new BigDecimal("12345678901234567890.12345"));
            ps.execute();
            for (int i = 1; i <= 14; i++) {
                ps.setNull(i, Types.NULL);
            }
            ps.execute();

            ResultSet rs = stat.executeQuery("select * from test");
            assertTrue(rs.next());
            assertEquals("test", rs.getString(1));
            assertEquals(12345678, rs.getInt(2));
            assertEquals((short) 12345, rs.getShort(3));
            assertEquals(1234567890123L, rs.getLong(4));
            assertEquals(123.456, rs.getDouble(5));
            assertEquals(123.456f, rs.getFloat(6));
            assertEquals(123.456f, rs.getFloat(7));
            assertEquals(true, rs.getBoolean(8));
            assertEquals((byte) 0xfe, rs.getByte(9));
            assertEquals(new byte[] { 'a', (byte) 0xfe, '\127' },
                    rs.getBytes(10));
            assertEquals(Date.valueOf("2015-01-31"), rs.getDate(11));
            assertEquals(Time.valueOf("20:11:15"), rs.getTime(12));
            assertEquals(Timestamp.valueOf("2001-10-30 14:16:10.111"), rs.getTimestamp(13));
            assertEquals(new BigDecimal("12345678901234567890.12345"), rs.getBigDecimal(14));
            assertTrue(rs.next());
            for (int i = 1; i <= 14; i++) {
                assertNull(rs.getObject(i));
            }
            assertFalse(rs.next());

            conn.close();
        } finally {
            server.stop();
        }
    }

    private void testDateTime() throws SQLException {
        if (!getPgJdbcDriver()) {
            return;
        }

        Server server = createPgServer(
                "-pgPort", "5535", "-pgDaemon", "-key", "pgserver", "mem:pgserver");
        try {
            Properties props = new Properties();
            props.setProperty("user", "sa");
            props.setProperty("password", "sa");
            // force binary
            props.setProperty("prepareThreshold", "-1");

            Connection conn = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5535/pgserver", props);
            Statement stat = conn.createStatement();

            stat.execute(
                    "create table test(x1 date, x2 time, x3 timestamp)");

            Date[] dates = { null, Date.valueOf("2017-02-20"),
                    Date.valueOf("1970-01-01"), Date.valueOf("1969-12-31"),
                    Date.valueOf("1940-01-10"), Date.valueOf("1950-11-10"),
                    Date.valueOf("1500-01-01")};
            Time[] times = { null, Time.valueOf("14:15:16"),
                    Time.valueOf("00:00:00"), Time.valueOf("23:59:59"),
                    Time.valueOf("00:10:59"), Time.valueOf("08:30:42"),
                    Time.valueOf("10:00:00")};
            Timestamp[] timestamps = { null, Timestamp.valueOf("2017-02-20 14:15:16.763"),
                    Timestamp.valueOf("1970-01-01 00:00:00"), Timestamp.valueOf("1969-12-31 23:59:59"),
                    Timestamp.valueOf("1940-01-10 00:10:59"), Timestamp.valueOf("1950-11-10 08:30:42.12"),
                    Timestamp.valueOf("1500-01-01 10:00:10")};
            int count = dates.length;

            PreparedStatement ps = conn.prepareStatement(
                    "insert into test values (?,?,?)");
                for (int i = 0; i < count; i++) {
                ps.setDate(1, dates[i]);
                ps.setTime(2, times[i]);
                ps.setTimestamp(3, timestamps[i]);
                ps.execute();
            }

            ResultSet rs = stat.executeQuery("select * from test");
            for (int i = 0; i < count; i++) {
                assertTrue(rs.next());
                assertEquals(dates[i], rs.getDate(1));
                assertEquals(times[i], rs.getTime(2));
                assertEquals(timestamps[i], rs.getTimestamp(3));
            }
            assertFalse(rs.next());

            conn.close();
        } finally {
            server.stop();
        }
    }

    private void testPrepareWithUnspecifiedType() throws Exception {
        if (!getPgJdbcDriver()) {
            return;
        }

        Server server = createPgServer(
                "-pgPort", "5535", "-pgDaemon", "-key", "pgserver", "mem:pgserver");
        try {
            Properties props = new Properties();

            props.setProperty("user", "sa");
            props.setProperty("password", "sa");
            // force server side prepare
            props.setProperty("prepareThreshold", "1");

            Connection conn = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5535/pgserver", props);

            Statement stmt = conn.createStatement();
            stmt.executeUpdate("create table t1 (id integer, value timestamp)");
            stmt.close();

            PreparedStatement pstmt = conn.prepareStatement("insert into t1 values(100500, ?)");
            // assertTrue(((PGStatement) pstmt).isUseServerPrepare());
            assertEquals(Types.TIMESTAMP, pstmt.getParameterMetaData().getParameterType(1));

            Timestamp t = new Timestamp(System.currentTimeMillis());
            pstmt.setObject(1, t);
            assertEquals(1, pstmt.executeUpdate());
            pstmt.close();

            pstmt = conn.prepareStatement("SELECT * FROM t1 WHERE value = ?");
            assertEquals(Types.TIMESTAMP, pstmt.getParameterMetaData().getParameterType(1));

            pstmt.setObject(1, t);
            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(100500, rs.getInt(1));
            rs.close();
            pstmt.close();

            conn.close();
        } finally {
            server.stop();
        }
    }
}
