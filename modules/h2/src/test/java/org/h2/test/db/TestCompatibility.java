/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import org.h2.api.ErrorCode;
import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * Tests the compatibility with other databases.
 */
public class TestCompatibility extends TestDb {

    private Connection conn;

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
        deleteDb("compatibility");

        testCaseSensitiveIdentifiers();
        testKeyAsColumnInMySQLMode();

        conn = getConnection("compatibility");
        testDomain();
        testColumnAlias();
        testUniqueIndexSingleNull();
        testUniqueIndexOracle();
        testPostgreSQL();
        testHsqlDb();
        testMySQL();
        testDB2();
        testDerby();
        testSybaseAndMSSQLServer();
        testIgnite();

        testUnknownSet();

        conn.close();
        testIdentifiers();
        deleteDb("compatibility");

        testUnknownURL();
    }

    private void testKeyAsColumnInMySQLMode() throws SQLException {
        Connection c = getConnection("compatibility;MODE=MYSQL");
        Statement stat = c.createStatement();
        stat.execute("create table test(id int primary key, key varchar)");
        stat.execute("drop table test");
        c.close();
    }

    private void testCaseSensitiveIdentifiers() throws SQLException {
        Connection c = getConnection("compatibility;DATABASE_TO_UPPER=FALSE;CASE_INSENSITIVE_IDENTIFIERS=TRUE");
        Statement stat = c.createStatement();
        stat.execute("create table test(id int primary key, name varchar) " +
                "as select 1, 'hello'");
        assertThrows(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, stat).
            execute("create table test(id int primary key, name varchar)");
        assertThrows(ErrorCode.DUPLICATE_COLUMN_NAME_1, stat).
            execute("alter table test add column Name varchar");
        ResultSet rs;

        DatabaseMetaData meta = c.getMetaData();
        rs = meta.getTables(null, null, "test", null);
        assertTrue(rs.next());
        assertEquals("test", rs.getString("TABLE_NAME"));

        rs = stat.executeQuery("select id, name from test");
        assertEquals("id", rs.getMetaData().getColumnLabel(1));
        assertEquals("name", rs.getMetaData().getColumnLabel(2));

        rs = stat.executeQuery("select Id, Name from Test");
        assertEquals("id", rs.getMetaData().getColumnLabel(1));
        assertEquals("name", rs.getMetaData().getColumnLabel(2));

        rs = stat.executeQuery("select ID, NAME from TEST");
        assertEquals("id", rs.getMetaData().getColumnLabel(1));
        assertEquals("name", rs.getMetaData().getColumnLabel(2));

        stat.execute("select COUNT(*), count(*), Count(*), Sum(id) from test");

        stat.execute("select LENGTH(name), length(name), Length(name) from test");

        stat.execute("select t.id from test t group by t.id");
        stat.execute("select id from test t group by t.id");
        stat.execute("select id from test group by ID");
        stat.execute("select id as c from test group by c");
        stat.execute("select t.id from test t group by T.ID");
        stat.execute("select id from test t group by T.ID");

        stat.execute("drop table test");

        rs = stat.executeQuery("select 1e10, 1000000000000000000000e10, 0xfAfBl");
        assertTrue(rs.next());
        assertEquals(1e10, rs.getDouble(1));
        assertEquals(1000000000000000000000e10, rs.getDouble(2));
        assertEquals(0xfafbL, rs.getLong(3));
        assertFalse(rs.next());

        stat.execute("create table \"t 1\" (a int, b int)");
        stat.execute("create view v as select * from \"t 1\"");
        stat.executeQuery("select * from v").close();
        stat.execute("drop view v");
        stat.execute("drop table \"t 1\"");

        c.close();
    }

    private void testDomain() throws SQLException {
        if (config.memory) {
            return;
        }
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key) as select 1");
        assertThrows(ErrorCode.DOMAIN_ALREADY_EXISTS_1, stat).
                execute("create domain int as varchar");
        conn.close();
        conn = getConnection("compatibility");
        stat = conn.createStatement();
        stat.execute("insert into test values(2)");
        stat.execute("drop table test");
    }

    private void testColumnAlias() throws SQLException {
        Statement stat = conn.createStatement();
        String[] modes = { "PostgreSQL", "MySQL", "HSQLDB", "MSSQLServer",
                "Derby", "Oracle", "Regular" };
        String columnAlias;
        columnAlias = "MySQL,Regular";
        stat.execute("CREATE TABLE TEST(ID INT)");
        for (String mode : modes) {
            stat.execute("SET MODE " + mode);
            ResultSet rs = stat.executeQuery("SELECT ID I FROM TEST");
            ResultSetMetaData meta = rs.getMetaData();
            String columnName = meta.getColumnName(1);
            String tableName = meta.getTableName(1);
            if ("ID".equals(columnName) && "TEST".equals(tableName)) {
                assertTrue(mode + " mode should not support columnAlias",
                        columnAlias.contains(mode));
            } else if ("I".equals(columnName) && tableName.equals("")) {
                assertTrue(mode + " mode should support columnAlias",
                        columnAlias.indexOf(mode) < 0);
            } else {
                fail();
            }
        }
        stat.execute("DROP TABLE TEST");
    }

    private void testUniqueIndexSingleNull() throws SQLException {
        Statement stat = conn.createStatement();
        String[] modes = { "PostgreSQL", "MySQL", "HSQLDB", "MSSQLServer",
                "Derby", "Oracle", "Regular" };
        String multiNull = "PostgreSQL,MySQL,Oracle,Regular";
        for (String mode : modes) {
            stat.execute("SET MODE " + mode);
            stat.execute("CREATE TABLE TEST(ID INT)");
            stat.execute("CREATE UNIQUE INDEX IDX_ID_U ON TEST(ID)");
            try {
                stat.execute("INSERT INTO TEST VALUES(1), (2), (NULL), (NULL)");
                assertTrue(mode + " mode should not support multiple NULL",
                        multiNull.contains(mode));
            } catch (SQLException e) {
                assertTrue(mode + " mode should support multiple NULL",
                        multiNull.indexOf(mode) < 0);
            }
            stat.execute("DROP TABLE TEST");
        }
    }

    private void testUniqueIndexOracle() throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("SET MODE ORACLE");
        stat.execute("create table t2(c1 int, c2 int)");
        stat.execute("create unique index i2 on t2(c1, c2)");
        stat.execute("insert into t2 values (null, 1)");
        assertThrows(ErrorCode.DUPLICATE_KEY_1, stat).
                execute("insert into t2 values (null, 1)");
        stat.execute("insert into t2 values (null, null)");
        stat.execute("insert into t2 values (null, null)");
        stat.execute("insert into t2 values (1, null)");
        assertThrows(ErrorCode.DUPLICATE_KEY_1, stat).
                execute("insert into t2 values (1, null)");
        stat.execute("DROP TABLE T2");
    }

    private void testHsqlDb() throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("set mode hsqldb");
        testLog(Math.log(10), stat);

        stat.execute("DROP TABLE TEST IF EXISTS; " +
                "CREATE TABLE TEST(ID INT PRIMARY KEY); ");
        stat.execute("CALL CURRENT_TIME");
        stat.execute("CALL CURRENT_TIMESTAMP");
        stat.execute("CALL CURRENT_DATE");
        stat.execute("CALL SYSDATE");
        stat.execute("CALL TODAY");

        stat.execute("DROP TABLE TEST IF EXISTS");
        stat.execute("CREATE TABLE TEST(ID INT)");
        stat.execute("INSERT INTO TEST VALUES(1)");
        PreparedStatement prep = conn.prepareStatement(
                "SELECT LIMIT ? 1 ID FROM TEST");
        prep.setInt(1, 2);
        prep.executeQuery();
        stat.execute("DROP TABLE TEST IF EXISTS");

        stat.execute("DROP TABLE TEST IF EXISTS");
        stat.execute("CREATE TABLE TEST(ID INT)");
        stat.executeQuery("SELECT * FROM TEST WHERE ID IN ()");
        stat.execute("DROP TABLE TEST IF EXISTS");
    }

    private void testLog(double expected, Statement stat) throws SQLException {
        stat.execute("create table log(id int)");
        stat.execute("insert into log values(1)");
        ResultSet rs = stat.executeQuery("select log(10) from log");
        rs.next();
        assertEquals((int) (expected * 100), (int) (rs.getDouble(1) * 100));
        rs = stat.executeQuery("select ln(10) from log");
        rs.next();
        assertEquals((int) (Math.log(10) * 100), (int) (rs.getDouble(1) * 100));
        stat.execute("drop table log");
    }

    private void testPostgreSQL() throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("SET MODE PostgreSQL");
        testLog(Math.log10(10), stat);

        assertResult("ABC", stat, "SELECT SUBSTRING('ABCDEF' FOR 3)");
        assertResult("ABCD", stat, "SELECT SUBSTRING('0ABCDEF' FROM 2 FOR 4)");

        /* --------- Behaviour of CHAR(N) --------- */

        /* Test right-padding of CHAR(N) at INSERT */
        stat.execute("CREATE TABLE TEST(CH CHAR(10))");
        stat.execute("INSERT INTO TEST (CH) VALUES ('Hello')");
        assertResult("Hello     ", stat, "SELECT CH FROM TEST");

        /* Test that WHERE clauses accept unpadded values and will pad before comparison */
        assertResult("Hello     ", stat, "SELECT CH FROM TEST WHERE CH = 'Hello'");

        /* Test CHAR which is identical to CHAR(1) */
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(CH CHAR)");
        stat.execute("INSERT INTO TEST (CH) VALUES ('')");
        assertResult(" ", stat, "SELECT CH FROM TEST");
        assertResult(" ", stat, "SELECT CH FROM TEST WHERE CH = ''");

        /* Test that excessive spaces are trimmed */
        stat.execute("DELETE FROM TEST");
        stat.execute("INSERT INTO TEST (CH) VALUES ('1   ')");
        assertResult("1", stat, "SELECT CH FROM TEST");
        assertResult("1", stat, "SELECT CH FROM TEST WHERE CH = '1      '");

        /* Test that we do not trim too far */
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(CH CHAR(2))");
        stat.execute("INSERT INTO TEST (CH) VALUES ('1   ')");
        assertResult("1 ", stat, "SELECT CH FROM TEST");
        assertResult("1 ", stat, "SELECT CH FROM TEST WHERE CH = '1      '");

        /* --------- Disallowed column types --------- */

        String[] DISALLOWED_TYPES = {"NUMBER", "IDENTITY", "TINYINT", "BLOB"};
        for (String type : DISALLOWED_TYPES) {
            stat.execute("DROP TABLE IF EXISTS TEST");
            try {
                stat.execute("CREATE TABLE TEST(COL " + type + ")");
                fail("Expect type " + type + " to not exist in PostgreSQL mode");
            } catch (SQLException e) {
                /* Expected! */
            }
        }

        /* Test MONEY data type */
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(M MONEY)");
        stat.execute("INSERT INTO TEST(M) VALUES (-92233720368547758.08)");
        stat.execute("INSERT INTO TEST(M) VALUES (0.11111)");
        stat.execute("INSERT INTO TEST(M) VALUES (92233720368547758.07)");
        ResultSet rs = stat.executeQuery("SELECT M FROM TEST ORDER BY M");
        assertTrue(rs.next());
        assertEquals(new BigDecimal("-92233720368547758.08"), rs.getBigDecimal(1));
        assertTrue(rs.next());
        assertEquals(new BigDecimal("0.11"), rs.getBigDecimal(1));
        assertTrue(rs.next());
        assertEquals(new BigDecimal("92233720368547758.07"), rs.getBigDecimal(1));
        assertFalse(rs.next());
    }

    private void testMySQL() throws SQLException {
        // need to reconnect to change DATABASE_TO_LOWER
        conn.close();
        deleteDb("compatibility");
        conn = getConnection("compatibility;MODE=MYSQL;DATABASE_TO_LOWER=TRUE");
        Statement stat = conn.createStatement();
        stat.execute("create schema test_schema");
        stat.execute("use test_schema");
        assertResult("test_schema", stat, "select schema()");
        stat.execute("use public");
        assertResult("public", stat, "select schema()");

        stat.execute("SELECT 1");
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE `TEST`(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World')");
        assertResult("0", stat, "SELECT UNIX_TIMESTAMP('1970-01-01 00:00:00Z')");
        assertResult("1196418619", stat, "SELECT UNIX_TIMESTAMP('2007-11-30 10:30:19Z')");
        assertResult("1196418619", stat, "SELECT UNIX_TIMESTAMP(FROM_UNIXTIME(1196418619))");
        assertResult("2007 November", stat, "SELECT FROM_UNIXTIME(1196300000, '%Y %M')");
        assertResult("2003-12-31", stat, "SELECT DATE('2003-12-31 11:02:03')");
        assertResult("2003-12-31", stat, "SELECT DATE('2003-12-31 11:02:03')");
        assertResult(null, stat, "SELECT DATE('100')");
        // check the weird MySQL variant of DELETE
        stat.execute("DELETE TEST FROM TEST WHERE 1=2");

        // Check conversion between VARCHAR and VARBINARY
        String string = "ABCD\u1234";
        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        stat.execute("CREATE TABLE TEST2(C VARCHAR, B VARBINARY)");
        stat.execute("INSERT INTO TEST2(C) VALUES ('" + string + "')");
        assertEquals(1, stat.executeUpdate("UPDATE TEST2 SET B = C"));
        ResultSet rs = stat.executeQuery("SELECT B FROM TEST2");
        assertTrue(rs.next());
        assertEquals(bytes, rs.getBytes(1));
        assertEquals(bytes, rs.getBytes("B"));
        assertEquals(1, stat.executeUpdate("UPDATE TEST2 SET C = B"));
        testMySQLBytesCheck(stat, string, bytes);
        PreparedStatement prep = conn.prepareStatement("UPDATE TEST2 SET C = ?");
        prep.setBytes(1, bytes);
        assertEquals(1, prep.executeUpdate());
        testMySQLBytesCheck(stat, string, bytes);
        stat.execute("DELETE FROM TEST2");
        prep = conn.prepareStatement("INSERT INTO TEST2(C) VALUES (?)");
        prep.setBytes(1, bytes);
        assertEquals(1, prep.executeUpdate());
        testMySQLBytesCheck(stat, string, bytes);
        prep = conn.prepareStatement("SELECT C FROM TEST2 WHERE C = ?");
        prep.setBytes(1, bytes);
        testMySQLBytesCheck(prep.executeQuery(), string, bytes);
        stat.execute("CREATE INDEX TEST2_C ON TEST2(C)");
        prep = conn.prepareStatement("SELECT C FROM TEST2 WHERE C = ?");
        prep.setBytes(1, bytes);
        testMySQLBytesCheck(prep.executeQuery(), string, bytes);
        stat.execute("DROP TABLE TEST2");

        if (config.memory) {
            return;
        }
        // need to reconnect, because meta data tables may be initialized
        conn.close();
        conn = getConnection("compatibility;MODE=MYSQL;DATABASE_TO_LOWER=TRUE");
        stat = conn.createStatement();
        testLog(Math.log(10), stat);

        DatabaseMetaData meta = conn.getMetaData();
        assertTrue(meta.storesLowerCaseIdentifiers());
        assertFalse(meta.storesLowerCaseQuotedIdentifiers());
        assertFalse(meta.storesMixedCaseIdentifiers());
        assertFalse(meta.storesMixedCaseQuotedIdentifiers());
        assertFalse(meta.storesUpperCaseIdentifiers());
        assertFalse(meta.storesUpperCaseQuotedIdentifiers());

        stat = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);
        assertResult("test", stat, "SHOW TABLES");
        rs = stat.executeQuery("SELECT * FROM TEST");
        rs.next();
        rs.updateString(2, "Hallo");
        rs.updateRow();

        // we used to have an NullPointerException in the MetaTable.checkIndex()
        // method
        rs = stat.executeQuery("SELECT * FROM " +
                "INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME > 'aaaa'");
        rs = stat.executeQuery("SELECT * FROM " +
                "INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME < 'aaaa'");

        stat.execute("CREATE TABLE TEST_1" +
                "(ID INT PRIMARY KEY) ENGINE=InnoDb");
        stat.execute("CREATE TABLE TEST_2" +
                "(ID INT PRIMARY KEY) ENGINE=MyISAM");
        stat.execute("CREATE TABLE TEST_3" +
                "(ID INT PRIMARY KEY) ENGINE=InnoDb charset=UTF8");
        stat.execute("CREATE TABLE TEST_4" +
                "(ID INT PRIMARY KEY) charset=UTF8");
        stat.execute("CREATE TABLE TEST_5" +
                "(ID INT PRIMARY KEY) ENGINE=InnoDb auto_increment=3 default charset=UTF8");
        stat.execute("CREATE TABLE TEST_6" +
                "(ID INT PRIMARY KEY) ENGINE=InnoDb auto_increment=3 charset=UTF8");
        stat.execute("CREATE TABLE TEST_7" +
                "(ID INT, KEY TEST_7_IDX(ID) USING BTREE)");
        stat.execute("CREATE TABLE TEST_8" +
                "(ID INT, UNIQUE KEY TEST_8_IDX(ID) USING BTREE)");

        // this maps to SET REFERENTIAL_INTEGRITY TRUE/FALSE
        stat.execute("SET foreign_key_checks = 0");
        stat.execute("SET foreign_key_checks = 1");

        // Check if mysql comments are supported, ensure clean connection
        conn.close();
        conn = getConnection("compatibility;MODE=MYSQL;DATABASE_TO_LOWER=TRUE");
        stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST_NO_COMMENT");
        stat.execute("CREATE table TEST_NO_COMMENT " +
                "(ID bigint not null auto_increment, " +
                "SOME_STR varchar(255), primary key (ID))");
        // now test creating a table with a comment
        stat.execute("DROP TABLE IF EXISTS TEST_COMMENT");
        stat.execute("create table TEST_COMMENT (ID bigint not null auto_increment, " +
                "SOME_STR varchar(255), primary key (ID)) comment='Some comment.'");
        // now test creating a table with a comment and engine
        // and other typical mysql stuff as generated by hibernate
        stat.execute("DROP TABLE IF EXISTS TEST_COMMENT_ENGINE");
        stat.execute("create table TEST_COMMENT_ENGINE " +
                "(ID bigint not null auto_increment, " +
                "ATTACHMENT_ID varchar(255), " +
                "SOME_ITEM_ID bigint not null, primary key (ID), " +
                "unique (ATTACHMENT_ID, SOME_ITEM_ID)) " +
                "comment='Comment Again' ENGINE=InnoDB");

        stat.execute("CREATE TABLE TEST2(ID INT) ROW_FORMAT=DYNAMIC");

        // check the MySQL index dropping syntax
        stat.execute("ALTER TABLE TEST_COMMENT_ENGINE ADD CONSTRAINT CommentUnique UNIQUE (SOME_ITEM_ID)");
        stat.execute("ALTER TABLE TEST_COMMENT_ENGINE DROP INDEX CommentUnique");
        stat.execute("CREATE INDEX IDX_ATTACHMENT_ID ON TEST_COMMENT_ENGINE (ATTACHMENT_ID)");
        stat.execute("DROP INDEX IDX_ATTACHMENT_ID ON TEST_COMMENT_ENGINE");

        stat.execute("DROP ALL OBJECTS");

        conn.close();
        deleteDb("compatibility");
        conn = getConnection("compatibility");
    }

    private void testMySQLBytesCheck(Statement stat, String string, byte[] bytes) throws SQLException {
        testMySQLBytesCheck(stat.executeQuery("SELECT C FROM TEST2"), string, bytes);
    }

    private void testMySQLBytesCheck(ResultSet rs, String string, byte[] bytes) throws SQLException {
        assertTrue(rs.next());
        assertEquals(string, rs.getString(1));
        assertEquals(bytes, rs.getBytes(1));
        assertEquals(bytes, rs.getBytes("C"));
    }

    private void testSybaseAndMSSQLServer() throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("SET MODE MSSQLServer");
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(NAME VARCHAR(50), SURNAME VARCHAR(50))");
        stat.execute("INSERT INTO TEST VALUES('John', 'Doe')");
        stat.execute("INSERT INTO TEST VALUES('Jack', 'Sullivan')");

        assertResult("abcd123", stat, "SELECT 'abc' + 'd123'");

        assertResult("Doe, John", stat,
                "SELECT surname + ', ' + name FROM test " +
                "WHERE SUBSTRING(NAME,1,1)+SUBSTRING(SURNAME,1,1) = 'JD'");

        stat.execute("ALTER TABLE TEST ADD COLUMN full_name VARCHAR(100)");
        stat.execute("UPDATE TEST SET full_name = name + ', ' + surname");
        assertResult("John, Doe", stat, "SELECT full_name FROM TEST where name='John'");

        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?, ?, ? + ', ' + ?)");
        int ca = 1;
        prep.setString(ca++, "Paul");
        prep.setString(ca++, "Frank");
        prep.setString(ca++, "Paul");
        prep.setString(ca++, "Frank");
        prep.executeUpdate();
        prep.close();

        assertResult("Paul, Frank", stat, "SELECT full_name FROM test " +
                "WHERE name = 'Paul'");

        prep = conn.prepareStatement("SELECT ? + ?");
        int cb = 1;
        prep.setString(cb++, "abcd123");
        prep.setString(cb++, "d123");
        prep.executeQuery();
        prep.close();

        prep = conn.prepareStatement("SELECT full_name FROM test " +
                "WHERE (SUBSTRING(name, 1, 1) + SUBSTRING(surname, 2, 3)) = ?");
        prep.setString(1, "Joe");
        ResultSet rs = prep.executeQuery();
        assertTrue("Result cannot be empty", rs.next());
        assertEquals("John, Doe", rs.getString(1));
        rs.close();
        prep.close();

        // CONVERT has it's parameters the other way around from the default
        // mode
        rs = stat.executeQuery("SELECT CONVERT(INT, '10')");
        rs.next();
        assertEquals(10, rs.getInt(1));
        rs.close();
        rs = stat.executeQuery("SELECT X FROM (SELECT CONVERT(INT, '10') AS X)");
        rs.next();
        assertEquals(10, rs.getInt(1));
        rs.close();

        // make sure we're ignoring the index part of the statement
        rs = stat.executeQuery("select * from test (index table1_index)");
        rs.close();

        // UNIQUEIDENTIFIER is MSSQL's equivalent of UUID
        stat.execute("create table test3 (id UNIQUEIDENTIFIER)");

        /* Test MONEY data type */
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(M MONEY)");
        stat.execute("INSERT INTO TEST(M) VALUES (-922337203685477.5808)");
        stat.execute("INSERT INTO TEST(M) VALUES (0.11111)");
        stat.execute("INSERT INTO TEST(M) VALUES (922337203685477.5807)");
        rs = stat.executeQuery("SELECT M FROM TEST ORDER BY M");
        assertTrue(rs.next());
        assertEquals(new BigDecimal("-922337203685477.5808"), rs.getBigDecimal(1));
        assertTrue(rs.next());
        assertEquals(new BigDecimal("0.1111"), rs.getBigDecimal(1));
        assertTrue(rs.next());
        assertEquals(new BigDecimal("922337203685477.5807"), rs.getBigDecimal(1));
        assertFalse(rs.next());

        /* Test SMALLMONEY data type */
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(M SMALLMONEY)");
        stat.execute("INSERT INTO TEST(M) VALUES (-214748.3648)");
        stat.execute("INSERT INTO TEST(M) VALUES (0.11111)");
        stat.execute("INSERT INTO TEST(M) VALUES (214748.3647)");
        rs = stat.executeQuery("SELECT M FROM TEST ORDER BY M");
        assertTrue(rs.next());
        assertEquals(new BigDecimal("-214748.3648"), rs.getBigDecimal(1));
        assertTrue(rs.next());
        assertEquals(new BigDecimal("0.1111"), rs.getBigDecimal(1));
        assertTrue(rs.next());
        assertEquals(new BigDecimal("214748.3647"), rs.getBigDecimal(1));
        assertFalse(rs.next());
    }

    private void testDB2() throws SQLException {
        conn.close();
        conn = getConnection("compatibility;MODE=DB2");
        Statement stat = conn.createStatement();
        testLog(Math.log(10), stat);

        ResultSet res = conn.createStatement().executeQuery(
                "SELECT 1 FROM sysibm.sysdummy1");
        res.next();
        assertEquals("1", res.getString(1));
        conn.close();
        conn = getConnection("compatibility;MODE=MySQL");
        assertThrows(ErrorCode.SCHEMA_NOT_FOUND_1, conn.createStatement()).
                executeQuery("SELECT 1 FROM sysibm.sysdummy1");
        conn.close();
        conn = getConnection("compatibility;MODE=DB2");
        stat = conn.createStatement();
        stat.execute("drop table test if exists");
        stat.execute("create table test(id varchar)");
        stat.execute("insert into test values ('3'),('1'),('2')");
        res = stat.executeQuery("select id from test order by id " +
                "fetch next 2 rows only");
        res.next();
        assertEquals("1", res.getString(1));
        res.next();
        assertEquals("2", res.getString(1));
        assertFalse(res.next());
        conn.close();

        // test isolation-clause
        conn = getConnection("compatibility;MODE=DB2");
        stat = conn.createStatement();
        stat.execute("drop table test if exists");
        stat.execute("create table test(id varchar)");
        res = stat.executeQuery("select * from test with ur");
        stat.executeUpdate("insert into test values (1) with ur");
        res = stat.executeQuery("select * from test where id = 1 with rr");
        res = stat.executeQuery("select * from test order by id " +
                "fetch next 2 rows only with rr");
        res = stat.executeQuery("select * from test order by id " +
                "fetch next 2 rows only with rs");
        res = stat.executeQuery("select * from test order by id " +
                "fetch next 2 rows only with cs");
        res = stat.executeQuery("select * from test order by id " +
                "fetch next 2 rows only with ur");
        // test isolation-clause with lock-request-clause
        res = stat.executeQuery("select * from test order by id " +
                "fetch next 2 rows only with rr use and keep share locks");
        res = stat.executeQuery("select * from test order by id " +
                "fetch next 2 rows only with rs use and keep update locks");
        res = stat.executeQuery("select * from test order by id " +
                "fetch next 2 rows only with rr use and keep exclusive locks");

        // Test DB2 TIMESTAMP format with dash separating date and time
        stat.execute("drop table test if exists");
        stat.execute("create table test(date TIMESTAMP)");
        stat.executeUpdate("insert into test (date) values ('2014-04-05-09.48.28.020005')");
        assertResult("2014-04-05 09:48:28.020005", stat,
                "select date from test"); // <- result is always H2 format timestamp!
        assertResult("2014-04-05 09:48:28.020005", stat,
                "select date from test where date = '2014-04-05-09.48.28.020005'");
        assertResult("2014-04-05 09:48:28.020005", stat,
                "select date from test where date = '2014-04-05 09:48:28.020005'");

        // Test limited support for DB2's special registers

        // Standard SQL functions like LOCALTIMESTAMP, CURRENT_TIMESTAMP and
        // others are used to compare values, their implementation in H2 is
        // compatible with standard, but may be not really compatible with DB2.
        assertResult("TRUE", stat, "SELECT LOCALTIMESTAMP = CURRENT TIMESTAMP");
        assertResult("TRUE", stat, "SELECT CAST(LOCALTIMESTAMP AS VARCHAR) = CAST(CURRENT TIMESTAMP AS VARCHAR)");
        assertResult("TRUE", stat, "SELECT CURRENT_TIMESTAMP = CURRENT TIMESTAMP WITH TIME ZONE");
        assertResult("TRUE", stat,
                "SELECT CAST(CURRENT_TIMESTAMP AS VARCHAR) = CAST(CURRENT TIMESTAMP WITH TIME ZONE AS VARCHAR)");
        assertResult("TRUE", stat, "SELECT CURRENT_TIME = CURRENT TIME");
        assertResult("TRUE", stat, "SELECT CURRENT_DATE = CURRENT DATE");
    }

    private void testDerby() throws SQLException {
        conn.close();
        conn = getConnection("compatibility;MODE=Derby");
        Statement stat = conn.createStatement();
        testLog(Math.log(10), stat);

        ResultSet res = conn.createStatement().executeQuery(
                "SELECT 1 FROM sysibm.sysdummy1 fetch next 1 row only");
        res.next();
        assertEquals("1", res.getString(1));
        conn.close();
        conn = getConnection("compatibility;MODE=PostgreSQL");
        assertThrows(ErrorCode.SCHEMA_NOT_FOUND_1, conn.createStatement()).
                executeQuery("SELECT 1 FROM sysibm.sysdummy1");
        conn.close();
        conn = getConnection("compatibility");
    }

    private void testIgnite() throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("SET MODE Ignite");
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("create table test(id int affinity key)");
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("create table test(id int affinity primary key)");
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("create table test(id int, v1 varchar, v2 long affinity key, primary key(v1, id))");
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("create table test(id int, v1 varchar, v2 long, primary key(v1, id), affinity key (id))");

        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("create table test(id int shard key)");
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("create table test(id int shard primary key)");
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("create table test(id int, v1 varchar, v2 long shard key, primary key(v1, id))");
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("create table test(id int, v1 varchar, v2 long, primary key(v1, id), shard key (id))");
    }

    private void testUnknownSet() throws SQLException {
        Statement stat = conn.createStatement();
        assertThrows(ErrorCode.UNKNOWN_MODE_1, stat).execute("SET MODE Unknown");
    }

    private void testIdentifiers() throws SQLException {
        deleteDb("compatibility");
        testIdentifiers(false, false, false);
        testIdentifiers(false, false, true);
        testIdentifiers(true, false, false);
        testIdentifiers(true, false, true);
        testIdentifiers(false, true, false);
        testIdentifiers(false, true, true);
    }

    private void testIdentifiers(boolean upper, boolean lower, boolean caseInsensitiveIdentifiers) throws SQLException
    {
        try (Connection conn = getConnection("compatibility;DATABASE_TO_UPPER=" + upper + ";DATABASE_TO_LOWER=" + lower
                + ";CASE_INSENSITIVE_IDENTIFIERS=" + caseInsensitiveIdentifiers)) {
            Statement stat = conn.createStatement();
            stat.execute("CREATE TABLE Test(Id INT) AS VALUES 2");
            String schema = "PUBLIC", table = "Test", column = "Id";
            if (upper) {
                table = table.toUpperCase(Locale.ROOT);
                column = column.toUpperCase(Locale.ROOT);
            } else if (lower) {
                schema = schema.toLowerCase(Locale.ROOT);
                table = table.toLowerCase(Locale.ROOT);
                column = column.toLowerCase(Locale.ROOT);
            }
            try (ResultSet rs = stat.executeQuery("SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME"
                    + " FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME ILIKE 'Test'")) {
                assertTrue(rs.next());
                assertEquals(schema, rs.getString(1));
                assertEquals(table, rs.getString(2));
                assertEquals(column, rs.getString(3));
            }
            testIdentifiers(stat, "Test", "Id", true);
            testIdentifiers(stat, "`Test`", "`Id`", true);
            boolean ok = upper || lower || caseInsensitiveIdentifiers;
            testIdentifiers(stat, "TEST", "ID", ok);
            testIdentifiers(stat, "`TEST`", "`ID`", ok);
            testIdentifiers(stat, "test", "id", ok);
            testIdentifiers(stat, "`test`", "`id`", ok);
            testIdentifiers(stat, '"' + table + '"', '"' + column + '"', true);
            testIdentifiers(stat, "\"TeSt\"", "\"iD\"", caseInsensitiveIdentifiers);
            stat.execute("CREATE TABLE T2(\"`\" INT, `\"'\"` INT) AS VALUES (1, 2)");
            try (ResultSet rs = stat.executeQuery("SELECT ````, \"\"\"'\"\"\" FROM T2")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals(2, rs.getInt(2));
            }
        } finally {
            deleteDb("compatibility");
        }
    }

    private void testIdentifiers(Statement stat, String table, String column, boolean ok) throws SQLException {
        String query = "SELECT _ROWID_, " + column + " FROM " + table;
        if (ok) {
            try (ResultSet rs = stat.executeQuery(query)) {
                assertTrue(rs.next());
                assertEquals(1L, rs.getLong(1));
                assertEquals(2, rs.getInt(2));
            }
        } else {
            assertThrows(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, stat).executeQuery(query);
        }
    }

    private void testUnknownURL() throws SQLException {
        try {
            getConnection("compatibility;MODE=Unknown").close();
            deleteDb("compatibility");
        } catch (SQLException ex) {
            assertEquals(ErrorCode.UNKNOWN_MODE_1, ex.getErrorCode());
            return;
        }
        fail();
    }

}
