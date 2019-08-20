/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.File;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;

/**
 * Various test cases.
 */
public class TestCases extends TestBase {

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
        testMinimalCoveringIndexPlan();
        testMinMaxDirectLookupIndex();
        testReferenceLaterTable();
        testAutoCommitInDatabaseURL();
        testReferenceableIndexUsage();
        testClearSyntaxException();
        testEmptyStatements();
        testViewParameters();
        testLargeKeys();
        testExtraSemicolonInDatabaseURL();
        testGroupSubquery();
        testSelfReferentialColumn();
        testCountDistinctNotNull();
        testDependencies();
        testDropTable();
        testConvertType();
        testSortedSelect();
        testMaxMemoryRows();
        testDeleteTop();
        testLikeExpressions();
        testUnicode();
        testOuterJoin();
        testCommentOnColumnWithSchemaEqualDatabase();
        testColumnWithConstraintAndComment();
        testTruncateConstraintsDisabled();
        testPreparedSubquery2();
        testPreparedSubquery();
        testCompareDoubleWithIntColumn();
        testDeleteIndexOutOfBounds();
        testOrderByWithSubselect();
        testInsertDeleteRollback();
        testLargeRollback();
        testConstraintAlterTable();
        testJoinWithView();
        testLobDecrypt();
        testInvalidDatabaseName();
        testReuseSpace();
        testDeleteGroup();
        testDisconnect();
        testExecuteTrace();
        testExplain();
        testExplainAnalyze();
        if (config.memory) {
            return;
        }
        testCheckConstraintWithFunction();
        testDeleteAndDropTableWithLobs(true);
        testDeleteAndDropTableWithLobs(false);
        testEmptyBtreeIndex();
        testReservedKeywordReconnect();
        testSpecialSQL();
        testUpperCaseLowerCaseDatabase();
        testManualCommitSet();
        testSchemaIdentityReconnect();
        testAlterTableReconnect();
        testPersistentSettings();
        testInsertSelectUnion();
        testViewReconnect();
        testDefaultQueryReconnect();
        testBigString();
        testRenameReconnect();
        testAllSizes();
        testCreateDrop();
        testPolePos();
        testQuick();
        testMutableObjects();
        testSelectForUpdate();
        testDoubleRecovery();
        testConstraintReconnect();
        testCollation();
        testBinaryCollation();
        deleteDb("cases");
    }

    private void testReferenceLaterTable() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create table a(id int)");
        stat.execute("create table b(id int)");
        stat.execute("drop table a");
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, stat).
            execute("create table a(id int check id < select max(id) from b)");
        stat.execute("drop table b");
        stat.execute("create table b(id int)");
        stat.execute("create table a(id int check id < select max(id) from b)");
        conn.close();
        conn = getConnection("cases");
        conn.close();
    }

    private void testAutoCommitInDatabaseURL() throws SQLException {
        Connection conn = getConnection("cases;autocommit=false");
        assertFalse(conn.getAutoCommit());
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("call autocommit()");
        rs.next();
        assertFalse(rs.getBoolean(1));
        conn.close();
    }

    private void testReferenceableIndexUsage() throws SQLException {
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("drop table if exists a, b");
        stat.execute("create table a(id int, x int) as select 1, 100");
        stat.execute("create index idx1 on a(id, x)");
        stat.execute("create table b(id int primary key, a_id int) as select 1, 1");
        stat.execute("alter table b add constraint x " +
                "foreign key(a_id) references a(id)");
        stat.execute("update a set x=200");
        stat.execute("drop table if exists a, b cascade");
        conn.close();
    }

    private void testClearSyntaxException() throws SQLException {
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        assertThrows(42000, stat).execute("select t.x, t.x t.y from dual t");
        conn.close();
    }

    private void testEmptyStatements() throws SQLException {
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;");
        stat.execute("");
        stat.execute(";");
        stat.execute(" ;");
        conn.close();
    }

    private void testViewParameters() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute(
                "create view test as select 0 value, 'x' name from dual");
        PreparedStatement prep = conn.prepareStatement(
                "select 1 from test where name=? and value=? and value<=?");
        prep.setString(1, "x");
        prep.setInt(2, 0);
        prep.setInt(3, 1);
        prep.executeQuery();
        conn.close();
    }

    private void testLargeKeys() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("create index on test(name)");
        stat.execute("insert into test values(1, '1' || space(1500))");
        conn.close();
        conn = getConnection("cases");
        stat = conn.createStatement();
        stat.execute("insert into test values(2, '2' || space(1500))");
        conn.close();
        conn = getConnection("cases");
        stat = conn.createStatement();
        stat.executeQuery("select name from test order by name");
        conn.close();
    }

    private void testExtraSemicolonInDatabaseURL() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases;");
        conn.close();
        conn = getConnection("cases;;mode=mysql;");
        conn.close();
    }

    private void testGroupSubquery() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create table test(a int, b int)");
        stat.execute("create index idx on test(a)");
        stat.execute("insert into test values (1, 9), (2, 9), (3, 9)");
        ResultSet rs = stat.executeQuery("select (select count(*)" +
                " from test where a = t.a and b = 0) from test t group by a");
        rs.next();
        assertEquals(0, rs.getInt(1));
        conn.close();
    }

    private void testSelfReferentialColumn() throws SQLException {
        deleteDb("selfreferential");
        Connection conn = getConnection("selfreferential");
        Statement stat = conn.createStatement();
        stat.execute("create table sr(id integer, usecount integer as usecount + 1)");
        assertThrows(ErrorCode.NULL_NOT_ALLOWED, stat).execute("insert into sr(id) values (1)");
        assertThrows(ErrorCode.MUST_GROUP_BY_COLUMN_1, stat).execute("select max(id), usecount from sr");
        conn.close();
    }

    private void testCountDistinctNotNull() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int not null) as " +
                "select 1 from system_range(1, 10)");
        ResultSet rs = stat.executeQuery("select count(distinct id) from test");
        rs.next();
        assertEquals(1, rs.getInt(1));
        conn.close();
    }

    private void testDependencies() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();

        // avoid endless recursion when adding dependencies
        stat.execute("create table test(id int primary key, parent int)");
        stat.execute("alter table test add constraint test check " +
                "(select count(*) from test) < 10");
        stat.execute("create table b()");
        stat.execute("drop table b");
        stat.execute("drop table test");

        // ensure the dependency is detected
        stat.execute("create alias is_positive as " +
                "'boolean isPositive(int x) { return x > 0; }'");
        stat.execute("create table a(a integer, constraint test check is_positive(a))");
        assertThrows(ErrorCode.CANNOT_DROP_2, stat).
                execute("drop alias is_positive");
        stat.execute("drop table a");
        stat.execute("drop alias is_positive");

        // ensure trying to reference the table fails
        // (otherwise re-opening the database is not possible)
        stat.execute("create table test(id int primary key)");
        assertThrows(ErrorCode.COLUMN_IS_REFERENCED_1, stat).
                execute("alter table test alter column id " +
                        "set default ifnull((select max(id) from test for update)+1, 0)");
        stat.execute("drop table test");
        conn.close();
    }

    private void testDropTable() throws SQLException {
        trace("testDropTable");
        final boolean[] booleans = new boolean[] { true, false };
        for (final boolean stdDropTableRestrict : booleans) {
            for (final boolean restrict : booleans) {
                testDropTableNoReference(stdDropTableRestrict, restrict);
                testDropTableViewReference(stdDropTableRestrict, restrict);
                testDropTableForeignKeyReference(stdDropTableRestrict, restrict);
            }
        }
    }

    private Statement createTable(final boolean stdDropTableRestrict) throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases;STANDARD_DROP_TABLE_RESTRICT=" + stdDropTableRestrict);
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int)");
        return stat;
    }

    private void dropTable(final boolean restrict, Statement stat, final boolean expectedDropSuccess)
            throws SQLException {
        assertThrows(expectedDropSuccess ? 0 : ErrorCode.CANNOT_DROP_2, stat)
                .execute("drop table test " + (restrict ? "restrict" : "cascade"));
        assertThrows(expectedDropSuccess ? ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1 : 0, stat)
                .execute("select * from test");
    }

    private void testDropTableNoReference(final boolean stdDropTableRestrict, final boolean restrict)
            throws SQLException {
        Statement stat = createTable(stdDropTableRestrict);
        // always succeed as there's no reference to the table
        dropTable(restrict, stat, true);
        stat.getConnection().close();
    }

    private void testDropTableViewReference(final boolean stdDropTableRestrict, final boolean restrict)
            throws SQLException {
        Statement stat = createTable(stdDropTableRestrict);
        stat.execute("create view abc as select * from test");
        // drop allowed only if cascade
        final boolean expectedDropSuccess = !restrict;
        dropTable(restrict, stat, expectedDropSuccess);
        // missing view if the drop succeeded
        assertThrows(expectedDropSuccess ? ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1 : 0, stat).execute("select * from abc");
        stat.getConnection().close();
    }

    private void testDropTableForeignKeyReference(final boolean stdDropTableRestrict, final boolean restrict)
            throws SQLException {
        Statement stat = createTable(stdDropTableRestrict);
        stat.execute("create table ref(id int, id_test int, foreign key (id_test) references test (id)) ");
        // test table is empty, so the foreign key forces ref table to be also
        // empty
        assertThrows(ErrorCode.REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1, stat)
                .execute("insert into ref values(1,2)");
        // drop allowed if cascade or old style
        final boolean expectedDropSuccess = !stdDropTableRestrict || !restrict;
        dropTable(restrict, stat, expectedDropSuccess);
        // insertion succeeds if the foreign key was dropped
        assertThrows(expectedDropSuccess ? 0 : ErrorCode.REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1, stat)
                .execute("insert into ref values(1,2)");
        stat.getConnection().close();
    }

    private void testConvertType() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create table test as select cast(0 as dec(10, 2)) x");
        ResultSetMetaData meta = stat.executeQuery("select * from test").getMetaData();
        assertEquals(2, meta.getPrecision(1));
        assertEquals(2, meta.getScale(1));
        stat.execute("alter table test add column y int");
        stat.execute("drop table test");
        conn.close();
    }

    private void testSortedSelect() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create memory temporary table test(id int) not persistent");
        stat.execute("insert into test(id) direct sorted select 1");
        stat.execute("drop table test");
        conn.close();
    }

    private void testMaxMemoryRows() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection(
                "cases;MAX_MEMORY_ROWS=1");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key)");
        stat.execute("insert into test values(1), (2)");
        stat.execute("select * from dual where x not in " +
                "(select id from test order by id)");
        stat.execute("select * from dual where x not in " +
                "(select id from test union select id from test)");
        stat.execute("(select id from test order by id) " +
                "intersect (select id from test order by id)");
        conn.close();
    }

    private void testUnicode() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id identity, name text)");
        String[] data = { "\uff1e", "\ud848\udf1e" };
        PreparedStatement prep = conn.prepareStatement(
                "insert into test(name) values(?)");
        for (int i = 0; i < data.length; i++) {
            prep.setString(1, data[i]);
            prep.execute();
        }
        prep = conn.prepareStatement("select * from test order by id");
        ResultSet rs = prep.executeQuery();
        for (int i = 0; i < data.length; i++) {
            assertTrue(rs.next());
            assertEquals(data[i], rs.getString(2));
        }
        stat.execute("drop table test");
        conn.close();
    }

    private void testCheckConstraintWithFunction() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create alias is_email as " +
                "'boolean isEmail(String x) { return x != null && x.indexOf(''@'') > 0; }'");
        stat.execute("create domain email as varchar check is_email(value)");
        stat.execute("create table test(e email)");
        conn.close();
        conn = getConnection("cases");
        conn.close();
    }

    private void testOuterJoin() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create table parent(p int primary key) as select 1");
        stat.execute("create table child(c int primary key, pc int) as select 2, 1");
        ResultSet rs = stat.executeQuery("select * from parent " +
                "left outer join child on p = pc where c is null");
        assertFalse(rs.next());
        stat.execute("drop all objects");
        conn.close();
    }

    private void testCommentOnColumnWithSchemaEqualDatabase() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create schema cases");
        stat.execute("create table cases.cases(cases int)");
        stat.execute("comment on column " +
                "cases.cases.cases is 'schema.table.column'");
        stat.execute("comment on column " +
                "cases.cases.cases.cases is 'db.schema.table.column'");
        conn.close();
    }

    private void testColumnWithConstraintAndComment() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int check id < 500)");
        stat.execute("comment on column test.id is 'comment'");
        conn.close();
        conn = getConnection("cases");
        conn.close();
    }

    private void testTruncateConstraintsDisabled() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create table parent(id identity) as select 0");
        stat.execute("create table child(id identity, " +
                "parent int references parent(id)) as select 0, 0");
        assertThrows(ErrorCode.CANNOT_TRUNCATE_1, stat).
                execute("truncate table parent");
        assertThrows(ErrorCode.REFERENTIAL_INTEGRITY_VIOLATED_CHILD_EXISTS_1, stat).
                execute("delete from parent");
        stat.execute("alter table parent set referential_integrity false");
        stat.execute("delete from parent");
        stat.execute("truncate table parent");
        stat.execute("alter table parent set referential_integrity true");
        assertThrows(ErrorCode.CANNOT_TRUNCATE_1, stat).
                execute("truncate table parent");
        stat.execute("set referential_integrity false");
        stat.execute("truncate table parent");
        conn.close();
    }

    private void testPreparedSubquery2() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar(255))");
        stat.execute("insert into test values(1, 'Hello')");
        stat.execute("insert into test values(2, 'World')");

        PreparedStatement ps = conn.prepareStatement(
                "select name from test where id in " +
                "(select id from test where name = ?)");
        ps.setString(1, "Hello");
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            if (!rs.getString("name").equals("Hello")) {
                fail("'" + rs.getString("name") + "' must be 'Hello'");
            }
        } else {
            fail("Must have a result!");
        }
        rs.close();

        ps.setString(1, "World");
        rs = ps.executeQuery();
        if (rs.next()) {
            if (!rs.getString("name").equals("World")) {
                fail("'" + rs.getString("name") + "' must be 'World'");
            }
        } else {
            fail("Must have a result!");
        }
        rs.close();
        conn.close();
    }

    private void testPreparedSubquery() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int)");
        stat.execute("insert into test values(1)");
        String sql = "select ?, ?, (select count(*) from test inner join " +
            "(select id from test where 0=?) as t2 on t2.id=test.id) from test";
        ResultSet rs;
        rs = stat.executeQuery(sql.replace('?', '0'));
        rs.next();
        assertEquals(1, rs.getInt(3));
        PreparedStatement prep = conn.prepareStatement(sql);
        prep.setInt(1, 0);
        prep.setInt(2, 0);
        prep.setInt(3, 0);
        rs = prep.executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(3));
        conn.close();
    }

    private void testCompareDoubleWithIntColumn() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        testCompareDoubleWithIntColumn(stat, false, 0.1, false);
        testCompareDoubleWithIntColumn(stat, false, 0.1, true);
        testCompareDoubleWithIntColumn(stat, false, 0.9, false);
        testCompareDoubleWithIntColumn(stat, false, 0.9, true);
        testCompareDoubleWithIntColumn(stat, true, 0.1, false);
        testCompareDoubleWithIntColumn(stat, true, 0.1, true);
        testCompareDoubleWithIntColumn(stat, true, 0.9, false);
        testCompareDoubleWithIntColumn(stat, true, 0.9, true);
        conn.close();
    }

    private void testCompareDoubleWithIntColumn(Statement stat, boolean pk,
            double x, boolean prepared) throws SQLException {
        if (pk) {
            stat.execute("create table test(id int primary key)");
        } else {
            stat.execute("create table test(id int)");
        }
        stat.execute("insert into test values(1)");
        ResultSet rs;
        if (prepared) {
            PreparedStatement prep = stat.getConnection().prepareStatement(
                    "select * from test where id > ?");
            prep.setDouble(1, x);
            rs = prep.executeQuery();
        } else {
            rs = stat.executeQuery("select * from test where id > " + x);
        }
        assertTrue(rs.next());
        stat.execute("drop table test");
    }

    private void testDeleteIndexOutOfBounds() throws SQLException {
        if (config.memory || !config.big) {
            return;
        }
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE IF NOT EXISTS test " +
                "(rowid INTEGER PRIMARY KEY AUTO_INCREMENT, txt VARCHAR(64000));");
        PreparedStatement prep = conn.prepareStatement(
                "insert into test (txt) values(space(?))");
        for (int i = 0; i < 3000; i++) {
            prep.setInt(1, i * 3);
            prep.execute();
        }
        stat.execute("DELETE FROM test;");
        conn.close();
    }

    private void testInsertDeleteRollback() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("set cache_size 1");
        stat.execute("SET MAX_MEMORY_ROWS " + Integer.MAX_VALUE);
        stat.execute("SET MAX_MEMORY_UNDO " + Integer.MAX_VALUE);
        stat.execute("SET MAX_OPERATION_MEMORY " + Integer.MAX_VALUE);
        stat.execute("create table test(id identity)");
        conn.setAutoCommit(false);
        stat.execute("insert into test select x from system_range(1, 11)");
        stat.execute("delete from test");
        conn.rollback();
        conn.close();
    }

    private void testLargeRollback() throws SQLException {
        Connection conn;
        Statement stat;

        deleteDb("cases");
        conn = getConnection("cases");
        stat = conn.createStatement();
        stat.execute("set max_operation_memory 1");
        stat.execute("create table test(id int)");
        stat.execute("insert into test values(1), (2)");
        stat.execute("create index idx on test(id)");
        conn.setAutoCommit(false);
        stat.execute("update test set id = id where id=2");
        stat.execute("update test set id = id");
        conn.rollback();
        conn.close();

        deleteDb("cases");
        conn = getConnection("cases");
        conn.createStatement().execute("set MAX_MEMORY_UNDO 1");
        conn.createStatement().execute("create table test(id number primary key)");
        conn.createStatement().execute(
                "insert into test(id) select x from system_range(1, 2)");
        Connection conn2 = getConnection("cases");
        conn2.setAutoCommit(false);
        assertEquals(2, conn2.createStatement().executeUpdate(
                "delete from test"));
        conn2.close();
        conn.close();

        deleteDb("cases");
        conn = getConnection("cases");
        conn.createStatement().execute("set MAX_MEMORY_UNDO 8");
        conn.createStatement().execute("create table test(id number primary key)");
        conn.setAutoCommit(false);
        conn.createStatement().execute(
                "insert into test select x from system_range(1, 10)");
        conn.createStatement().execute("delete from test");
        conn.rollback();
        conn.close();
    }

    private void testConstraintAlterTable() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create table parent (pid int)");
        stat.execute("create table child (cid int primary key, pid int)");
        stat.execute("alter table child add foreign key (pid) references parent(pid)");
        stat.execute("alter table child add column c2 int");
        stat.execute("alter table parent add column p2 varchar");
        conn.close();
    }

    private void testEmptyBtreeIndex() throws SQLException {
        deleteDb("cases");
        Connection conn;
        conn = getConnection("cases");
        conn.createStatement().execute("CREATE TABLE test(id int PRIMARY KEY);");
        conn.createStatement().execute(
                "INSERT INTO test SELECT X FROM SYSTEM_RANGE(1, 77)");
        conn.createStatement().execute("DELETE from test");
        conn.close();
        conn = getConnection("cases");
        conn.createStatement().execute("INSERT INTO test (id) VALUES (1)");
        conn.close();
        conn = getConnection("cases");
        conn.createStatement().execute("DELETE from test");
        conn.createStatement().execute("drop table test");
        conn.close();
    }

    private void testJoinWithView() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        conn.createStatement().execute(
                "create table t(i identity, n varchar) as select 1, 'x'");
        PreparedStatement prep = conn.prepareStatement(
                "select 1 from dual " +
                "inner join(select n from t where i=?) a on a.n='x' " +
                "inner join(select n from t where i=?) b on b.n='x'");
        prep.setInt(1, 1);
        prep.setInt(2, 1);
        prep.execute();
        conn.close();
    }

    private void testLobDecrypt() throws SQLException {
        Connection conn = getConnection("cases");
        String key = "key";
        String value = "Hello World";
        PreparedStatement prep = conn.prepareStatement(
                "CALL ENCRYPT('AES', RAWTOHEX(?), STRINGTOUTF8(?))");
        prep.setCharacterStream(1, new StringReader(key), -1);
        prep.setCharacterStream(2, new StringReader(value), -1);
        ResultSet rs = prep.executeQuery();
        rs.next();
        String encrypted = rs.getString(1);
        PreparedStatement prep2 = conn.prepareStatement(
                "CALL TRIM(CHAR(0) FROM " +
                "UTF8TOSTRING(DECRYPT('AES', RAWTOHEX(?), ?)))");
        prep2.setCharacterStream(1, new StringReader(key), -1);
        prep2.setCharacterStream(2, new StringReader(encrypted), -1);
        ResultSet rs2 = prep2.executeQuery();
        rs2.first();
        String decrypted = rs2.getString(1);
        prep2.close();
        assertEquals(value, decrypted);
        conn.close();
    }

    private void testReservedKeywordReconnect() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create table \"UNIQUE\"(\"UNIQUE\" int)");
        conn.close();
        conn = getConnection("cases");
        stat = conn.createStatement();
        stat.execute("select \"UNIQUE\" from \"UNIQUE\"");
        stat.execute("drop table \"UNIQUE\"");
        conn.close();
    }

    private void testInvalidDatabaseName() throws SQLException {
        if (config.memory) {
            return;
        }
        assertThrows(ErrorCode.INVALID_DATABASE_NAME_1, this).
            getConnection("cases/");
    }

    private void testReuseSpace() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        int tableCount = getSize(2, 5);
        for (int i = 0; i < tableCount; i++) {
            stat.execute("create table t" + i + "(data varchar)");
        }
        Random random = new Random(1);
        int len = getSize(50, 500);
        for (int i = 0; i < len; i++) {
            String table = "t" + random.nextInt(tableCount);
            String sql;
            if (random.nextBoolean()) {
                sql = "insert into " + table + " values(space(100000))";
            } else {
                sql = "delete from " + table;
            }
            stat.execute(sql);
            stat.execute("script to '" + getBaseDir() + "/test.sql'");
        }
        conn.close();
        FileUtils.delete(getBaseDir() + "/test.sql");
    }

    private void testDeleteGroup() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("set max_memory_rows 2");
        stat.execute("create table test(id int primary key, x int)");
        stat.execute("insert into test values(0, 0), (1, 1), (2, 2)");
        stat.execute("delete from test where id not in " +
                "(select min(x) from test group by id)");
        conn.close();
    }

    private void testSpecialSQL() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        assertThrows(ErrorCode.SYNTAX_ERROR_2, stat).
                execute("create table address" +
                        "(id identity, name varchar check? instr(value, '@') > 1)");
        stat.execute("SET AUTOCOMMIT OFF; \n//" +
                "create sequence if not exists object_id;\n");
        stat.execute("SET AUTOCOMMIT OFF;\n//" +
                "create sequence if not exists object_id;\n");
        stat.execute("SET AUTOCOMMIT OFF; //" +
                "create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF;//" +
                "create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF \n//" +
                "create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF\n//" +
                "create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF //" +
                "create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF//" +
                "create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF; \n///" +
                "create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF;\n///" +
                "create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF; ///" +
                "create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF;///" +
                "create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF \n///" +
                "create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF\n///" +
                "create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF ///" +
                "create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF///" +
                "create sequence if not exists object_id;");
        conn.close();
    }

    private void testUpperCaseLowerCaseDatabase() throws SQLException {
        if (File.separatorChar != '\\' || config.googleAppEngine) {
            return;
        }
        deleteDb("cases");
        deleteDb("CaSeS");
        Connection conn, conn2;
        ResultSet rs;
        conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("CHECKPOINT");
        stat.execute("CREATE TABLE TEST(ID INT)");
        stat.execute("INSERT INTO TEST VALUES(1)");
        stat.execute("CHECKPOINT");

        conn2 = getConnection("CaSeS");
        rs = conn.createStatement().executeQuery("SELECT * FROM TEST");
        assertTrue(rs.next());
        conn2.close();

        conn.close();

        conn = getConnection("cases");
        rs = conn.createStatement().executeQuery("SELECT * FROM TEST");
        assertTrue(rs.next());
        conn.close();

        conn = getConnection("CaSeS");
        rs = conn.createStatement().executeQuery("SELECT * FROM TEST");
        assertTrue(rs.next());
        conn.close();

        deleteDb("cases");
        deleteDb("CaSeS");

    }

    private void testManualCommitSet() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Connection conn2 = getConnection("cases");
        conn.setAutoCommit(false);
        conn2.setAutoCommit(false);
        conn.createStatement().execute("SET MODE REGULAR");
        conn2.createStatement().execute("SET MODE REGULAR");
        conn.close();
        conn2.close();
    }

    private void testSchemaIdentityReconnect() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create schema s authorization sa");
        stat.execute("create table s.test(id identity)");
        conn.close();
        conn = getConnection("cases");
        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT * FROM S.TEST");
        while (rs.next()) {
            // ignore
        }
        conn.close();
    }

    private void testDisconnect() throws Exception {
        if (config.networked || config.codeCoverage) {
            return;
        }
        deleteDb("cases");
        Connection conn = getConnection("cases");
        final Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID IDENTITY)");
        for (int i = 0; i < 1000; i++) {
            stat.execute("INSERT INTO TEST() VALUES()");
        }
        final SQLException[] stopped = { null };
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    long time = System.nanoTime();
                    ResultSet rs = stat.executeQuery("SELECT MAX(T.ID) " +
                            "FROM TEST T, TEST, TEST, TEST, TEST, " +
                            "TEST, TEST, TEST, TEST, TEST, TEST");
                    rs.next();
                    time = System.nanoTime() - time;
                    TestBase.logError("query was too quick; result: " +
                            rs.getInt(1) + " time:" + TimeUnit.NANOSECONDS.toMillis(time), null);
                } catch (SQLException e) {
                    stopped[0] = e;
                    // ok
                }
            }
        });
        t.start();
        Thread.sleep(300);
        long time = System.nanoTime();
        conn.close();
        t.join(5000);
        if (stopped[0] == null) {
            fail("query still running");
        } else {
            assertKnownException(stopped[0]);
        }
        time = System.nanoTime() - time;
        if (time > TimeUnit.SECONDS.toNanos(5)) {
            if (!config.reopen) {
                fail("closing took " + time);
            }
        }
        deleteDb("cases");
    }

    private void testExecuteTrace() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(
                "SELECT ? FROM DUAL {1: 'Hello'}");
        rs.next();
        assertEquals("Hello", rs.getString(1));
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT ? FROM DUAL UNION ALL " +
                "SELECT ? FROM DUAL {1: 'Hello', 2:'World' }");
        rs.next();
        assertEquals("Hello", rs.getString(1));
        rs.next();
        assertEquals("World", rs.getString(1));
        assertFalse(rs.next());

        conn.close();
    }

    private void checkExplain(Statement stat, String sql, String expected) throws SQLException {
        ResultSet rs = stat.executeQuery(sql);

        assertTrue(rs.next());

        assertEquals(expected, rs.getString(1));
    }

    private void testExplain() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();

        stat.execute("CREATE TABLE ORGANIZATION" +
                "(id int primary key, name varchar(100))");
        stat.execute("CREATE TABLE PERSON" +
                "(id int primary key, orgId int, name varchar(100), salary int)");

        checkExplain(stat, "/* bla-bla */ EXPLAIN SELECT ID FROM ORGANIZATION WHERE id = ?",
            "SELECT\n" +
                "    ID\n" +
                "FROM PUBLIC.ORGANIZATION\n" +
                "    /* PUBLIC.PRIMARY_KEY_D: ID = ?1 */\n" +
                "WHERE ID = ?1");

        checkExplain(stat, "EXPLAIN SELECT ID FROM ORGANIZATION WHERE id = 1",
            "SELECT\n" +
                "    ID\n" +
                "FROM PUBLIC.ORGANIZATION\n" +
                "    /* PUBLIC.PRIMARY_KEY_D: ID = 1 */\n" +
                "WHERE ID = 1");

        checkExplain(stat, "EXPLAIN SELECT * FROM PERSON WHERE id = ?",
            "SELECT\n" +
                "    PERSON.ID,\n" +
                "    PERSON.ORGID,\n" +
                "    PERSON.NAME,\n" +
                "    PERSON.SALARY\n" +
                "FROM PUBLIC.PERSON\n" +
                "    /* PUBLIC.PRIMARY_KEY_8: ID = ?1 */\n" +
                "WHERE ID = ?1");

        checkExplain(stat, "EXPLAIN SELECT * FROM PERSON WHERE id = 50",
            "SELECT\n" +
                "    PERSON.ID,\n" +
                "    PERSON.ORGID,\n" +
                "    PERSON.NAME,\n" +
                "    PERSON.SALARY\n" +
                "FROM PUBLIC.PERSON\n" +
                "    /* PUBLIC.PRIMARY_KEY_8: ID = 50 */\n" +
                "WHERE ID = 50");

        checkExplain(stat, "EXPLAIN SELECT * FROM PERSON WHERE salary > ? and salary < ?",
            "SELECT\n" +
                "    PERSON.ID,\n" +
                "    PERSON.ORGID,\n" +
                "    PERSON.NAME,\n" +
                "    PERSON.SALARY\n" +
                "FROM PUBLIC.PERSON\n" +
                "    /* PUBLIC.PERSON.tableScan */\n" +
                "WHERE (SALARY > ?1)\n" +
                "    AND (SALARY < ?2)");

        checkExplain(stat, "EXPLAIN SELECT * FROM PERSON WHERE salary > 1000 and salary < 2000",
            "SELECT\n" +
                "    PERSON.ID,\n" +
                "    PERSON.ORGID,\n" +
                "    PERSON.NAME,\n" +
                "    PERSON.SALARY\n" +
                "FROM PUBLIC.PERSON\n" +
                "    /* PUBLIC.PERSON.tableScan */\n" +
                "WHERE (SALARY > 1000)\n" +
                "    AND (SALARY < 2000)");

        checkExplain(stat, "EXPLAIN SELECT * FROM PERSON WHERE name = lower(?)",
            "SELECT\n" +
                "    PERSON.ID,\n" +
                "    PERSON.ORGID,\n" +
                "    PERSON.NAME,\n" +
                "    PERSON.SALARY\n" +
                "FROM PUBLIC.PERSON\n" +
                "    /* PUBLIC.PERSON.tableScan */\n" +
                "WHERE NAME = LOWER(?1)");

        checkExplain(stat, "EXPLAIN SELECT * FROM PERSON WHERE name = lower('Smith')",
            "SELECT\n" +
                "    PERSON.ID,\n" +
                "    PERSON.ORGID,\n" +
                "    PERSON.NAME,\n" +
                "    PERSON.SALARY\n" +
                "FROM PUBLIC.PERSON\n" +
                "    /* PUBLIC.PERSON.tableScan */\n" +
                "WHERE NAME = 'smith'");

        checkExplain(stat, "EXPLAIN SELECT * FROM PERSON p " +
            "INNER JOIN ORGANIZATION o ON p.id = o.id WHERE o.id = ? AND p.salary > ?",
            "SELECT\n" +
                "    P.ID,\n" +
                "    P.ORGID,\n" +
                "    P.NAME,\n" +
                "    P.SALARY,\n" +
                "    O.ID,\n" +
                "    O.NAME\n" +
                "FROM PUBLIC.ORGANIZATION O\n" +
                "    /* PUBLIC.PRIMARY_KEY_D: ID = ?1 */\n" +
                "    /* WHERE O.ID = ?1\n" +
                "    */\n" +
                "INNER JOIN PUBLIC.PERSON P\n" +
                "    /* PUBLIC.PRIMARY_KEY_8: ID = O.ID */\n" +
                "    ON 1=1\n" +
                "WHERE (P.ID = O.ID)\n" +
                "    AND ((O.ID = ?1)\n" +
                "    AND (P.SALARY > ?2))");

        checkExplain(stat, "EXPLAIN SELECT * FROM PERSON p " +
            "INNER JOIN ORGANIZATION o ON p.id = o.id WHERE o.id = 10 AND p.salary > 1000",
            "SELECT\n" +
                "    P.ID,\n" +
                "    P.ORGID,\n" +
                "    P.NAME,\n" +
                "    P.SALARY,\n" +
                "    O.ID,\n" +
                "    O.NAME\n" +
                "FROM PUBLIC.ORGANIZATION O\n" +
                "    /* PUBLIC.PRIMARY_KEY_D: ID = 10 */\n" +
                "    /* WHERE O.ID = 10\n" +
                "    */\n" +
                "INNER JOIN PUBLIC.PERSON P\n" +
                "    /* PUBLIC.PRIMARY_KEY_8: ID = O.ID */\n" +
                "    ON 1=1\n" +
                "WHERE (P.ID = O.ID)\n" +
                "    AND ((O.ID = 10)\n" +
                "    AND (P.SALARY > 1000))");

        PreparedStatement pStat = conn.prepareStatement(
                "/* bla-bla */ EXPLAIN SELECT ID FROM ORGANIZATION WHERE id = ?");

        ResultSet rs = pStat.executeQuery();

        assertTrue(rs.next());

        assertEquals("SELECT\n" +
                "    ID\n" +
                "FROM PUBLIC.ORGANIZATION\n" +
                "    /* PUBLIC.PRIMARY_KEY_D: ID = ?1 */\n" +
                "WHERE ID = ?1",
            rs.getString(1));

        conn.close();
    }

    private void testExplainAnalyze() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();

        stat.execute("CREATE TABLE ORGANIZATION" +
                "(id int primary key, name varchar(100))");
        stat.execute("CREATE TABLE PERSON" +
                "(id int primary key, orgId int, name varchar(100), salary int)");

        stat.execute("INSERT INTO ORGANIZATION VALUES(1, 'org1')");
        stat.execute("INSERT INTO ORGANIZATION VALUES(2, 'org2')");

        stat.execute("INSERT INTO PERSON VALUES(1, 1, 'person1', 1000)");
        stat.execute("INSERT INTO PERSON VALUES(2, 1, 'person2', 2000)");
        stat.execute("INSERT INTO PERSON VALUES(3, 2, 'person3', 3000)");
        stat.execute("INSERT INTO PERSON VALUES(4, 2, 'person4', 4000)");

        assertThrows(ErrorCode.PARAMETER_NOT_SET_1, stat,
                "/* bla-bla */ EXPLAIN ANALYZE SELECT ID FROM ORGANIZATION WHERE id = ?");

        PreparedStatement pStat = conn.prepareStatement(
                "/* bla-bla */ EXPLAIN ANALYZE SELECT ID FROM ORGANIZATION WHERE id = ?");

        assertThrows(ErrorCode.PARAMETER_NOT_SET_1, pStat).executeQuery();

        pStat.setInt(1, 1);

        ResultSet rs = pStat.executeQuery();

        assertTrue(rs.next());

        assertEquals("SELECT\n" +
                "    ID\n" +
                "FROM PUBLIC.ORGANIZATION\n" +
                "    /* PUBLIC.PRIMARY_KEY_D: ID = ?1 */\n" +
                "    /* scanCount: 2 */\n" +
                "WHERE ID = ?1",
            rs.getString(1));

        pStat = conn.prepareStatement("EXPLAIN ANALYZE SELECT * FROM PERSON p " +
            "INNER JOIN ORGANIZATION o ON o.id = p.id WHERE o.id = ?");

        assertThrows(ErrorCode.PARAMETER_NOT_SET_1, pStat).executeQuery();

        pStat.setInt(1, 1);

        rs = pStat.executeQuery();

        assertTrue(rs.next());

        assertEquals("SELECT\n" +
                "    P.ID,\n" +
                "    P.ORGID,\n" +
                "    P.NAME,\n" +
                "    P.SALARY,\n" +
                "    O.ID,\n" +
                "    O.NAME\n" +
                "FROM PUBLIC.ORGANIZATION O\n" +
                "    /* PUBLIC.PRIMARY_KEY_D: ID = ?1 */\n" +
                "    /* WHERE O.ID = ?1\n" +
                "    */\n" +
                "    /* scanCount: 2 */\n" +
                "INNER JOIN PUBLIC.PERSON P\n" +
                "    /* PUBLIC.PRIMARY_KEY_8: ID = O.ID\n" +
                "        AND ID = ?1\n" +
                "     */\n" +
                "    ON 1=1\n" +
                "    /* scanCount: 2 */\n" +
                "WHERE ((O.ID = ?1)\n" +
                "    AND (O.ID = P.ID))\n" +
                "    AND (P.ID = ?1)",
            rs.getString(1));

        conn.close();
    }

    private void testAlterTableReconnect() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id identity);");
        stat.execute("insert into test values(1);");
        assertThrows(ErrorCode.NULL_NOT_ALLOWED, stat).
                execute("alter table test add column name varchar not null;");
        conn.close();
        conn = getConnection("cases");
        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT * FROM TEST");
        rs.next();
        assertEquals("1", rs.getString(1));
        assertFalse(rs.next());
        stat = conn.createStatement();
        stat.execute("drop table test");
        stat.execute("create table test(id identity)");
        stat.execute("insert into test values(1)");
        stat.execute("alter table test alter column id set default 'x'");
        conn.close();
        conn = getConnection("cases");
        stat = conn.createStatement();
        rs = conn.createStatement().executeQuery("SELECT * FROM TEST");
        rs.next();
        assertEquals("1", rs.getString(1));
        assertFalse(rs.next());
        stat.execute("drop table test");
        stat.execute("create table test(id identity)");
        stat.execute("insert into test values(1)");
        assertThrows(ErrorCode.INVALID_DATETIME_CONSTANT_2, stat).
                execute("alter table test alter column id date");
        conn.close();
        conn = getConnection("cases");
        rs = conn.createStatement().executeQuery("SELECT * FROM TEST");
        rs.next();
        assertEquals("1", rs.getString(1));
        assertFalse(rs.next());
        conn.close();
    }

    private void testCollation() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("SET COLLATION ENGLISH STRENGTH PRIMARY");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, " +
                "NAME VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello'), " +
                "(2, 'World'), (3, 'WORLD'), (4, 'HELLO')");
        stat.execute("create index idxname on test(name)");
        ResultSet rs;
        rs = stat.executeQuery("select name from test order by name");
        rs.next();
        assertEquals("Hello", rs.getString(1));
        rs.next();
        assertEquals("HELLO", rs.getString(1));
        rs.next();
        assertEquals("World", rs.getString(1));
        rs.next();
        assertEquals("WORLD", rs.getString(1));
        rs = stat.executeQuery("select name from test where name like 'He%'");
        rs.next();
        assertEquals("Hello", rs.getString(1));
        rs.next();
        assertEquals("HELLO", rs.getString(1));
        conn.close();
    }

    private void testBinaryCollation() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        ResultSet rs;

        // test the default (SIGNED)
        if (Constants.VERSION_MINOR < 4) {
            stat.execute("create table bin( x binary(1) );");
            stat.execute("insert into bin(x) values (x'09'),(x'0a'),(x'99'),(x'aa');");
            rs = stat.executeQuery("select * from bin order by x;");
            rs.next();
            assertEquals("99", rs.getString(1));
            rs.next();
            assertEquals("aa", rs.getString(1));
            rs.next();
            assertEquals("09", rs.getString(1));
            rs.next();
            assertEquals("0a", rs.getString(1));
            stat.execute("drop table bin");
        }

        // test UNSIGNED mode
        stat.execute("SET BINARY_COLLATION UNSIGNED");
        stat.execute("create table bin( x binary(1) );");
        stat.execute("insert into bin(x) values (x'09'),(x'0a'),(x'99'),(x'aa');");
        rs = stat.executeQuery("select * from bin order by x;");
        rs.next();
        assertEquals("09", rs.getString(1));
        rs.next();
        assertEquals("0a", rs.getString(1));
        rs.next();
        assertEquals("99", rs.getString(1));
        rs.next();
        assertEquals("aa", rs.getString(1));

        conn.close();
    }

    private void testPersistentSettings() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("SET COLLATION de_DE");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, " +
                "NAME VARCHAR)");
        stat.execute("CREATE INDEX IDXNAME ON TEST(NAME)");
        // \u00f6 = oe
        stat.execute("INSERT INTO TEST VALUES(1, 'B\u00f6hlen'), " +
                "(2, 'Bach'), (3, 'Bucher')");
        conn.close();
        conn = getConnection("cases");
        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT NAME FROM TEST ORDER BY NAME");
        rs.next();
        assertEquals("Bach", rs.getString(1));
        rs.next();
        assertEquals("B\u00f6hlen", rs.getString(1));
        rs.next();
        assertEquals("Bucher", rs.getString(1));
        conn.close();
    }

    private void testInsertSelectUnion() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ORDER_ID INT PRIMARY KEY, " +
                "ORDER_DATE DATETIME, " +
                "USER_ID INT, DESCRIPTION VARCHAR, STATE VARCHAR, " +
                "TRACKING_ID VARCHAR)");
        Timestamp orderDate = Timestamp.valueOf("2005-05-21 17:46:00");
        String sql = "insert into TEST (ORDER_ID,ORDER_DATE," +
                "USER_ID,DESCRIPTION,STATE,TRACKING_ID) " +
                "select cast(? as int),cast(? as date),cast(? as int),cast(? as varchar)," +
                "cast(? as varchar),cast(? as varchar) union all select ?,?,?,?,?,?";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setInt(1, 5555);
        ps.setTimestamp(2, orderDate);
        ps.setInt(3, 2222);
        ps.setString(4, "test desc");
        ps.setString(5, "test_state");
        ps.setString(6, "testid");
        ps.setInt(7, 5556);
        ps.setTimestamp(8, orderDate);
        ps.setInt(9, 2222);
        ps.setString(10, "test desc");
        ps.setString(11, "test_state");
        ps.setString(12, "testid");
        assertEquals(2, ps.executeUpdate());
        ps.close();
        conn.close();
    }

    private void testViewReconnect() throws SQLException {
        trace("testViewReconnect");
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int)");
        stat.execute("create view abc as select * from test");
        stat.execute("drop table test cascade");
        conn.close();
        conn = getConnection("cases");
        stat = conn.createStatement();
        assertThrows(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, stat).
                execute("select * from abc");
        conn.close();
    }

    private void testDefaultQueryReconnect() throws SQLException {
        trace("testDefaultQueryReconnect");
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create table parent(id int)");
        stat.execute("insert into parent values(1)");
        stat.execute("create table test(id int default " +
                "(select max(id) from parent), name varchar)");

        conn.close();
        conn = getConnection("cases");
        stat = conn.createStatement();
        conn.setAutoCommit(false);
        stat.execute("insert into parent values(2)");
        stat.execute("insert into test(name) values('test')");
        ResultSet rs = stat.executeQuery("select * from test");
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());
        conn.close();
    }

    private void testBigString() throws SQLException {
        trace("testBigString");
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT, TEXT VARCHAR, TEXT_C CLOB)");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?, ?, ?)");
        int len = getSize(1000, 66000);
        char[] buff = new char[len];

        // Unicode problem:
        // The UCS code values 0xd800-0xdfff (UTF-16 surrogates)
        // as well as 0xfffe and 0xffff (UCS non-characters)
        // should not appear in conforming UTF-8 streams.
        // (String.getBytes("UTF-8") only returns 1 byte for 0xd800-0xdfff)
        Random random = new Random();
        random.setSeed(1);
        for (int i = 0; i < len; i++) {
            char c;
            do {
                c = (char) random.nextInt();
            } while (c >= 0xd800 && c <= 0xdfff);
            buff[i] = c;
        }
        String big = new String(buff);
        prep.setInt(1, 1);
        prep.setString(2, big);
        prep.setString(3, big);
        prep.execute();
        prep.setInt(1, 2);
        prep.setCharacterStream(2, new StringReader(big), 0);
        prep.setCharacterStream(3, new StringReader(big), 0);
        prep.execute();
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals(big, rs.getString(2));
        assertEquals(big, readString(rs.getCharacterStream(2)));
        assertEquals(big, rs.getString(3));
        assertEquals(big, readString(rs.getCharacterStream(3)));
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals(big, rs.getString(2));
        assertEquals(big, readString(rs.getCharacterStream(2)));
        assertEquals(big, rs.getString(3));
        assertEquals(big, readString(rs.getCharacterStream(3)));
        rs.next();
        assertFalse(rs.next());
        conn.close();
    }

    private void testConstraintReconnect() throws SQLException {
        trace("testConstraintReconnect");
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("drop table if exists parent");
        stat.execute("drop table if exists child");
        stat.execute("create table parent(id int)");
        stat.execute("create table child(c_id int, p_id int, " +
                "foreign key(p_id) references parent(id))");
        stat.execute("insert into parent values(1), (2)");
        stat.execute("insert into child values(1, 1)");
        stat.execute("insert into child values(2, 2)");
        stat.execute("insert into child values(3, 2)");
        stat.execute("delete from child");
        conn.close();
        conn = getConnection("cases");
        conn.close();
    }

    private void testDoubleRecovery() throws SQLException {
        if (config.networked || config.googleAppEngine) {
            return;
        }
        trace("testDoubleRecovery");
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("SET WRITE_DELAY 0");
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST" +
                "(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        conn.setAutoCommit(false);
        stat.execute("INSERT INTO TEST VALUES(2, 'World')");
        crash(conn);

        conn = getConnection("cases");
        stat = conn.createStatement();
        stat.execute("SET WRITE_DELAY 0");
        stat.execute("INSERT INTO TEST VALUES(3, 'Break')");
        crash(conn);

        conn = getConnection("cases");
        stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        rs.next();
        assertEquals(3, rs.getInt(1));
        assertEquals("Break", rs.getString(2));
        conn.close();
    }

    private void testRenameReconnect() throws SQLException {
        trace("testRenameReconnect");
        deleteDb("cases");
        Connection conn = getConnection("cases");
        conn.createStatement().execute("CREATE TABLE TEST_SEQ" +
                "(ID INT IDENTITY, NAME VARCHAR(255))");
        conn.createStatement().execute("CREATE TABLE TEST" +
                "(ID INT PRIMARY KEY)");
        conn.createStatement().execute("ALTER TABLE TEST RENAME TO TEST2");
        conn.createStatement().execute("CREATE TABLE TEST_B" +
                "(ID INT PRIMARY KEY, NAME VARCHAR, UNIQUE(NAME))");
        conn.close();
        conn = getConnection("cases");
        conn.createStatement().execute("INSERT INTO TEST_SEQ(NAME) VALUES('Hi')");
        ResultSet rs = conn.createStatement().executeQuery("CALL IDENTITY()");
        rs.next();
        assertEquals(1, rs.getInt(1));
        conn.createStatement().execute("SELECT * FROM TEST2");
        conn.createStatement().execute("SELECT * FROM TEST_B");
        conn.createStatement().execute("ALTER TABLE TEST_B RENAME TO TEST_B2");
        conn.close();
        conn = getConnection("cases");
        conn.createStatement().execute("SELECT * FROM TEST_B2");
        conn.createStatement().execute(
                "INSERT INTO TEST_SEQ(NAME) VALUES('World')");
        rs = conn.createStatement().executeQuery("CALL IDENTITY()");
        rs.next();
        assertEquals(2, rs.getInt(1));
        conn.close();
    }

    private void testAllSizes() throws SQLException {
        trace("testAllSizes");
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(A INT, B INT, C INT, DATA VARCHAR)");
        int increment = getSize(100, 1);
        for (int i = 1; i < 500; i += increment) {
            StringBuilder buff = new StringBuilder();
            buff.append("CREATE TABLE TEST");
            for (int j = 0; j < i; j++) {
                buff.append('a');
            }
            buff.append("(ID INT)");
            String sql = buff.toString();
            stat.execute(sql);
            stat.execute("INSERT INTO TEST VALUES(" + i + ", 0, 0, '" + sql + "')");
        }
        conn.close();
        conn = getConnection("cases");
        stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
        while (rs.next()) {
            int id = rs.getInt(1);
            String s = rs.getString("DATA");
            if (!s.endsWith(")")) {
                fail("id=" + id);
            }
        }
        conn.close();
    }

    private void testSelectForUpdate() throws SQLException {
        trace("testSelectForUpdate");
        deleteDb("cases");
        Connection conn1 = getConnection("cases");
        Statement stat1 = conn1.createStatement();
        stat1.execute("CREATE TABLE TEST(ID INT)");
        stat1.execute("INSERT INTO TEST VALUES(1)");
        conn1.setAutoCommit(false);
        stat1.execute("SELECT * FROM TEST FOR UPDATE");
        Connection conn2 = getConnection("cases");
        Statement stat2 = conn2.createStatement();
        assertThrows(ErrorCode.LOCK_TIMEOUT_1, stat2).
                execute("UPDATE TEST SET ID=2");
        conn1.commit();
        stat2.execute("UPDATE TEST SET ID=2");
        conn1.close();
        conn2.close();
    }

    private void testMutableObjects() throws SQLException {
        trace("testMutableObjects");
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT, D DATE, T TIME, TS TIMESTAMP)");
        stat.execute("INSERT INTO TEST VALUES(1, '2001-01-01', " +
                "'20:00:00', '2002-02-02 22:22:22.2')");
        stat.execute("INSERT INTO TEST VALUES(1, '2001-01-01', " +
                "'20:00:00', '2002-02-02 22:22:22.2')");
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
        rs.next();
        Date d1 = rs.getDate("D");
        Time t1 = rs.getTime("T");
        Timestamp ts1 = rs.getTimestamp("TS");
        rs.next();
        Date d2 = rs.getDate("D");
        Time t2 = rs.getTime("T");
        Timestamp ts2 = rs.getTimestamp("TS");
        assertTrue(ts1 != ts2);
        assertTrue(d1 != d2);
        assertTrue(t1 != t2);
        assertTrue(t2 != rs.getObject("T"));
        assertTrue(d2 != rs.getObject("D"));
        assertTrue(ts2 != rs.getObject("TS"));
        assertFalse(rs.next());
        conn.close();
    }

    private void testCreateDrop() throws SQLException {
        trace("testCreateDrop");
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create table employee(id int, firstName VARCHAR(50), "
                + "salary decimal(10, 2), "
                + "superior_id int, CONSTRAINT PK_employee PRIMARY KEY (id), "
                + "CONSTRAINT FK_superior FOREIGN KEY (superior_id) "
                + "REFERENCES employee(ID))");
        stat.execute("DROP TABLE employee");
        conn.close();
        conn = getConnection("cases");
        conn.close();
    }

    private void testPolePos() throws SQLException {
        trace("testPolePos");
        // poleposition-0.20

        Connection c0 = getConnection("cases");
        c0.createStatement().executeUpdate("SET AUTOCOMMIT FALSE");
        c0.createStatement().executeUpdate(
                "create table australia (ID  INTEGER NOT NULL, " +
                "Name VARCHAR(100), firstName VARCHAR(100), " +
                "Points INTEGER, LicenseID INTEGER, PRIMARY KEY(ID))");
        c0.createStatement().executeUpdate("COMMIT");
        c0.close();

        c0 = getConnection("cases");
        c0.createStatement().executeUpdate("SET AUTOCOMMIT FALSE");
        PreparedStatement p15 = c0.prepareStatement("insert into australia"
                + "(id, Name, firstName, Points, LicenseID) values (?, ?, ?, ?, ?)");
        int len = getSize(1, 1000);
        for (int i = 0; i < len; i++) {
            p15.setInt(1, i);
            p15.setString(2, "Pilot_" + i);
            p15.setString(3, "Herkules");
            p15.setInt(4, i);
            p15.setInt(5, i);
            p15.executeUpdate();
        }
        c0.createStatement().executeUpdate("COMMIT");
        c0.close();

        // c0=getConnection("cases");
        // c0.createStatement().executeUpdate("SET AUTOCOMMIT FALSE");
        // c0.createStatement().executeQuery("select * from australia");
        // c0.createStatement().executeQuery("select * from australia");
        // c0.close();

        // c0=getConnection("cases");
        // c0.createStatement().executeUpdate("SET AUTOCOMMIT FALSE");
        // c0.createStatement().executeUpdate("COMMIT");
        // c0.createStatement().executeUpdate("delete from australia");
        // c0.createStatement().executeUpdate("COMMIT");
        // c0.close();

        c0 = getConnection("cases");
        c0.createStatement().executeUpdate("SET AUTOCOMMIT FALSE");
        c0.createStatement().executeUpdate("drop table australia");
        c0.createStatement().executeUpdate("create table australia " +
                "(ID  INTEGER NOT NULL, Name VARCHAR(100), " +
                "firstName VARCHAR(100), Points INTEGER, " +
                "LicenseID INTEGER, PRIMARY KEY(ID))");
        c0.createStatement().executeUpdate("COMMIT");
        c0.close();

        c0 = getConnection("cases");
        c0.createStatement().executeUpdate("SET AUTOCOMMIT FALSE");
        PreparedStatement p65 = c0.prepareStatement(
                "insert into australia" +
                "(id, Name, FirstName, Points, LicenseID) values (?, ?, ?, ?, ?)");
        len = getSize(1, 1000);
        for (int i = 0; i < len; i++) {
            p65.setInt(1, i);
            p65.setString(2, "Pilot_" + i);
            p65.setString(3, "Herkules");
            p65.setInt(4, i);
            p65.setInt(5, i);
            p65.executeUpdate();
        }
        c0.createStatement().executeUpdate("COMMIT");
        c0.createStatement().executeUpdate("COMMIT");
        c0.createStatement().executeUpdate("COMMIT");
        c0.close();

        c0 = getConnection("cases");
        c0.close();
    }

    private void testQuick() throws SQLException {
        trace("testQuick");
        deleteDb("cases");

        Connection c0 = getConnection("cases");
        c0.createStatement().executeUpdate(
                "create table test (ID  int PRIMARY KEY)");
        c0.createStatement().executeUpdate("insert into test values(1)");
        c0.createStatement().executeUpdate("drop table test");
        c0.createStatement().executeUpdate(
                "create table test (ID  int PRIMARY KEY)");
        c0.close();

        c0 = getConnection("cases");
        c0.createStatement().executeUpdate("insert into test values(1)");
        c0.close();

        c0 = getConnection("cases");
        c0.close();
    }

    private void testOrderByWithSubselect() throws SQLException {
        deleteDb("cases");

        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create table master" +
                "(id number primary key, name varchar2(30));");
        stat.execute("create table detail" +
                "(id number references master(id), location varchar2(30));");

        stat.execute("Insert into master values(1,'a'), (2,'b'), (3,'c');");
        stat.execute("Insert into detail values(1,'a'), (2,'b'), (3,'c');");

        ResultSet rs = stat.executeQuery(
                "select master.id, master.name " +
                "from master " +
                "where master.id in (select detail.id from detail) " +
                "order by master.id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));

        conn.close();
    }

    private void testDeleteAndDropTableWithLobs(boolean useDrop)
            throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(id int, content BLOB)");
        stat.execute("set MAX_LENGTH_INPLACE_LOB 1");

        PreparedStatement prepared = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?, ?)");
        byte[] blobContent = "BLOB_CONTENT".getBytes();
        prepared.setInt(1, 1);
        prepared.setBytes(2, blobContent);
        prepared.execute();

        if (useDrop) {
            stat.execute("DROP TABLE TEST");
        } else {
            stat.execute("DELETE FROM TEST");
        }

        conn.close();

        List<String> list = FileUtils.newDirectoryStream(getBaseDir() +
                "/cases.lobs.db");
        assertEquals("Lob file was not deleted: " + list, 0, list.size());
    }

    private void testMinimalCoveringIndexPlan() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();

        stat.execute("create table t(a int, b int, c int)");
        stat.execute("create index a_idx on t(a)");
        stat.execute("create index b_idx on t(b)");
        stat.execute("create index ab_idx on t(a, b)");
        stat.execute("create index abc_idx on t(a, b, c)");

        ResultSet rs;
        String plan;

        rs = stat.executeQuery("explain select a from t");
        assertTrue(rs.next());
        plan = rs.getString(1);
        assertContains(plan, "/* PUBLIC.A_IDX */");
        rs.close();

        rs = stat.executeQuery("explain select b from t");
        assertTrue(rs.next());
        plan = rs.getString(1);
        assertContains(plan, "/* PUBLIC.B_IDX */");
        rs.close();

        rs = stat.executeQuery("explain select b, a from t");
        assertTrue(rs.next());
        plan = rs.getString(1);
        assertContains(plan, "/* PUBLIC.AB_IDX */");
        rs.close();

        rs = stat.executeQuery("explain select b, a, c from t");
        assertTrue(rs.next());
        plan = rs.getString(1);
        assertContains(plan, "/* PUBLIC.ABC_IDX */");
        rs.close();

        conn.close();
    }

    private void testMinMaxDirectLookupIndex() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();

        stat.execute("create table t(a int, b int)");
        stat.execute("create index b_idx on t(b desc)");
        stat.execute("create index ab_idx on t(a, b)");

        final int count = 100;

        PreparedStatement p = conn.prepareStatement("insert into t values (?,?)");
        for (int i = 0; i <= count; i++) {
            p.setInt(1, i);
            p.setInt(2, count - i);
            assertEquals(1, p.executeUpdate());
        }
        p.close();

        ResultSet rs;
        String plan;

        rs = stat.executeQuery("select max(b) from t");
        assertTrue(rs.next());
        assertEquals(count, rs.getInt(1));
        rs.close();

        rs = stat.executeQuery("explain select max(b) from t");
        assertTrue(rs.next());
        plan = rs.getString(1);
        assertContains(plan, "/* PUBLIC.B_IDX */");
        assertContains(plan, "/* direct lookup */");
        rs.close();

        rs = stat.executeQuery("select min(b) from t");
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        rs.close();

        rs = stat.executeQuery("explain select min(b) from t");
        assertTrue(rs.next());
        plan = rs.getString(1);
        assertContains(plan, "/* PUBLIC.B_IDX */");
        assertContains(plan, "/* direct lookup */");
        rs.close();

        conn.close();
    }

    private void testDeleteTop() throws SQLException {
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();

        stat.execute("CREATE TABLE TEST(id int) AS " +
                "SELECT x FROM system_range(1, 100)");
        stat.execute("DELETE TOP 10 FROM TEST");
        ResultSet rs = stat.executeQuery("SELECT COUNT(*) FROM TEST");
        assertTrue(rs.next());
        assertEquals(90, rs.getInt(1));

        stat.execute("DELETE FROM TEST LIMIT ((SELECT COUNT(*) FROM TEST) / 10)");
        rs = stat.executeQuery("SELECT COUNT(*) FROM TEST");
        assertTrue(rs.next());
        assertEquals(81, rs.getInt(1));

        rs = stat.executeQuery("EXPLAIN DELETE " +
                "FROM TEST LIMIT ((SELECT COUNT(*) FROM TEST) / 10)");
        rs.next();
        assertEquals("DELETE FROM PUBLIC.TEST\n" +
                "    /* PUBLIC.TEST.tableScan */\n" +
                "LIMIT ((SELECT\n" +
                "    COUNT(*)\n" +
                "FROM PUBLIC.TEST\n" +
                "    /* PUBLIC.TEST.tableScan */\n" +
                "/* direct lookup */) / 10)",
                rs.getString(1));

        PreparedStatement prep;
        prep = conn.prepareStatement("SELECT * FROM TEST LIMIT ?");
        prep.setInt(1, 10);
        prep.execute();

        prep = conn.prepareStatement("DELETE FROM TEST LIMIT ?");
        prep.setInt(1, 10);
        prep.execute();
        rs = stat.executeQuery("SELECT COUNT(*) FROM TEST");
        assertTrue(rs.next());
        assertEquals(71, rs.getInt(1));

        conn.close();
    }

    /** Tests fix for bug #682: Queries with 'like' expressions may filter rows incorrectly */
    private void testLikeExpressions() throws SQLException {
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select * from (select 'fo%' a union all select '%oo') where 'foo' like a");
        assertTrue(rs.next());
        assertEquals("fo%", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("%oo", rs.getString(1));
        conn.close();
    }
}
