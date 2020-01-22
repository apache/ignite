/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.mvcc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;

/**
 * Basic MVCC (multi version concurrency) test cases.
 */
public class TestMvcc1 extends TestBase {

    private Connection c1, c2;
    private Statement s1, s2;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.config.mvcc = true;
        test.test();
    }

    @Override
    public void test() throws SQLException {
        testCases();
        testSetMode();
        deleteDb("mvcc1");
    }

    private void testSetMode() throws SQLException {
        deleteDb("mvcc1");
        c1 = getConnection("mvcc1;MVCC=FALSE");
        Statement stat = c1.createStatement();
        ResultSet rs = stat.executeQuery(
                "select * from information_schema.settings where name='MVCC'");
        rs.next();
        assertEquals("FALSE", rs.getString("VALUE"));
        assertThrows(ErrorCode.CANNOT_CHANGE_SETTING_WHEN_OPEN_1, stat).
                execute("SET MVCC TRUE");
        rs = stat.executeQuery("select * from information_schema.settings " +
                "where name='MVCC'");
        rs.next();
        assertEquals("FALSE", rs.getString("VALUE"));
        c1.close();
    }

    private void testCases() throws SQLException {
        if (!config.mvcc) {
            return;
        }
        ResultSet rs;

        // TODO Prio 1: document: exclusive table lock still used when altering
        // tables, adding indexes, select ... for update; table level locks are
        // checked
        // TODO Prio 2: if MVCC is used, rows of transactions need to fit in
        // memory
        // TODO Prio 2: getFirst / getLast in MultiVersionIndex
        // TODO Prio 2: snapshot isolation (currently read-committed, not
        // repeatable read)

        // TODO test: one thread appends, the other
        //     selects new data (select * from test where id > ?) and deletes

        deleteDb("mvcc1");
        c1 = getConnection("mvcc1;MVCC=TRUE;LOCK_TIMEOUT=10");
        s1 = c1.createStatement();
        c2 = getConnection("mvcc1;MVCC=TRUE;LOCK_TIMEOUT=10");
        s2 = c2.createStatement();
        c1.setAutoCommit(false);
        c2.setAutoCommit(false);

        // table rollback problem
        assertThrows(ErrorCode.COLUMN_NOT_FOUND_1, s1).
                execute("create table b(primary key(x))");
        s1.execute("create table a(id int as 1 unique)");
        s1.execute("drop table a");

        // update same key problem
        s1.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR, PRIMARY KEY(ID))");
        s1.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        c1.commit();
        assertResult("Hello", s2, "SELECT NAME FROM TEST WHERE ID=1");
        s1.execute("UPDATE TEST SET NAME = 'Hallo' WHERE ID=1");
        assertResult("Hello", s2, "SELECT NAME FROM TEST WHERE ID=1");
        assertResult("Hallo", s1, "SELECT NAME FROM TEST WHERE ID=1");
        s1.execute("DROP TABLE TEST");
        c1.commit();
        c2.commit();

        // referential integrity problem
        s1.execute("create table a (id integer identity not null, " +
                "code varchar(10) not null, primary key(id))");
        s1.execute("create table b (name varchar(100) not null, a integer, " +
                "primary key(name), foreign key(a) references a(id))");
        s1.execute("insert into a(code) values('one')");
        assertThrows(ErrorCode.REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1, s2).
                execute("insert into b values('un B', 1)");
        c2.commit();
        c1.rollback();
        s1.execute("drop table a, b");
        c2.commit();

        // it should not be possible to drop a table
        // when an uncommitted transaction changed something
        s1.execute("create table test(id int primary key)");
        s1.execute("insert into test values(1)");
        assertThrows(ErrorCode.LOCK_TIMEOUT_1, s2).
                execute("drop table test");
        c1.rollback();
        s2.execute("drop table test");
        c2.rollback();

        // table scan problem
        s1.execute("create table test(id int, name varchar)");
        s1.execute("insert into test values(1, 'A'), (2, 'B')");
        c1.commit();
        assertResult("2", s1, "select count(*) from test where name<>'C'");
        s2.execute("update test set name='B2' where id=2");
        assertResult("2", s1, "select count(*) from test where name<>'C'");
        c2.commit();
        s2.execute("drop table test");
        c2.rollback();

        // select for update should do an exclusive lock, even with mvcc
        s1.execute("create table test(id int primary key, name varchar(255))");
        s1.execute("insert into test values(1, 'y')");
        c1.commit();
        s2.execute("select * from test where id = 1 for update");
        assertThrows(ErrorCode.LOCK_TIMEOUT_1, s1).
                execute("delete from test");
        c2.rollback();
        s1.execute("drop table test");
        c1.commit();
        c2.commit();

        s1.execute("create table test(id int primary key, name varchar(255))");
        s2.execute("insert into test values(4, 'Hello')");
        c2.rollback();
        assertResult("0", s1, "select count(*) from test where name = 'Hello'");
        assertResult("0", s2, "select count(*) from test where name = 'Hello'");
        c1.commit();
        c2.commit();
        s1.execute("DROP TABLE TEST");

        s1.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        s1.execute("INSERT INTO TEST VALUES(1, 'Test')");
        c1.commit();
        assertResult("1", s1, "select max(id) from test");
        s1.execute("INSERT INTO TEST VALUES(2, 'World')");
        c1.rollback();
        assertResult("1", s1, "select max(id) from test");
        c1.commit();
        c2.commit();
        s1.execute("DROP TABLE TEST");


        s1.execute("create table test as select * from table(id int=(1, 2))");
        s1.execute("update test set id=1 where id=1");
        s1.execute("select max(id) from test");
        assertResult("2", s1, "select max(id) from test");
        c1.commit();
        c2.commit();
        s1.execute("DROP TABLE TEST");

        s1.execute("CREATE TABLE TEST(ID INT)");
        s1.execute("INSERT INTO TEST VALUES(1)");
        c1.commit();
        assertResult("1", s2, "SELECT COUNT(*) FROM TEST");
        s1.executeUpdate("DELETE FROM TEST");
        PreparedStatement p2 = c2.prepareStatement("select count(*) from test");
        rs = p2.executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertResult("1", s2, "SELECT COUNT(*) FROM TEST");
        assertResult("0", s1, "SELECT COUNT(*) FROM TEST");
        c1.commit();
        assertResult("0", s2, "SELECT COUNT(*) FROM TEST");
        rs = p2.executeQuery();
        rs.next();
        assertEquals(0, rs.getInt(1));
        c1.commit();
        c2.commit();
        s1.execute("DROP TABLE TEST");

        s1.execute("CREATE TABLE TEST(ID INT)");
        s1.execute("INSERT INTO TEST VALUES(1)");
        c1.commit();
        s1.execute("DELETE FROM TEST");
        assertResult("0", s1, "SELECT COUNT(*) FROM TEST");
        c1.commit();
        assertResult("0", s1, "SELECT COUNT(*) FROM TEST");
        s1.execute("INSERT INTO TEST VALUES(1)");
        s1.execute("DELETE FROM TEST");
        c1.commit();
        assertResult("0", s1, "SELECT COUNT(*) FROM TEST");
        s1.execute("DROP TABLE TEST");

        s1.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        s1.execute("INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World')");
        assertResult("0", s2, "SELECT COUNT(*) FROM TEST");
        c1.commit();
        assertResult("2", s2, "SELECT COUNT(*) FROM TEST");
        s1.execute("INSERT INTO TEST VALUES(3, '!')");
        c1.rollback();
        assertResult("2", s2, "SELECT COUNT(*) FROM TEST");
        s1.execute("DROP TABLE TEST");
        c1.commit();

        s1.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        s1.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        s1.execute("DELETE FROM TEST");
        assertResult("0", s2, "SELECT COUNT(*) FROM TEST");
        c1.commit();
        assertResult("0", s2, "SELECT COUNT(*) FROM TEST");
        s1.execute("DROP TABLE TEST");
        c1.commit();

        s1.execute("CREATE TABLE TEST(ID INT IDENTITY, NAME VARCHAR)");
        s1.execute("INSERT INTO TEST(NAME) VALUES('Ruebezahl')");
        assertResult("0", s2, "SELECT COUNT(*) FROM TEST");
        assertResult("1", s1, "SELECT COUNT(*) FROM TEST");
        s1.execute("DROP TABLE TEST");
        c1.commit();

        s1.execute("CREATE TABLE TEST(ID INT IDENTITY, NAME VARCHAR)");
        s1.execute("INSERT INTO TEST(NAME) VALUES('Ruebezahl')");
        s1.execute("INSERT INTO TEST(NAME) VALUES('Ruebezahl')");
        s1.execute("DROP TABLE TEST");
        c1.commit();

        s1.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        s1.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        c1.commit();
        s1.execute("DELETE FROM TEST WHERE ID=1");
        c1.rollback();
        s1.execute("DROP TABLE TEST");
        c1.commit();

        Random random = new Random(1);
        s1.execute("CREATE TABLE TEST(ID INT IDENTITY, NAME VARCHAR)");
        Statement s;
        Connection c;
        for (int i = 0; i < 1000; i++) {
            if (random.nextBoolean()) {
                s = s1;
                c = c1;
            } else {
                s = s2;
                c = c2;
            }
            switch (random.nextInt(5)) {
            case 0:
                s.execute("INSERT INTO TEST(NAME) VALUES('Hello')");
                break;
            case 1:
                s.execute("UPDATE TEST SET NAME=" + i + " WHERE ID=" + random.nextInt(i));
                break;
            case 2:
                s.execute("DELETE FROM TEST WHERE ID=" + random.nextInt(i));
                break;
            case 3:
                c.commit();
                break;
            case 4:
                c.rollback();
                break;
            default:
            }
            s1.execute("SELECT * FROM TEST ORDER BY ID");
            s2.execute("SELECT * FROM TEST ORDER BY ID");
        }
        c2.rollback();
        s1.execute("DROP TABLE TEST");
        c1.commit();
        c2.commit();

        random = new Random(1);
        s1.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        for (int i = 0; i < 1000; i++) {
            if (random.nextBoolean()) {
                s = s1;
                c = c1;
            } else {
                s = s2;
                c = c2;
            }
            switch (random.nextInt(5)) {
            case 0:
                s.execute("INSERT INTO TEST VALUES(" + i + ", 'Hello')");
                break;
            case 1:
                try {
                    s.execute("UPDATE TEST SET NAME=" + i + " WHERE ID=" + random.nextInt(i));
                } catch (SQLException e) {
                    assertEquals(ErrorCode.CONCURRENT_UPDATE_1, e.getErrorCode());
                }
                break;
            case 2:
                s.execute("DELETE FROM TEST WHERE ID=" + random.nextInt(i));
                break;
            case 3:
                c.commit();
                break;
            case 4:
                c.rollback();
                break;
            default:
            }
            s1.execute("SELECT * FROM TEST ORDER BY ID");
            s2.execute("SELECT * FROM TEST ORDER BY ID");
        }
        c2.rollback();
        s1.execute("DROP TABLE TEST");
        c1.commit();
        c2.commit();

        s1.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR)");
        s1.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        assertResult("0", s2, "SELECT COUNT(*) FROM TEST WHERE NAME!='X'");
        assertResult("1", s1, "SELECT COUNT(*) FROM TEST WHERE NAME!='X'");
        c1.commit();
        assertResult("1", s2, "SELECT COUNT(*) FROM TEST WHERE NAME!='X'");
        assertResult("1", s2, "SELECT COUNT(*) FROM TEST WHERE NAME!='X'");
        s1.execute("DROP TABLE TEST");
        c1.commit();
        c2.commit();

        s1.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        s1.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        assertResult("0", s2, "SELECT COUNT(*) FROM TEST WHERE ID<100");
        assertResult("1", s1, "SELECT COUNT(*) FROM TEST WHERE ID<100");
        c1.commit();
        assertResult("1", s2, "SELECT COUNT(*) FROM TEST WHERE ID<100");
        assertResult("1", s2, "SELECT COUNT(*) FROM TEST WHERE ID<100");
        s1.execute("DROP TABLE TEST");
        c1.commit();
        c2.commit();

        s1.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR, PRIMARY KEY(ID, NAME))");
        s1.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        c1.commit();
        assertResult("Hello", s2, "SELECT NAME FROM TEST WHERE ID=1");
        s1.execute("UPDATE TEST SET NAME = 'Hallo' WHERE ID=1");
        assertResult("Hello", s2, "SELECT NAME FROM TEST WHERE ID=1");
        assertResult("Hallo", s1, "SELECT NAME FROM TEST WHERE ID=1");
        s1.execute("DROP TABLE TEST");
        c1.commit();
        c2.commit();


        s1.execute("create table test(id int primary key, name varchar(255))");
        s1.execute("insert into test values(1, 'Hello'), (2, 'World')");
        c1.commit();
        assertThrows(ErrorCode.DUPLICATE_KEY_1, s1).
                execute("update test set id=2 where id=1");
        rs = s1.executeQuery("select * from test order by id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("World", rs.getString(2));
        assertFalse(rs.next());

        rs = s2.executeQuery("select * from test order by id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("World", rs.getString(2));
        assertFalse(rs.next());
        s1.execute("drop table test");
        c1.commit();
        c2.commit();

        c1.close();
        c2.close();

    }

}
