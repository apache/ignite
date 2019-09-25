/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.engine.Constants;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Recover;
import org.h2.util.IOUtils;

/**
 * Tests database recovery.
 */
public class TestRecovery extends TestBase {

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
        if (config.memory) {
            return;
        }
        if (!config.mvStore) {
            testRecoverTestMode();
        }
        testRecoverClob();
        testRecoverFulltext();
        testRedoTransactions();
        testCorrupt();
        testWithTransactionLog();
        testCompressedAndUncompressed();
        testRunScript();
    }

    private void testRecoverTestMode() throws Exception {
        String recoverTestLog = getBaseDir() + "/recovery.h2.db.log";
        FileUtils.delete(recoverTestLog);
        deleteDb("recovery");
        Connection conn = getConnection("recovery;RECOVER_TEST=1");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int, name varchar)");
        stat.execute("drop all objects delete files");
        conn.close();
        assertTrue(FileUtils.exists(recoverTestLog));
    }

    private void testRecoverClob() throws Exception {
        DeleteDbFiles.execute(getBaseDir(), "recovery", true);
        Connection conn = getConnection("recovery");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int, data clob)");
        stat.execute("insert into test values(1, space(100000))");
        conn.close();
        Recover.main("-dir", getBaseDir(), "-db", "recovery");
        DeleteDbFiles.execute(getBaseDir(), "recovery", true);
        conn = getConnection(
                "recovery;init=runscript from '" +
                getBaseDir() + "/recovery.h2.sql'");
        stat = conn.createStatement();
        stat.execute("select * from test");
        conn.close();
    }

    private void testRecoverFulltext() throws Exception {
        DeleteDbFiles.execute(getBaseDir(), "recovery", true);
        Connection conn = getConnection("recovery");
        Statement stat = conn.createStatement();
        stat.execute("CREATE ALIAS IF NOT EXISTS FTL_INIT " +
                "FOR \"org.h2.fulltext.FullTextLucene.init\"");
        stat.execute("CALL FTL_INIT()");
        stat.execute("create table test(id int primary key, name varchar) as " +
                "select 1, 'Hello'");
        stat.execute("CALL FTL_CREATE_INDEX('PUBLIC', 'TEST', 'NAME')");
        conn.close();
        Recover.main("-dir", getBaseDir(), "-db", "recovery");
        DeleteDbFiles.execute(getBaseDir(), "recovery", true);
        conn = getConnection(
                "recovery;init=runscript from '" +
                getBaseDir() + "/recovery.h2.sql'");
        conn.close();
    }

    private void testRedoTransactions() throws Exception {
        if (config.mvStore) {
            // not needed for MV_STORE=TRUE
            return;
        }
        DeleteDbFiles.execute(getBaseDir(), "recovery", true);
        Connection conn = getConnection("recovery");
        Statement stat = conn.createStatement();
        stat.execute("set write_delay 0");
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("insert into test select x, 'Hello' from system_range(1, 5)");
        stat.execute("create table test2(id int primary key)");
        stat.execute("drop table test2");
        stat.execute("update test set name = 'Hallo' where id < 3");
        stat.execute("delete from test where id = 1");
        stat.execute("shutdown immediately");
        try {
            conn.close();
        } catch (Exception e) {
            // ignore
        }
        Recover.main("-dir", getBaseDir(), "-db", "recovery", "-transactionLog");
        DeleteDbFiles.execute(getBaseDir(), "recovery", true);
        conn = getConnection("recovery;init=runscript from '" +
                getBaseDir() + "/recovery.h2.sql'");
        stat = conn.createStatement();
        ResultSet rs;
        rs = stat.executeQuery("select * from test order by id");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("Hallo", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());
        conn.close();
    }

    private void testCorrupt() throws Exception {
        if (config.mvStore) {
            // not needed for MV_STORE=TRUE
            return;
        }
        DeleteDbFiles.execute(getBaseDir(), "recovery", true);
        Connection conn = getConnection("recovery");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int, name varchar) as " +
                "select 1, 'Hello World1'");
        conn.close();
        FileChannel f = FileUtils.open(getBaseDir() + "/recovery.h2.db", "rw");
        byte[] buff = new byte[Constants.DEFAULT_PAGE_SIZE];
        while (f.position() < f.size()) {
            FileUtils.readFully(f, ByteBuffer.wrap(buff));
            if (new String(buff).contains("Hello World1")) {
                buff[buff.length - 1]++;
                f.position(f.position() - buff.length);
                f.write(ByteBuffer.wrap(buff));
            }
        }
        f.close();
        Recover.main("-dir", getBaseDir(), "-db", "recovery");
        String script = IOUtils.readStringAndClose(
                new InputStreamReader(
                FileUtils.newInputStream(getBaseDir() + "/recovery.h2.sql")), -1);
        assertContains(script, "checksum mismatch");
        assertContains(script, "dump:");
        assertContains(script, "Hello World2");
    }

    private void testWithTransactionLog() throws SQLException {
        if (config.mvStore) {
            // not needed for MV_STORE=TRUE
            return;
        }
        DeleteDbFiles.execute(getBaseDir(), "recovery", true);
        Connection conn = getConnection("recovery");
        Statement stat = conn.createStatement();
        stat.execute("create table truncate(id int primary key) as " +
                "select x from system_range(1, 1000)");
        stat.execute("create table test(id int primary key, data int, text varchar)");
        stat.execute("create index on test(data, id)");
        stat.execute("insert into test direct select x, 0, null " +
                "from system_range(1, 1000)");
        stat.execute("insert into test values(-1, -1, space(10000))");
        stat.execute("checkpoint");
        stat.execute("delete from test where id = -1");
        stat.execute("truncate table truncate");
        conn.setAutoCommit(false);
        long base = 0;
        while (true) {
            ResultSet rs = stat.executeQuery(
                        "select value from information_schema.settings " +
                        "where name = 'info.FILE_WRITE'");
            rs.next();
            long count = rs.getLong(1);
            if (base == 0) {
                base = count;
            } else if (count > base + 10) {
                break;
            }
            stat.execute("update test set data=0");
            stat.execute("update test set text=space(10000) where id = 0");
            stat.execute("update test set data=1, text = null");
            conn.commit();
        }
        stat.execute("shutdown immediately");
        try {
            conn.close();
        } catch (Exception e) {
            // expected
        }
        Recover.main("-dir", getBaseDir(), "-db", "recovery");
        conn = getConnection("recovery");
        conn.close();
        Recover.main("-dir", getBaseDir(), "-db", "recovery", "-removePassword");
        conn = getConnection("recovery", getUser(), "");
        conn.close();
        DeleteDbFiles.execute(getBaseDir(), "recovery", true);
    }

    private void testCompressedAndUncompressed() throws SQLException {
        DeleteDbFiles.execute(getBaseDir(), "recovery", true);
        DeleteDbFiles.execute(getBaseDir(), "recovery2", true);
        org.h2.Driver.load();
        Connection conn = getConnection("recovery");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, data clob)");
        stat.execute("insert into test values(1, space(10000))");
        stat.execute("set compress_lob lzf");
        stat.execute("insert into test values(2, space(10000))");
        conn.close();
        Recover rec = new Recover();
        rec.runTool("-dir", getBaseDir(), "-db", "recovery");
        Connection conn2 = getConnection("recovery2");
        Statement stat2 = conn2.createStatement();
        String name = "recovery.h2.sql";
        stat2.execute("runscript from '" + getBaseDir() + "/" + name + "'");
        stat2.execute("select * from test");
        conn2.close();

        conn = getConnection("recovery");
        stat = conn.createStatement();
        conn2 = getConnection("recovery2");
        stat2 = conn2.createStatement();

        assertEqualDatabases(stat, stat2);
        conn.close();
        conn2.close();
        DeleteDbFiles.execute(getBaseDir(), "recovery", true);
        DeleteDbFiles.execute(getBaseDir(), "recovery2", true);
    }

    private void testRunScript() throws SQLException {
        DeleteDbFiles.execute(getBaseDir(), "recovery", true);
        DeleteDbFiles.execute(getBaseDir(), "recovery2", true);
        org.h2.Driver.load();
        Connection conn = getConnection("recovery");
        Statement stat = conn.createStatement();
        stat.execute("create table \"Joe\"\"s Table\" as " +
                "select 1");
        stat.execute("create table test as " +
                "select * from system_range(1, 100)");
        stat.execute("create view \"TEST VIEW OF TABLE TEST\" as " +
                "select * from test");
        stat.execute("create table a(id int primary key) as " +
                "select * from system_range(1, 100)");
        stat.execute("create table b(id int references a(id)) as " +
                "select * from system_range(1, 100)");
        stat.execute("create table lob(c clob, b blob) as " +
                "select space(10000) || 'end', SECURE_RAND(10000)");
        stat.execute("create table d(d varchar) as " +
                "select space(10000) || 'end'");
        stat.execute("alter table a add foreign key(id) references b(id)");
        // all rows have the same value - so that SCRIPT can't re-order the rows
        stat.execute("create table e(id varchar) as " +
                "select space(10) from system_range(1, 1000)");
        stat.execute("create index idx_e_id on e(id)");
        conn.close();

        Recover rec = new Recover();
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        rec.setOut(new PrintStream(buff));
        rec.runTool("-dir", getBaseDir(), "-db", "recovery", "-trace");
        String out = new String(buff.toByteArray());
        assertContains(out, "Created file");

        Connection conn2 = getConnection("recovery2");
        Statement stat2 = conn2.createStatement();
        String name = "recovery.h2.sql";

        stat2.execute("runscript from '" + getBaseDir() + "/" + name + "'");
        stat2.execute("select * from test");
        conn2.close();

        conn = getConnection("recovery");
        stat = conn.createStatement();
        conn2 = getConnection("recovery2");
        stat2 = conn2.createStatement();

        assertEqualDatabases(stat, stat2);
        conn.close();
        conn2.close();

        Recover.execute(getBaseDir(), "recovery");

        deleteDb("recovery");
        deleteDb("recovery2");
        FileUtils.delete(getBaseDir() + "/recovery.h2.sql");
        String dir = getBaseDir() + "/recovery.lobs.db";
        FileUtils.deleteRecursive(dir, false);
    }

}
