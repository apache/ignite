/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.File;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.h2.api.ErrorCode;
import org.h2.dev.fs.FilePathZip2;
import org.h2.store.FileLister;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.tools.Backup;
import org.h2.tools.Server;

/**
 * Test for the read-only database feature.
 */
public class TestReadOnly extends TestBase {

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
        testReadOnlyInZip();
        testReadOnlyTempTableResult();
        testReadOnlyConnect();
        testReadOnlyDbCreate();
        if (!config.googleAppEngine) {
            testReadOnlyFiles(true);
        }
        testReadOnlyFiles(false);
    }

    private void testReadOnlyInZip() throws SQLException {
        if (config.cipher != null) {
            return;
        }
        deleteDb("readonlyInZip");
        String dir = getBaseDir();
        Connection conn = getConnection("readonlyInZip");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT) AS " +
                "SELECT X FROM SYSTEM_RANGE(1, 20)");
        conn.close();
        Backup.execute(dir + "/readonly.zip", dir, "readonlyInZip", true);
        conn = getConnection(
                "jdbc:h2:zip:"+dir+"/readonly.zip!/readonlyInZip", getUser(), getPassword());
        conn.createStatement().execute("select * from test where id=1");
        conn.close();
        Server server = Server.createTcpServer("-baseDir", dir);
        server.start();
        int port = server.getPort();
        try {
            conn = getConnection(
                    "jdbc:h2:tcp://localhost:" + port + "/zip:readonly.zip!/readonlyInZip",
                        getUser(), getPassword());
            conn.createStatement().execute("select * from test where id=1");
            conn.close();
            FilePathZip2.register();
            conn = getConnection(
                    "jdbc:h2:tcp://localhost:" + port + "/zip2:readonly.zip!/readonlyInZip",
                        getUser(), getPassword());
            conn.createStatement().execute("select * from test where id=1");
            conn.close();
        } finally {
            server.stop();
        }
        deleteDb("readonlyInZip");
    }

    private void testReadOnlyTempTableResult() throws SQLException {
        deleteDb("readonlyTemp");
        Connection conn = getConnection("readonlyTemp");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT) AS " +
                "SELECT X FROM SYSTEM_RANGE(1, 20)");
        conn.close();
        conn = getConnection(
                "readonlyTemp;ACCESS_MODE_DATA=r;" +
                "MAX_MEMORY_ROWS=10");
        stat = conn.createStatement();
        stat.execute("SELECT DISTINCT ID FROM TEST");
        conn.close();
        deleteDb("readonlyTemp");
    }

    private void testReadOnlyDbCreate() throws SQLException {
        deleteDb("readonlyDbCreate");
        Connection conn = getConnection("readonlyDbCreate");
        Statement stat = conn.createStatement();
        stat.execute("create table a(id int)");
        stat.execute("create index ai on a(id)");
        conn.close();
        conn = getConnection("readonlyDbCreate;ACCESS_MODE_DATA=r");
        stat = conn.createStatement();
        stat.execute("create table if not exists a(id int)");
        stat.execute("create index if not exists ai on a(id)");
        assertThrows(ErrorCode.DATABASE_IS_READ_ONLY, stat).
                execute("CREATE TABLE TEST(ID INT)");
        assertThrows(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, stat).
                execute("SELECT * FROM TEST");
        stat.execute("create local temporary linked table test(" +
                "null, 'jdbc:h2:mem:test3', 'sa', 'sa', 'INFORMATION_SCHEMA.TABLES')");
        ResultSet rs = stat.executeQuery("select * from test");
        assertTrue(rs.next());
        conn.close();
    }

    private void testReadOnlyFiles(boolean setReadOnly) throws Exception {
        new File(System.getProperty("java.io.tmpdir")).mkdirs();
        File f = File.createTempFile("test", "temp");
        assertTrue(f.canWrite());
        f.setReadOnly();
        assertTrue(!f.canWrite());
        f.delete();

        f = File.createTempFile("test", "temp");
        RandomAccessFile r = new RandomAccessFile(f, "rw");
        r.write(1);
        f.setReadOnly();
        r.close();
        assertTrue(!f.canWrite());
        f.delete();

        deleteDb("readonlyFiles");
        Connection conn = getConnection("readonlyFiles");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        stat.execute("INSERT INTO TEST VALUES(2, 'World')");
        assertTrue(!conn.isReadOnly());
        conn.close();

        if (setReadOnly) {
            setReadOnly();
            conn = getConnection("readonlyFiles");
        } else {
            conn = getConnection("readonlyFiles;ACCESS_MODE_DATA=r");
        }
        assertTrue(conn.isReadOnly());
        stat = conn.createStatement();
        stat.execute("SELECT * FROM TEST");
        assertThrows(ErrorCode.DATABASE_IS_READ_ONLY, stat).
                execute("DELETE FROM TEST");
        conn.close();

        if (setReadOnly) {
            conn = getConnection(
                    "readonlyFiles;DB_CLOSE_DELAY=1");
        } else {
            conn = getConnection(
                    "readonlyFiles;DB_CLOSE_DELAY=1;ACCESS_MODE_DATA=r");
        }
        stat = conn.createStatement();
        stat.execute("SELECT * FROM TEST");
        assertThrows(ErrorCode.DATABASE_IS_READ_ONLY, stat).
                execute("DELETE FROM TEST");
        stat.execute("SET DB_CLOSE_DELAY=0");
        conn.close();
    }

    private void setReadOnly() {
        ArrayList<String> list = FileLister.getDatabaseFiles(
                getBaseDir(), "readonlyFiles", true);
        for (String fileName : list) {
            FileUtils.setReadOnly(fileName);
        }
    }

    private void testReadOnlyConnect() throws SQLException {
        deleteDb("readonlyConnect");
        Connection conn = getConnection("readonlyConnect;OPEN_NEW=TRUE");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id identity)");
        stat.execute("insert into test select x from system_range(1, 11)");
        assertThrows(ErrorCode.DATABASE_ALREADY_OPEN_1, this).
                getConnection("readonlyConnect;ACCESS_MODE_DATA=r;OPEN_NEW=TRUE");
        conn.close();
        deleteDb("readonlyConnect");
    }

}
