/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.h2.fulltext.FullText;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.util.IOUtils;
import org.h2.util.Task;

/**
 * Fulltext search tests.
 */
public class TestFullText extends TestBase {

    /**
     * The words used in this test.
     */
    static final String[] KNOWN_WORDS = { "skiing", "balance", "storage",
            "water", "train" };
    private static final String LUCENE_FULLTEXT_CLASS_NAME =
            "org.h2.fulltext.FullTextLucene";

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
        testUuidPrimaryKey(false);
        testAutoAnalyze();
        testNativeFeatures();
        testTransaction(false);
        testCreateDropNative();
        testStreamLob();
        test(false, "VARCHAR");
        test(false, "CLOB");
        testPerformance(false);
        testReopen(false);
        testDropIndex(false);
        if (!config.reopen) {
            try {
                Class.forName(LUCENE_FULLTEXT_CLASS_NAME);
                testCreateDropLucene();
                testUuidPrimaryKey(true);
                testMultiThreaded(true);
                if(config.mvStore || !config.multiThreaded) {
                    testMultiThreaded(false);
                }
                testTransaction(true);
                test(true, "VARCHAR");
                test(true, "CLOB");
                testPerformance(true);
                testReopen(true);
                testDropIndex(true);
            } catch (ClassNotFoundException e) {
                println("Class not found, not tested: " + LUCENE_FULLTEXT_CLASS_NAME);
                // ok
            } catch (NoClassDefFoundError e) {
                println("Class not found, not tested: " + LUCENE_FULLTEXT_CLASS_NAME);
                // ok
            }
            FullText.closeAll();
        }
        deleteDb("fullText");
        deleteDb("fullTextReopen");
    }

    private static void close(Collection<Connection> list) {
        for (Connection conn : list) {
            IOUtils.closeSilently(conn);
        }
    }

    private Connection getConnection(String name, Collection<Connection> list)
            throws SQLException {
        Connection conn = getConnection(name);
        list.add(conn);
        return conn;
    }

    private void testAutoAnalyze() throws SQLException {
        deleteDb("fullTextNative");
        Connection conn;
        Statement stat;

        ArrayList<Connection> connList = new ArrayList<>();

        conn = getConnection("fullTextNative", connList);
        stat = conn.createStatement();
        stat.execute("create alias if not exists ft_init " +
                "for \"org.h2.fulltext.FullText.init\"");
        stat.execute("call ft_init()");
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("call ft_create_index('PUBLIC', 'TEST', 'NAME')");

        if (!config.memory) {
            conn.close();
        }

        conn = getConnection("fullTextNative", connList);
        stat = conn.createStatement();
        stat.execute("insert into test select x, 'x' from system_range(1, 3000)");
        close(connList);
    }

    private void testNativeFeatures() throws SQLException {
        deleteDb("fullTextNative");
        ArrayList<Connection> connList = new ArrayList<>();
        Connection conn = getConnection("fullTextNative", connList);
        Statement stat = conn.createStatement();
        stat.execute("CREATE ALIAS IF NOT EXISTS FT_INIT " +
                "FOR \"org.h2.fulltext.FullText.init\"");
        stat.execute("CALL FT_INIT()");
        FullText.setIgnoreList(conn, "to,this");
        FullText.setWhitespaceChars(conn, " ,.-");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES(1, 'Welcome to this world, One_Word')");
        stat.execute("CALL FT_CREATE_INDEX('PUBLIC', 'TEST', NULL)");
        ResultSet rs;
        rs = stat.executeQuery("SELECT * FROM FT_SEARCH('Welcome', 0, 0)");
        assertTrue(rs.next());
        assertEquals("QUERY", rs.getMetaData().getColumnLabel(1));
        assertEquals("SCORE", rs.getMetaData().getColumnLabel(2));
        assertEquals("\"PUBLIC\".\"TEST\" WHERE \"ID\"=1", rs.getString(1));
        assertEquals("1.0", rs.getString(2));
        rs = stat.executeQuery("SELECT * FROM FT_SEARCH_DATA('One', 0, 0)");
        assertFalse(rs.next());
        rs = stat.executeQuery("SELECT * FROM FT_SEARCH_DATA('One_Word', 0, 0)");
        assertTrue(rs.next());
        rs = stat.executeQuery("SELECT * FROM FT_SEARCH_DATA('Welcome', 0, 0)");
        assertTrue(rs.next());
        assertEquals("SCHEMA", rs.getMetaData().getColumnLabel(1));
        assertEquals("TABLE", rs.getMetaData().getColumnLabel(2));
        assertEquals("COLUMNS", rs.getMetaData().getColumnLabel(3));
        assertEquals("KEYS", rs.getMetaData().getColumnLabel(4));
        assertEquals("PUBLIC", rs.getString(1));
        assertEquals("TEST", rs.getString(2));
        assertEquals("(ID)", rs.getString(3));
        assertEquals("(1)", rs.getString(4));

        rs = stat.executeQuery("SELECT * FROM FT_SEARCH('this', 0, 0)");
        assertFalse(rs.next());

        if (!config.memory) {
            conn.close();
        }
        conn = getConnection("fullTextNative", connList);
        stat = conn.createStatement();
        conn.setAutoCommit(false);
        rs = stat.executeQuery("SELECT * FROM FT_SEARCH_DATA('Welcome', 0, 0)");
        assertTrue(rs.next());
        stat.execute("delete from test");
        rs = stat.executeQuery("SELECT * FROM FT_SEARCH_DATA('Welcome', 0, 0)");
        assertFalse(rs.next());
        conn.rollback();
        rs = stat.executeQuery("SELECT * FROM FT_SEARCH_DATA('Welcome', 0, 0)");
        assertTrue(rs.next());
        conn.setAutoCommit(true);
        close(connList);
    }

    private void testUuidPrimaryKey(boolean lucene) throws SQLException {
        deleteDb("fullText");
        Connection conn = getConnection("fullText");
        Statement stat = conn.createStatement();
        String prefix = lucene ? "FTL" : "FT";
        initFullText(stat, lucene);
        stat.execute("CREATE TABLE TEST(ID UUID PRIMARY KEY, NAME VARCHAR)");
        String id = UUID.randomUUID().toString();
        stat.execute("INSERT INTO TEST VALUES('" + id + "', 'Hello World')");
        stat.execute("CALL " + prefix + "_CREATE_INDEX('PUBLIC', 'TEST', 'NAME')");
        ResultSet rs = stat.executeQuery("SELECT * FROM " +
                prefix + "_SEARCH('Hello', 0, 0)");
        assertTrue(rs.next());
        stat.execute("UPDATE TEST SET NAME=NULL WHERE ID='" + id + "'");
        rs = stat.executeQuery("SELECT * FROM " +
                prefix + "_SEARCH('Hello', 0, 0)");
        assertFalse(rs.next());
        stat.execute("UPDATE TEST SET NAME='Good Bye' WHERE ID='" + id + "'");
        rs = stat.executeQuery("SELECT * FROM " +
                prefix + "_SEARCH('bye', 0, 0)");
        assertTrue(rs.next());
        FullText.dropAll(conn);
        conn.close();
        deleteDb("fullText");
    }

    private void testTransaction(boolean lucene) throws SQLException {
        String prefix = lucene ? "FTL" : "FT";
        deleteDb("fullTextTransaction");
        FileUtils.deleteRecursive(getBaseDir() + "/fullTextTransaction", false);
        ArrayList<Connection> connList = new ArrayList<>();
        Connection conn = getConnection("fullTextTransaction", connList);
        Statement stat = conn.createStatement();
        initFullText(stat, lucene);
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello World')");
        stat.execute("CALL " + prefix + "_CREATE_INDEX('PUBLIC', 'TEST', NULL)");
        ResultSet rs = stat.executeQuery("SELECT * FROM " +
                prefix + "_SEARCH('Hello', 0, 0)");
        assertTrue(rs.next());
        stat.execute("UPDATE TEST SET NAME=NULL WHERE ID=1");
        stat.execute("UPDATE TEST SET NAME='Hello World' WHERE ID=1");
        conn.setAutoCommit(false);
        stat.execute("insert into test values(2, 'Hello Moon!')");
        conn.rollback();
        if (!config.memory) {
            conn.close();
        }
        conn = getConnection("fullTextTransaction", connList);
        stat = conn.createStatement();
        rs = stat.executeQuery("SELECT * FROM " +
                prefix + "_SEARCH('Hello', 0, 0)");
        assertTrue(rs.next());
        rs = stat.executeQuery("SELECT * FROM " +
                prefix + "_SEARCH('Moon', 0, 0)");
        assertFalse(rs.next());
        FullText.dropAll(conn);
        close(connList);
        deleteDb("fullTextTransaction");
        FileUtils.deleteRecursive(getBaseDir() + "/fullTextTransaction", false);
    }

    private void testMultiThreaded(boolean lucene) throws Exception {
        final String prefix = lucene ? "FTL" : "FT";
        trace("Testing multithreaded " + prefix);
        deleteDb("fullText");
        ArrayList<Connection> connList = new ArrayList<>();
        try {
            int len = 2;
            Task[] task = new Task[len];
            for (int i = 0; i < len; i++) {
                final Connection conn = getConnection("fullText;LOCK_TIMEOUT=60000", connList);
                Statement stat = conn.createStatement();
                initFullText(stat, lucene);
                initFullText(stat, lucene);
                final String tableName = "TEST" + i;
                stat.execute("CREATE TABLE " + tableName +
                        "(ID INT PRIMARY KEY, DATA VARCHAR)");
                stat.execute("CALL " + prefix +
                        "_CREATE_INDEX('PUBLIC', '" + tableName + "', NULL)");
                task[i] = new Task() {
                    @Override
                    public void call() throws SQLException {
                        trace("starting thread " + Thread.currentThread());
                        PreparedStatement prep = conn.prepareStatement(
                                "INSERT INTO " + tableName + " VALUES(?, ?)");
                        Statement stat = conn.createStatement();
                        Random random = new Random();
                        int x = 0;
                        while (!stop) {
                            trace("stop = " + stop + " for " + Thread.currentThread());
                            StringBuilder buff = new StringBuilder();
                            for (int j = 0; j < 1000; j++) {
                                buff.append(" ").append(random.nextInt(10000));
                                buff.append(" x").append(j);
                                buff.append(" ").append(KNOWN_WORDS[j % KNOWN_WORDS.length]);
                            }
                            prep.setInt(1, x);
                            prep.setString(2, buff.toString());
                            prep.execute();
                            x++;
                            for (String knownWord : KNOWN_WORDS) {
                                trace("searching for " + knownWord + " with " +
                                        Thread.currentThread());
                                ResultSet rs = stat.executeQuery("SELECT * FROM " +
                                        prefix + "_SEARCH('" + knownWord +
                                        "', 0, 0)");
                                assertTrue(rs.next());
                            }
                        }
                        trace("closing connection");
                        if (!config.memory) {
                            conn.close();
                        }
                        trace("completed thread " + Thread.currentThread());
                    }
                };
            }
            for (Task t : task) {
                t.execute();
            }
            trace("sleeping");
            Thread.sleep(1000);

            trace("setting stop to true");
            for (Task t : task) {
                trace("joining " + t);
                t.get();
                trace("done joining " + t);
            }
        } finally {
            close(connList);
        }
    }

    private void testStreamLob() throws SQLException {
        deleteDb("fullText");
        Connection conn = getConnection("fullText");
        Statement stat = conn.createStatement();
        stat.execute("CREATE ALIAS IF NOT EXISTS FT_INIT " +
                "FOR \"org.h2.fulltext.FullText.init\"");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, DATA CLOB)");
        FullText.createIndex(conn, "PUBLIC", "TEST", null);
        conn.setAutoCommit(false);
        stat.execute("insert into test values(1, 'Hello Moon!')");
        conn.rollback();
        conn.setAutoCommit(true);
        stat.execute("insert into test values(0, 'Hello World!')");
        PreparedStatement prep = conn.prepareStatement(
                "insert into test values(1, ?)");
        final int length = 1024 * 1024;
        prep.setCharacterStream(1, new Reader() {
            int remaining = length;

            @Override
            public void close() {
                // ignore
            }

            @Override
            public int read(char[] buff, int off, int len) {
                if (remaining >= len) {
                    remaining -= len;
                    return len;
                }
                remaining = -1;
                return -1;
            }
        }, length);
        prep.execute();
        ResultSet rs = stat.executeQuery(
                "SELECT * FROM FT_SEARCH('World', 0, 0)");
        assertTrue(rs.next());
        rs = stat.executeQuery("SELECT * FROM FT_SEARCH('Moon', 0, 0)");
        assertFalse(rs.next());
        FullText.dropAll(conn);
        conn.close();
        deleteDb("fullText");
    }

    private void testCreateDropNative() throws SQLException {
        deleteDb("fullText");
        FileUtils.deleteRecursive(getBaseDir() + "/fullText", false);
        Connection conn = getConnection("fullText");
        Statement stat = conn.createStatement();
        stat.execute("CREATE ALIAS IF NOT EXISTS FT_INIT " +
                "FOR \"org.h2.fulltext.FullText.init\"");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        for (int i = 0; i < 10; i++) {
            FullText.createIndex(conn, "PUBLIC", "TEST", null);
            FullText.dropIndex(conn, "PUBLIC", "TEST");
        }
        conn.close();
        deleteDb("fullText");
        FileUtils.deleteRecursive(getBaseDir() + "/fullText", false);
    }

    private void testCreateDropLucene() throws SQLException, SecurityException,
            NoSuchMethodException, ClassNotFoundException,
            IllegalArgumentException, IllegalAccessException,
            InvocationTargetException {
        deleteDb("fullText");
        FileUtils.deleteRecursive(getBaseDir() + "/fullText", false);
        Connection conn = getConnection("fullText");
        Statement stat = conn.createStatement();
        initFullText(stat, true);
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        Method createIndexMethod = Class.forName(
                LUCENE_FULLTEXT_CLASS_NAME).getMethod("createIndex",
                new Class[] { java.sql.Connection.class,
                        String.class, String.class, String.class });
        Method dropIndexMethod = Class.forName(
                LUCENE_FULLTEXT_CLASS_NAME).getMethod("dropIndex",
                new Class[] { java.sql.Connection.class,
                        String.class, String.class });
        for (int i = 0; i < 10; i++) {
            createIndexMethod.invoke(null, conn, "PUBLIC", "TEST", null);
            dropIndexMethod.invoke(null, conn, "PUBLIC", "TEST");
        }
        conn.close();
        deleteDb("fullText");
        FileUtils.deleteRecursive(getBaseDir() + "/fullText", false);
    }

    private void testReopen(boolean lucene) throws SQLException {
        if (config.memory) {
            return;
        }
        String prefix = lucene ? "FTL" : "FT";
        deleteDb("fullTextReopen");
        FileUtils.deleteRecursive(getBaseDir() + "/fullTextReopen", false);
        Connection conn = getConnection("fullTextReopen");
        Statement stat = conn.createStatement();
        initFullText(stat, lucene);
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello World')");
        stat.execute("CALL " + prefix + "_CREATE_INDEX('PUBLIC', 'TEST', NULL)");
        stat.execute("UPDATE TEST SET NAME=NULL WHERE ID=1");
        stat.execute("UPDATE TEST SET NAME='Hello World' WHERE ID=1");
        conn.close();

        conn = getConnection("fullTextReopen");
        stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT * FROM " +
                prefix + "_SEARCH('Hello', 0, 0)");
        assertTrue(rs.next());
        stat.executeQuery("SELECT * FROM " + prefix + "_SEARCH(NULL, 0, 0)");
        stat.execute("INSERT INTO TEST VALUES(2, NULL)");
        conn.close();

        FullText.closeAll();
        conn = getConnection("fullTextReopen");
        stat = conn.createStatement();
        stat.execute("INSERT INTO TEST VALUES(3, 'Hello')");
        conn.close();
        FileUtils.deleteRecursive(getBaseDir() + "/fullTextReopen", false);
    }

    private void testPerformance(boolean lucene) throws SQLException {
        deleteDb("fullText");
        FileUtils.deleteRecursive(getBaseDir() + "/fullText", false);
        Connection conn = getConnection("fullText");
        String prefix = lucene ? "FTL" : "FT";
        Statement stat = conn.createStatement();
        initFullText(stat, lucene);
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute(
                "CREATE TABLE TEST AS SELECT * FROM INFORMATION_SCHEMA.HELP");
        stat.execute("ALTER TABLE TEST ALTER COLUMN ID INT NOT NULL");
        stat.execute("CREATE PRIMARY KEY ON TEST(ID)");
        long time = System.nanoTime();
        stat.execute("CALL " + prefix + "_CREATE_INDEX('PUBLIC', 'TEST', NULL)");
        println("create " + prefix + ": " +
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time));
        PreparedStatement prep = conn.prepareStatement(
                "SELECT * FROM " + prefix + "_SEARCH(?, 0, 0)");
        time = System.nanoTime();
        ResultSet rs = stat.executeQuery("SELECT TEXT FROM TEST");
        int count = 0;
        while (rs.next()) {
            String text = rs.getString(1);
            StringTokenizer tokenizer = new StringTokenizer(
                        text, " ()[].,;:-+*/!?=<>{}#@'\"~$_%&|");
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken();
                if (word.length() < 10) {
                    continue;
                }
                prep.setString(1, word);
                ResultSet rs2 = prep.executeQuery();
                while (rs2.next()) {
                    rs2.getString(1);
                    count++;
                }
            }
        }
        println("search " + prefix + ": " +
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time) + " count: " + count);
        stat.execute("CALL " + prefix + "_DROP_ALL()");
        conn.close();
    }

    private void test(boolean lucene, String dataType) throws SQLException {
        if (lucene && getBaseDir().indexOf(':') > 0) {
            return;
        }
        deleteDb("fullText");
        Connection conn = getConnection("fullText");
        String prefix = lucene ? "FTL_" : "FT_";
        Statement stat = conn.createStatement();
        String className = lucene ? "FullTextLucene" : "FullText";
        stat.execute("CREATE ALIAS IF NOT EXISTS " +
                prefix + "INIT FOR \"org.h2.fulltext." + className + ".init\"");
        stat.execute("CALL " + prefix + "INIT()");
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME " + dataType + ")");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello World')");
        stat.execute("CALL " + prefix + "CREATE_INDEX('PUBLIC', 'TEST', NULL)");
        ResultSet rs;
        rs = stat.executeQuery("SELECT * FROM " +
                prefix + "SEARCH('Hello', 0, 0)");
        rs.next();
        assertEquals("\"PUBLIC\".\"TEST\" WHERE \"ID\"=1", rs.getString(1));
        assertFalse(rs.next());
        rs = stat.executeQuery("SELECT * FROM " +
                prefix + "SEARCH('Hallo', 0, 0)");
        assertFalse(rs.next());
        stat.execute("INSERT INTO TEST VALUES(2, 'Hallo Welt')");
        rs = stat.executeQuery("SELECT * FROM " +
                prefix + "SEARCH('Hello', 0, 0)");
        rs.next();
        assertEquals("\"PUBLIC\".\"TEST\" WHERE \"ID\"=1", rs.getString(1));
        assertFalse(rs.next());
        rs = stat.executeQuery("SELECT * FROM " +
                prefix + "SEARCH('Hallo', 0, 0)");
        rs.next();
        assertEquals("\"PUBLIC\".\"TEST\" WHERE \"ID\"=2", rs.getString(1));
        assertFalse(rs.next());

        stat.execute("CALL " + prefix + "REINDEX()");
        rs = stat.executeQuery("SELECT * FROM " +
                prefix + "SEARCH('Hello', 0, 0)");
        rs.next();
        assertEquals("\"PUBLIC\".\"TEST\" WHERE \"ID\"=1", rs.getString(1));
        assertFalse(rs.next());
        rs = stat.executeQuery("SELECT * FROM " +
                prefix + "SEARCH('Hallo', 0, 0)");
        rs.next();
        assertEquals("\"PUBLIC\".\"TEST\" WHERE \"ID\"=2", rs.getString(1));
        assertFalse(rs.next());

        stat.execute("INSERT INTO TEST VALUES(3, 'Hello World')");
        stat.execute("INSERT INTO TEST VALUES(4, 'Hello World')");
        stat.execute("INSERT INTO TEST VALUES(5, 'Hello World')");

        rs = stat.executeQuery("SELECT * FROM " +
                prefix + "SEARCH('World', 0, 0) ORDER BY QUERY");
        rs.next();
        assertEquals("\"PUBLIC\".\"TEST\" WHERE \"ID\"=1", rs.getString(1));
        rs.next();
        assertEquals("\"PUBLIC\".\"TEST\" WHERE \"ID\"=3", rs.getString(1));
        rs.next();
        assertEquals("\"PUBLIC\".\"TEST\" WHERE \"ID\"=4", rs.getString(1));
        rs.next();
        assertEquals("\"PUBLIC\".\"TEST\" WHERE \"ID\"=5", rs.getString(1));
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT * FROM " +
                prefix + "SEARCH('World', 1, 0)");
        rs.next();
        assertTrue(rs.getString(1).startsWith("\"PUBLIC\".\"TEST\" WHERE \"ID\"="));
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT * FROM " +
                prefix + "SEARCH('World', 0, 2) ORDER BY QUERY");
        rs.next();
        assertTrue(rs.getString(1).startsWith("\"PUBLIC\".\"TEST\" WHERE \"ID\"="));
        rs.next();
        assertTrue(rs.getString(1).startsWith("\"PUBLIC\".\"TEST\" WHERE \"ID\"="));
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT * FROM " +
                prefix + "SEARCH('World', 2, 1) ORDER BY QUERY");
        rs.next();
        assertTrue(rs.getString(1).startsWith("\"PUBLIC\".\"TEST\" WHERE \"ID\"="));
        rs.next();
        assertTrue(rs.getString(1).startsWith("\"PUBLIC\".\"TEST\" WHERE \"ID\"="));
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT * FROM " +
                prefix + "SEARCH('1', 0, 0)");
        rs.next();
        assertEquals("\"PUBLIC\".\"TEST\" WHERE \"ID\"=1", rs.getString(1));
        assertFalse(rs.next());

        if (lucene) {
            rs = stat.executeQuery("SELECT * FROM " +
                    prefix + "SEARCH('NAME:Hallo', 0, 0)");
            rs.next();
            assertEquals("\"PUBLIC\".\"TEST\" WHERE \"ID\"=2", rs.getString(1));
            assertFalse(rs.next());
        }

        if (!config.memory) {
            conn.close();
            conn = getConnection("fullText");
        }

        stat = conn.createStatement();
        stat.executeQuery("SELECT * FROM " + prefix + "SEARCH('World', 0, 0)");

        stat.execute("CALL " + prefix + "DROP_ALL()");

        conn.close();
    }

    private void testDropIndex(boolean lucene) throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("fullTextDropIndex");
        String prefix = lucene ? "FTL" : "FT";
        FileUtils.deleteRecursive(getBaseDir() + "/fullTextDropIndex", false);
        Connection conn = getConnection("fullTextDropIndex");
        Statement stat = conn.createStatement();
        initFullText(stat, lucene);
        stat.execute("CREATE TABLE TEST" +
                "(ID INT PRIMARY KEY, NAME1 VARCHAR, NAME2 VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES" +
                "(1, 'Hello World', 'Hello Again')");
        stat.execute("CALL " + prefix +
                "_CREATE_INDEX('PUBLIC', 'TEST', 'NAME1')");
        stat.execute("UPDATE TEST SET NAME1=NULL WHERE ID=1");
        stat.execute("UPDATE TEST SET NAME1='Hello World' WHERE ID=1");
        stat.execute("CALL " + prefix +
                "_DROP_INDEX('PUBLIC', 'TEST')");
        stat.execute("CALL " + prefix +
                "_CREATE_INDEX('PUBLIC', 'TEST', 'NAME1, NAME2')");
        stat.execute("UPDATE TEST SET NAME2=NULL WHERE ID=1");
        stat.execute("UPDATE TEST SET NAME2='Hello World' WHERE ID=1");

        conn.close();
        FileUtils.deleteRecursive(getBaseDir() + "/fullTextDropIndex", false);
    }

    private static void initFullText(Statement stat, boolean lucene)
            throws SQLException {
        String prefix = lucene ? "FTL" : "FT";
        String className = lucene ? "FullTextLucene" : "FullText";
        stat.execute("CREATE ALIAS IF NOT EXISTS " + prefix +
                "_INIT FOR \"org.h2.fulltext." + className + ".init\"");
        stat.execute("CALL " + prefix + "_INIT()");
    }
}
