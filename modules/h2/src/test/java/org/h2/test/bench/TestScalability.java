/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.bench;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.util.IOUtils;
import org.h2.util.JdbcUtils;

/**
 * Used to compare scalability between the old engine and the new MVStore
 * engine. Mostly it runs BenchB with various numbers of threads.
 */
public class TestScalability implements Database.DatabaseTest {

    /**
     * Whether data should be collected.
     */
    boolean collect;

    /**
     * The flag used to enable or disable trace messages.
     */
    boolean trace;

    /**
     * This method is called when executing this sample application.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        new TestScalability().test();
    }

    private static Connection getResultConnection() throws SQLException {
        org.h2.Driver.load();
        return DriverManager.getConnection("jdbc:h2:./data/results");
    }

    private static void openResults() throws SQLException {
        Connection conn = null;
        Statement stat = null;
        try {
            conn = getResultConnection();
            stat = conn.createStatement();
            stat.execute(
                    "CREATE TABLE IF NOT EXISTS RESULTS(TESTID INT, " +
                    "TEST VARCHAR, UNIT VARCHAR, DBID INT, " +
                    "DB VARCHAR, TCNT INT, RESULT VARCHAR)");
        } finally {
            JdbcUtils.closeSilently(stat);
            JdbcUtils.closeSilently(conn);
        }
    }

    private void test() throws Exception {
        FileUtils.deleteRecursive("data", true);
        final String out = "benchmark.html";
        final int size = 400;

        ArrayList<Database> dbs = new ArrayList<>();
        int id = 1;
        final String h2Url = "jdbc:h2:./data/test;" +
                "LOCK_TIMEOUT=10000;MV_STORE=FALSE;LOCK_MODE=3";
        dbs.add(createDbEntry(id++, "H2", 1, h2Url));
        dbs.add(createDbEntry(id++, "H2", 2, h2Url));
        dbs.add(createDbEntry(id++, "H2", 4, h2Url));
        dbs.add(createDbEntry(id++, "H2", 8, h2Url));
        dbs.add(createDbEntry(id++, "H2", 16, h2Url));
        dbs.add(createDbEntry(id++, "H2", 32, h2Url));
        dbs.add(createDbEntry(id++, "H2", 64, h2Url));

        final String mvUrl = "jdbc:h2:./data/mvTest;" +
                "LOCK_TIMEOUT=10000;MULTI_THREADED=1";
        dbs.add(createDbEntry(id++, "MV", 1, mvUrl));
        dbs.add(createDbEntry(id++, "MV", 2, mvUrl));
        dbs.add(createDbEntry(id++, "MV", 4, mvUrl));
        dbs.add(createDbEntry(id++, "MV", 8, mvUrl));
        dbs.add(createDbEntry(id++, "MV", 16, mvUrl));
        dbs.add(createDbEntry(id++, "MV", 32, mvUrl));
        dbs.add(createDbEntry(id++, "MV", 64, mvUrl));

        final BenchB test = new BenchB() {
            // Since we focus on scalability here, lets emphasize multi-threaded
            // part of the test (transactions) and minimize impact of the init.
            @Override
            protected int getTransactionsPerClient(int size) {
                return size * 8;
            }
        };
        testAll(dbs, test, size);
        collect = false;

        ArrayList<Object[]> results = dbs.get(0).getResults();
        Connection conn = null;
        PreparedStatement prep = null;
        Statement stat = null;
        PrintWriter writer = null;
        try {
            openResults();
            conn = getResultConnection();
            stat = conn.createStatement();
            prep = conn.prepareStatement(
                    "INSERT INTO RESULTS(TESTID, " +
                    "TEST, UNIT, DBID, DB, TCNT, RESULT) VALUES(?, ?, ?, ?, ?, ?, ?)");
            for (int i = 0; i < results.size(); i++) {
                Object[] res = results.get(i);
                prep.setInt(1, i);
                prep.setString(2, res[0].toString());
                prep.setString(3, res[1].toString());
                for (Database db : dbs) {
                    prep.setInt(4, db.getId());
                    prep.setString(5, db.getName());
                    prep.setInt(6, db.getThreadsCount());
                    Object[] v = db.getResults().get(i);
                    prep.setString(7, v[2].toString());
                    prep.execute();
                }
            }

            writer = new PrintWriter(new FileWriter(out));
            ResultSet rs = stat.executeQuery(
                "CALL '<table border=\"1\"><tr><th rowspan=\"2\">Test Case</th>" +
                "<th rowspan=\"2\">Unit</th>' " +
                "|| (SELECT GROUP_CONCAT('<th colspan=\"' || COLSPAN || '\">' || TCNT || '</th>' " +
                "ORDER BY TCNT SEPARATOR '') FROM " +
                "(SELECT TCNT, COUNT(*) COLSPAN FROM (SELECT DISTINCT DB, TCNT FROM RESULTS) GROUP BY TCNT))" +
                "|| '</tr>' || CHAR(10) " +
                "|| '<tr>' || (SELECT GROUP_CONCAT('<th>' || DB || '</th>' ORDER BY TCNT, DB SEPARATOR '')" +
                " FROM (SELECT DISTINCT DB, TCNT FROM RESULTS)) || '</tr>' || CHAR(10) " +
                "|| (SELECT GROUP_CONCAT('<tr><td>' || TEST || '</td><td>' || UNIT || '</td>' || ( " +
                "SELECT GROUP_CONCAT('<td>' || RESULT || '</td>' ORDER BY TCNT,DB SEPARATOR '')" +
                " FROM RESULTS R2 WHERE R2.TESTID = R1.TESTID) || '</tr>' " +
                "ORDER BY TESTID SEPARATOR CHAR(10)) FROM " +
                "(SELECT DISTINCT TESTID, TEST, UNIT FROM RESULTS) R1)" +
                "|| '</table>'");
            rs.next();
            String result = rs.getString(1);
            writer.println(result);
        } finally {
            JdbcUtils.closeSilently(prep);
            JdbcUtils.closeSilently(stat);
            JdbcUtils.closeSilently(conn);
            IOUtils.closeSilently(writer);
        }
    }

    private Database createDbEntry(int id, String namePrefix,
            int threadCount, String url) {
        Database db = Database.parse(this, id, namePrefix +
                ", org.h2.Driver, " + url + ", sa, sa", threadCount);
        return db;
    }


    private void testAll(ArrayList<Database> dbs, BenchB test, int size)
            throws Exception {
        for (int i = 0; i < dbs.size(); i++) {
            if (i > 0) {
                Thread.sleep(1000);
            }
            // calls garbage collection
            TestBase.getMemoryUsed();
            Database db = dbs.get(i);
            System.out.println("Testing the performance of " + db.getName()
                    + " (" + db.getThreadsCount() + " threads)");
            db.startServer();
            Connection conn = db.openNewConnection();
            DatabaseMetaData meta = conn.getMetaData();
            System.out.println(" " + meta.getDatabaseProductName() + " " +
                    meta.getDatabaseProductVersion());
            runDatabase(db, test, 1);
            runDatabase(db, test, 1);
            collect = true;
            runDatabase(db, test, size);
            conn.close();
            db.log("Executed statements", "#", db.getExecutedStatements());
            db.log("Total time", "ms", db.getTotalTime());
            int statPerSec = (int) (db.getExecutedStatements() *
                    1000L / db.getTotalTime());
            db.log("Statements per second", "#", statPerSec);
            System.out.println("Statements per second: " + statPerSec);
            System.out.println("GC overhead: " + (100 * db.getTotalGCTime() / db.getTotalTime()) + "%");
            collect = false;
            db.stopServer();
        }
    }

    private static void runDatabase(Database db, BenchB bench, int size)
            throws Exception {
        bench.init(db, size);
        bench.setThreadCount(db.getThreadsCount());
        bench.runTest();
    }

    /**
     * Print a message to system out if trace is enabled.
     *
     * @param s the message
     */
    @Override
    public void trace(String s) {
        if (trace) {
            System.out.println(s);
        }
    }

    @Override
    public boolean isCollect() {
        return collect;
    }
}
