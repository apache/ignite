/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.io.InputStream;
import java.lang.ProcessBuilder.Redirect;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;
import org.h2.api.ErrorCode;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.test.utils.SelfDestructor;
import org.h2.tools.Backup;
import org.h2.util.New;

/**
 * Standalone recovery test. A new process is started and then killed while it
 * executes random statements using multiple connection.
 */
public class TestKillRestartMulti extends TestBase {

    /**
     * We want self-destruct to occur before the read times out and we kill the
     * child process.
     */
    private static final int CHILD_READ_TIMEOUT_MS = 7 * 60 * 1000; // 7 minutes
    private static final int CHILD_SELFDESTRUCT_TIMEOUT_MINS = 5;

    private String driver = "org.h2.Driver";
    private String url;
    private String user = "sa";
    private String password = "sa";
    private final ArrayList<Connection> connections = New.arrayList();
    private final ArrayList<String> tables = New.arrayList();
    private int openCount;


    /**
     * This method is called when executing this application from the command
     * line.
     *
     * Note that this entry can be used in two different ways, either
     * (a) running just this test
     * (b) or when this test invokes itself in a child process
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        if (args != null && args.length > 0) {
            // the child process case
            SelfDestructor.startCountdown(CHILD_SELFDESTRUCT_TIMEOUT_MINS);
            new TestKillRestartMulti().test(args);
        }
        else
        {
            // the standalone test case
            TestBase.createCaller().init().test();
        }
    }

    @Override
    public void test() throws Exception {
        if (config.networked) {
            return;
        }
        if (getBaseDir().indexOf(':') > 0) {
            return;
        }
        deleteDb("killRestartMulti");
        url = getURL("killRestartMulti", true);
        user = getUser();
        password = getPassword();
        String selfDestruct = SelfDestructor.getPropertyString(60);
        // Inherit error so that the stacktraces reported from SelfDestructor
        // show up in our log.
        ProcessBuilder pb = new ProcessBuilder().redirectError(Redirect.INHERIT)
                .command("java", selfDestruct, "-cp", getClassPath(),
                        getClass().getName(), "-url", url, "-user", user,
                        "-password", password);
        deleteDb("killRestartMulti");
        int len = getSize(3, 10);
        Random random = new Random();
        for (int i = 0; i < len; i++) {
            Process p = pb.start();
            InputStream in = p.getInputStream();
            OutputCatcher catcher = new OutputCatcher(in);
            catcher.start();
            while (true) {
                String s = catcher.readLine(CHILD_READ_TIMEOUT_MS);
                // System.out.println("> " + s);
                if (s == null) {
                    fail("No reply from process");
                } else if (!s.startsWith("#")) {
                    // System.out.println(s);
                    fail("Expected: #..., got: " + s);
                } else if (s.startsWith("#Running")) {
                    int sleep = 10 + random.nextInt(100);
                    Thread.sleep(sleep);
                    printTime("killing: " + i);
                    p.destroy();
                    printTime("killing, waiting for: " + i);
                    p.waitFor();
                    printTime("killing, dead: " + i);
                    break;
                } else if (s.startsWith("#Info")) {
                    // System.out.println("info: " + s);
                } else if (s.startsWith("#Fail")) {
                    System.err.println(s);
                    while (true) {
                        String a = catcher.readLine(CHILD_READ_TIMEOUT_MS);
                        if (a == null || "#End".endsWith(a)) {
                            break;
                        }
                        System.err.println("   " + a);
                    }
                    fail("Failed: " + s);
                }
            }
            String backup = getBaseDir() + "/killRestartMulti-" +
                    System.currentTimeMillis() + ".zip";
            try {
                Backup.execute(backup, getBaseDir(), "killRestartMulti", true);
                Connection conn = null;
                for (int j = 0;; j++) {
                    try {
                        conn = openConnection();
                        break;
                    } catch (SQLException e2) {
                        if (e2.getErrorCode() == ErrorCode.DATABASE_ALREADY_OPEN_1
                                && j < 3) {
                            Thread.sleep(100);
                        } else {
                            throw e2;
                        }
                    }
                }
                testConsistent(conn);
                Statement stat = conn.createStatement();
                stat.execute("DROP ALL OBJECTS");
                conn.close();
                conn = openConnection();
                conn.close();
                FileUtils.delete(backup);
            } catch (SQLException e) {
                FileUtils.move(backup, backup + ".error");
                throw e;
            }
        }
        deleteDb("killRestartMulti");
    }

    private void test(String... args) {
        for (int i = 0; i < args.length; i++) {
            if ("-url".equals(args[i])) {
                url = args[++i];
            } else if ("-driver".equals(args[i])) {
                driver = args[++i];
            } else if ("-user".equals(args[i])) {
                user = args[++i];
            } else if ("-password".equals(args[i])) {
                password = args[++i];
            }
        }
        System.out.println("#Started; driver: " + driver + " url: " + url +
                " user: " + user + " password: " + password);
        try {
            System.out.println("#Starting...");
            Random random = new Random();
            boolean wasRunning = false;
            for (int i = 0; i < 3000; i++) {
                if (i > 1000 && connections.size() > 1 && tables.size() > 1) {
                    System.out.println("#Running connections: " +
                            connections.size() + " tables: " + tables.size());
                    wasRunning = true;
                }
                if (connections.size() < 1) {
                    openConnection();
                }
                if (tables.size() < 1) {
                    createTable(random);
                }
                int p = random.nextInt(100);
                if ((p -= 2) <= 0) {
                    // 2%: open new connection
                    if (connections.size() < 5) {
                        openConnection();
                    }
                } else if ((p -= 1) <= 0) {
                    // 1%: close connection
                    if (connections.size() > 1) {
                        Connection conn = connections.remove(
                                random.nextInt(connections.size()));
                        if (random.nextBoolean()) {
                            conn.close();
                        }
                    }
                } else if ((p -= 10) <= 0) {
                    // 10% create table
                    createTable(random);
                } else if ((p -= 20) <= 0) {
                    // 20% large insert, delete, or update
                    if (tables.size() > 0) {
                        Connection conn = connections.get(
                                random.nextInt(connections.size()));
                        Statement stat = conn.createStatement();
                        String table = tables.get(random.nextInt(tables.size()));
                        if (random.nextBoolean()) {
                            // 10% insert
                            stat.execute("INSERT INTO " + table +
                                    "(NAME) SELECT 'Hello ' || X FROM SYSTEM_RANGE(0, 20)");
                        } else if (random.nextBoolean()) {
                            // 5% update
                            stat.execute("UPDATE " + table + " SET NAME='Hallo Welt'");
                        } else {
                            // 5% delete
                            stat.execute("DELETE FROM " + table);
                        }
                    }
                } else if ((p -= 5) < 0) {
                    // 5% truncate or drop table
                    if (tables.size() > 0) {
                        Connection conn = connections.get(random.nextInt(connections.size()));
                        Statement stat = conn.createStatement();
                        String table = tables.get(random.nextInt(tables.size()));
                        if (random.nextBoolean()) {
                            stat.execute("TRUNCATE TABLE " + table);
                        } else {
                            stat.execute("DROP TABLE " + table);
                            System.out.println("#Info table dropped: " + table);
                            tables.remove(table);
                        }
                    }
                } else if ((p -= 30) <= 0) {
                    // 30% insert
                    if (tables.size() > 0) {
                        Connection conn = connections.get(random.nextInt(connections.size()));
                        Statement stat = conn.createStatement();
                        String table = tables.get(random.nextInt(tables.size()));
                        stat.execute("INSERT INTO " + table + "(NAME) VALUES('Hello World')");
                    }
                } else {
                    // 32% delete
                    if (tables.size() > 0) {
                        Connection conn = connections.get(random.nextInt(connections.size()));
                        Statement stat = conn.createStatement();
                        String table = tables.get(random.nextInt(tables.size()));
                        stat.execute("DELETE FROM " + table +
                                " WHERE ID = SELECT MIN(ID) FROM " + table);
                    }
                }
            }
            System.out.println("#Fail: end " + wasRunning);
            System.out.println("#End");
        } catch (Throwable e) {
            System.out.println("#Fail: openCount=" +
                    openCount + " url=" + url + " " + e.toString());
            e.printStackTrace(System.out);
            System.out.println("#End");
        }
    }

    private Connection openConnection() throws Exception {
        Class.forName(driver);
        openCount++;
        Connection conn = DriverManager.getConnection(url, user, password);
        connections.add(conn);
        return conn;
    }

    private void createTable(Random random) throws SQLException {
        Connection conn = connections.get(random.nextInt(connections.size()));
        Statement stat = conn.createStatement();
        String table = "TEST" + random.nextInt(10);
        try {
            stat.execute("CREATE TABLE " + table + "(ID IDENTITY, NAME VARCHAR)");
            System.out.println("#Info table created: " + table);
            tables.add(table);
        } catch (SQLException e) {
            if (e.getErrorCode() == ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1) {
                System.out.println("#Info table already exists: " + table);
                if (!tables.contains(table)) {
                    tables.add(table);
                }
                // ok
            } else {
                throw e;
            }
        }
    }

    private static void testConsistent(Connection conn) throws SQLException {
        for (int i = 0; i < 20; i++) {
            Statement stat = conn.createStatement();
            try {
                ResultSet rs = stat.executeQuery("SELECT * FROM TEST" + i);
                while (rs.next()) {
                    rs.getLong("ID");
                    rs.getString("NAME");
                }
                rs = stat.executeQuery("SELECT * FROM TEST" + i + " ORDER BY ID");
                while (rs.next()) {
                    rs.getLong("ID");
                    rs.getString("NAME");
                }
            } catch (SQLException e) {
                if (e.getErrorCode() == ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1) {
                    // ok
                } else {
                    throw e;
                }
            }
        }
    }

}
