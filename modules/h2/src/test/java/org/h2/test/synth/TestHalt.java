/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import org.h2.test.TestAll;
import org.h2.test.TestBase;
import org.h2.test.utils.SelfDestructor;
import org.h2.tools.Backup;
import org.h2.tools.DeleteDbFiles;
import org.h2.util.StringUtils;

/**
 * Tests database recovery by destroying a process that writes to the database.
 */
public abstract class TestHalt extends TestBase {

    /**
     * This bit flag means insert operations should be performed.
     */
    protected static final int OP_INSERT = 1;

    /**
     * This bit flag means delete operations should be performed.
     */
    protected static final int OP_DELETE = 2;

    /**
     * This bit flag means update operations should be performed.
     */
    protected static final int OP_UPDATE = 4;

    /**
     * This bit flag means select operations should be performed.
     */
    protected static final int OP_SELECT = 8;

    /**
     * This bit flag means operations should be written to the transaction log
     * immediately.
     */
    protected static final int FLAG_NO_DELAY = 1;

    /**
     * This bit flag means the test should use LOB values.
     */
    protected static final int FLAG_LOBS = 2;

    private static final String DATABASE_NAME = "halt";
    private static final String TRACE_FILE_NAME = "haltTrace.trace.db";

    /**
     * The current operations bit mask.
     */
    protected int operations;

    /**
     * The current flags bit mask.
     */
    protected int flags;

    /**
     * The current test value, for example the number of rows.
     */
    protected int value;

    /**
     * The database connection.
     */
    protected Connection conn;

    /**
     * The pseudo random number generator used for this test.
     */
    protected Random random = new Random();

    private final SimpleDateFormat dateFormat =
            new SimpleDateFormat("MM-dd HH:mm:ss ");
    private int errorId;
    private int sequenceId;

    /**
     * Initialize the test.
     */
    abstract void controllerInit() throws SQLException;

    /**
     * Check if the database is consistent after a simulated database crash.
     */
    abstract void controllerCheckAfterCrash() throws SQLException;

    /**
     * Wait for some time after the application has been started.
     */
    abstract void controllerWaitAfterAppStart() throws Exception;

    /**
     * Start the application.
     */
    abstract void processAppStart() throws SQLException;

    /**
     * Run the application.
     */
    abstract void processAppRun() throws SQLException;

    @Override
    public void test() {
        for (int i = 0;; i++) {
            operations = OP_INSERT | i;
            flags = i >> 4;
            // flags |= FLAG_NO_DELAY; // | FLAG_LOBS;
            try {
                controllerTest();
            } catch (Throwable t) {
                System.out.println("Error: " + t);
                t.printStackTrace();
            }
        }
    }

    Connection getConnection() throws SQLException {
        org.h2.Driver.load();
        String url = "jdbc:h2:" + getBaseDir() + "/halt";
        // String url = "jdbc:h2:" + baseDir + "/halt;TRACE_LEVEL_FILE=3";
        return DriverManager.getConnection(url, "sa", "sa");
    }

    /**
     * The second process starts the application and executes random operations.
     */
    void processRunRandom() throws SQLException {
        connect();
        try {
            traceOperation("connected, operations:" +
                    operations + " flags:" + flags + " value:" + value);
            processAppStart();
            System.out.println("READY");
            System.out.println("READY");
            System.out.println("READY");
            processAppRun();
            traceOperation("done");
        } catch (Exception e) {
            traceOperation("run", e);
        }
        disconnect();
    }

    private void connect() throws SQLException {
        try {
            traceOperation("connecting");
            conn = getConnection();
        } catch (SQLException e) {
            traceOperation("connect", e);
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * Print a trace message to the trace file.
     *
     * @param s the message
     */
    protected void traceOperation(String s) {
        traceOperation(s, null);
    }

    /**
     * Print a trace message to the trace file.
     *
     * @param s the message
     * @param e the exception or null
     */
    protected void traceOperation(String s, Exception e) {
        File f = new File(getBaseDir() + "/" + TRACE_FILE_NAME);
        f.getParentFile().mkdirs();
        try (FileWriter writer = new FileWriter(f, true)) {
            PrintWriter w = new PrintWriter(writer);
            s = dateFormat.format(new Date()) + ": " + s;
            w.println(s);
            if (e != null) {
                e.printStackTrace(w);
            }
        } catch (IOException e2) {
            e2.printStackTrace();
        }
    }

    /**
     * Run one test. The controller starts the process, waits, kills the
     * process, and checks if everything is ok.
     */
    void controllerTest() throws Exception {
        traceOperation("delete database -----------------------------");
        DeleteDbFiles.execute(getBaseDir(), DATABASE_NAME, true);
        new File(getBaseDir() + "/" + TRACE_FILE_NAME).delete();

        connect();
        controllerInit();
        disconnect();
        for (int i = 0; i < 10; i++) {
            traceOperation("backing up " + sequenceId);
            Backup.execute(getBaseDir() + "/haltSeq" +
                    sequenceId + ".zip", getBaseDir(), null, true);
            sequenceId++;
            // int operations = OP_INSERT;
            // OP_DELETE = 1, OP_UPDATE = 2, OP_SELECT = 4;
            // int flags = FLAG_NODELAY;
            // FLAG_NO_DELAY = 1, FLAG_AUTO_COMMIT = 2, FLAG_SMALL_CACHE = 4;
            int testValue = random.nextInt(1000);
            // for Derby and HSQLDB
            // String classPath = "-cp
            // .;D:/data/java/hsqldb.jar;D:/data/java/derby.jar";
            String selfDestruct = SelfDestructor.getPropertyString(60);
            String[] procDef = { "java", selfDestruct,
                    "-cp", getClassPath(),
                    getClass().getName(), "" + operations, "" + flags, "" + testValue};
            traceOperation("start: " + StringUtils.arrayCombine(procDef, ' '));
            Process p = Runtime.getRuntime().exec(procDef);
            InputStream in = p.getInputStream();
            OutputCatcher catcher = new OutputCatcher(in);
            catcher.start();
            String s = catcher.readLine(5 * 60 * 1000);
            if (s == null) {
                throw new IOException(
                        "No reply from process, command: " +
                        StringUtils.arrayCombine(procDef, ' '));
            } else if (s.startsWith("READY")) {
                traceOperation("got reply: " + s);
            }
            controllerWaitAfterAppStart();
            p.destroy();
            p.waitFor();
            try {
                traceOperation("backing up " + sequenceId);
                Backup.execute(getBaseDir() + "/haltSeq" +
                        sequenceId + ".zip", getBaseDir(), null, true);
                // new File(BASE_DIR + "/haltSeq" + (sequenceId-20) +
                // ".zip").delete();
                connect();
                controllerCheckAfterCrash();
            } catch (Exception e) {
                File zip = new File(getBaseDir() + "/haltSeq" +
                        sequenceId + ".zip");
                File zipId = new File(getBaseDir() + "/haltSeq" +
                        sequenceId + "-" + errorId + ".zip");
                zip.renameTo(zipId);
                printTime("ERROR: " + sequenceId + " " + errorId + " " + e.toString());
                e.printStackTrace();
                errorId++;
            } finally {
                sequenceId++;
                disconnect();
            }
        }
    }

    /**
     * Close the database connection normally.
     */
    protected void disconnect() {
        try {
            traceOperation("disconnect");
            conn.close();
        } catch (Exception e) {
            traceOperation("disconnect", e);
        }
    }

//    public Connection getConnectionHSQLDB() throws SQLException {
//        File lock = new File("test.lck");
//        while (lock.exists()) {
//            lock.delete();
//            System.gc();
//        }
//        Class.forName("org.hsqldb.jdbcDriver");
//        return DriverManager.getConnection("jdbc:hsqldb:test", "sa", "");
//    }

//    public Connection getConnectionDerby() throws SQLException {
//        File lock = new File("test3/db.lck");
//        while (lock.exists()) {
//            lock.delete();
//            System.gc();
//        }
//        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
//        try {
//            return DriverManager.getConnection(
//                    "jdbc:derby:test3;create=true", "sa", "sa");
//        } catch (SQLException e) {
//            Exception e2 = e;
//            do {
//                e.printStackTrace();
//                e = e.getNextException();
//            } while (e != null);
//            throw e2;
//        }
//    }

//    void disconnectHSQLDB() {
//        try {
//            conn.createStatement().execute("SHUTDOWN");
//        } catch (Exception e) {
//            // ignore
//        }
//        // super.disconnect();
//    }

//    void disconnectDerby() {
//        // super.disconnect();
//        try {
//            Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
//            DriverManager.getConnection(
//                    "jdbc:derby:;shutdown=true", "sa", "sa");
//        } catch (Exception e) {
//            // ignore
//        }
//    }

    /**
     * Create a random string with the specified length.
     *
     * @param len the number of characters
     * @return the random string
     */
    protected String getRandomString(int len) {
        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < len; i++) {
            buff.append('a' + random.nextInt(20));
        }
        return buff.toString();
    }

    @Override
    public TestBase init(TestAll conf) throws Exception {
        super.init(conf);
        return this;
    }

}
