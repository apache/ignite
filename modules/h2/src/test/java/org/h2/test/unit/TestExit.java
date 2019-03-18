/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.h2.api.DatabaseEventListener;
import org.h2.test.TestBase;
import org.h2.test.utils.SelfDestructor;

/**
 * Tests the flag db_close_on_exit. A new process is started.
 */
public class TestExit extends TestBase {

    private static Connection conn;

    private static final int OPEN_WITH_CLOSE_ON_EXIT = 1,
            OPEN_WITHOUT_CLOSE_ON_EXIT = 2;

    @Override
    public void test() throws Exception {
        if (config.codeCoverage || config.networked) {
            return;
        }
        if (getBaseDir().indexOf(':') > 0) {
            return;
        }
        deleteDb("exit");
        String url = getURL(OPEN_WITH_CLOSE_ON_EXIT);
        String selfDestruct = SelfDestructor.getPropertyString(60);
        String[] procDef = { "java", selfDestruct, "-cp", getClassPath(),
                getClass().getName(), url };
        Process proc = Runtime.getRuntime().exec(procDef);
        while (true) {
            int ch = proc.getErrorStream().read();
            if (ch < 0) {
                break;
            }
            System.out.print((char) ch);
        }
        while (true) {
            int ch = proc.getInputStream().read();
            if (ch < 0) {
                break;
            }
            System.out.print((char) ch);
        }
        proc.waitFor();
        Thread.sleep(100);
        if (!getClosedFile().exists()) {
            fail("did not close database");
        }
        url = getURL(OPEN_WITHOUT_CLOSE_ON_EXIT);
        procDef = new String[] { "java", "-cp", getClassPath(),
                getClass().getName(), url };
        proc = Runtime.getRuntime().exec(procDef);
        proc.waitFor();
        Thread.sleep(100);
        if (getClosedFile().exists()) {
            fail("closed database");
        }
        deleteDb("exit");
    }

    private String getURL(int action) {
        String url = "";
        switch (action) {
        case OPEN_WITH_CLOSE_ON_EXIT:
            url = "jdbc:h2:" + getBaseDir() +
                    "/exit;database_event_listener='" +
                    MyDatabaseEventListener.class.getName() +
                    "';db_close_on_exit=true";
            break;
        case OPEN_WITHOUT_CLOSE_ON_EXIT:
            url = "jdbc:h2:" + getBaseDir() +
                    "/exit;database_event_listener='" +
                    MyDatabaseEventListener.class.getName() +
                    "';db_close_on_exit=false";
            break;
        default:
        }
        url = getURL(url, true);
        return url;
    }

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws SQLException {
        SelfDestructor.startCountdown(60);
        if (args.length == 0) {
            System.exit(1);
        }
        String url = args[0];
        TestExit.execute(url);
    }

    private static void execute(String url) throws SQLException {
        org.h2.Driver.load();
        conn = open(url);
        Connection conn2 = open(url);
        conn2.close();
        // do not close
        conn.isClosed();
    }

    private static Connection open(String url) throws SQLException {
        getClosedFile().delete();
        return DriverManager.getConnection(url, "sa", "");
    }

    static File getClosedFile() {
        return new File(TestBase.BASE_TEST_DIR + "/closed.txt");
    }

    /**
     * A database event listener used in this test.
     */
    public static final class MyDatabaseEventListener implements
            DatabaseEventListener {

        @Override
        public void exceptionThrown(SQLException e, String sql) {
            // nothing to do
        }

        @Override
        public void closingDatabase() {
            try {
                getClosedFile().createNewFile();
            } catch (IOException e) {
                TestBase.logError("error", e);
            }
        }

        @Override
        public void setProgress(int state, String name, int x, int max) {
            // nothing to do
        }

        @Override
        public void init(String url) {
            // nothing to do
        }

        @Override
        public void opened() {
            // nothing to do
        }

    }

}
