/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.h2.test.TestBase;
import org.h2.test.utils.SelfDestructor;

/**
 * Standalone recovery test. A new process is started and then killed while it
 * executes random statements.
 */
public class TestKillRestart extends TestBase {

    @Override
    public void test() throws Exception {
        if (config.networked) {
            return;
        }
        if (getBaseDir().indexOf(':') > 0) {
            return;
        }
        deleteDb("killRestart");
        String url = getURL("killRestart", true);
        // String url = getURL(
        //        "killRestart;CACHE_SIZE=2048;WRITE_DELAY=0", true);
        String user = getUser(), password = getPassword();
        String selfDestruct = SelfDestructor.getPropertyString(60);
        String[] procDef = { "java", selfDestruct,
                "-cp", getClassPath(),
                getClass().getName(), "-url", url, "-user", user,
                "-password", password };

        int len = getSize(2, 15);
        for (int i = 0; i < len; i++) {
            Process p = new ProcessBuilder().redirectErrorStream(true).command(procDef).start();
            InputStream in = p.getInputStream();
            OutputCatcher catcher = new OutputCatcher(in);
            catcher.start();
            while (true) {
                String s = catcher.readLine(60 * 1000);
                // System.out.println("> " + s);
                if (s == null) {
                    fail("No reply from process");
                } else if (!s.startsWith("#")) {
                    // System.out.println(s);
                    fail("Expected: #..., got: " + s);
                } else if (s.startsWith("#Running")) {
                    Thread.sleep(100);
                    printTime("killing: " + i);
                    p.destroy();
                    waitForTimeout(p);
                    break;
                } else if (s.startsWith("#Fail")) {
                    fail("Failed: " + s);
                }
            }
        }
        deleteDb("killRestart");
    }

    /**
     * Wait for a subprocess with timeout.
     */
    private static void waitForTimeout(final Process p)
            throws InterruptedException, IOException {
        final long pid = getPidOfProcess(p);
        if (pid == -1) {
            p.waitFor();
        }
        // when we hit Java8 we can use the waitFor(1,TimeUnit.MINUTES) method
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread("waitForTimeout") {
            @Override
            public void run() {
                try {
                    p.waitFor();
                    latch.countDown();
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }.start();
        if (!latch.await(2, TimeUnit.MINUTES)) {
            String[] procDef = { "jstack", "-F", "-m", "-l", "" + pid };
            new ProcessBuilder().redirectErrorStream(true).command(procDef)
                    .start();
            OutputCatcher catcher = new OutputCatcher(p.getInputStream());
            catcher.start();
            Thread.sleep(500);
            throw new IOException("timed out waiting for subprocess to die");
        }
    }

    /**
     * Get the PID of a subprocess. Only works on Linux and OSX.
     */
    private static long getPidOfProcess(Process p) {
        // When we hit Java9 we can call getPid() on Process.
        long pid = -1;
        try {
            if (p.getClass().getName().equals("java.lang.UNIXProcess")) {
                Field f = p.getClass().getDeclaredField("pid");
                f.setAccessible(true);
                pid = f.getLong(p);
                f.setAccessible(false);
            }
        } catch (Exception e) {
            pid = -1;
        }
        return pid;
    }

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) {
        SelfDestructor.startCountdown(60);
        String driver = "org.h2.Driver";
        String url = "jdbc:h2:mem:test", user = "sa", password = "sa";
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
            Class.forName(driver);
            System.out.println("#Opening...");
            Connection conn = DriverManager.getConnection(url, user, password);
            Statement stat = conn.createStatement();
            stat.execute("CREATE TABLE IF NOT EXISTS TEST" +
                    "(ID IDENTITY, NAME VARCHAR)");
            stat.execute("CREATE TABLE IF NOT EXISTS TEST2" +
                    "(ID IDENTITY, NAME VARCHAR)");
            ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
            while (rs.next()) {
                rs.getLong("ID");
                rs.getString("NAME");
            }
            rs = stat.executeQuery("SELECT * FROM TEST2");
            while (rs.next()) {
                rs.getLong("ID");
                rs.getString("NAME");
            }
            stat.execute("DROP ALL OBJECTS DELETE FILES");
            System.out.println("#Closing with delete...");
            conn.close();
            System.out.println("#Starting...");
            conn = DriverManager.getConnection(url, user, password);
            stat = conn.createStatement();
            stat.execute("DROP ALL OBJECTS");
            stat.execute("CREATE TABLE TEST(ID IDENTITY, NAME VARCHAR)");
            stat.execute("CREATE TABLE TEST2(ID IDENTITY, NAME VARCHAR)");
            stat.execute("CREATE TABLE TEST_META(ID INT)");
            PreparedStatement prep = conn.prepareStatement(
                    "INSERT INTO TEST(NAME) VALUES(?)");
            PreparedStatement prep2 = conn.prepareStatement(
                    "INSERT INTO TEST2(NAME) VALUES(?)");
            Random r = new Random(0);
//            Runnable stopper = new Runnable() {
//                public void run() {
//                    try {
//                        Thread.sleep(500);
//                    } catch (InterruptedException e) {
//                    }
//                    System.out.println("#Halt...");
//                    Runtime.getRuntime().halt(0);
//                }
//            };
//            new Thread(stopper).start();
            for (int i = 0; i < 2000; i++) {
                if (i == 100) {
                    System.out.println("#Running...");
                }
                if (r.nextInt(100) < 10) {
                    conn.createStatement().execute(
                            "ALTER TABLE TEST_META " +
                            "ALTER COLUMN ID INT DEFAULT 10");
                }
                if (r.nextBoolean()) {
                    if (r.nextBoolean()) {
                        prep.setString(1, new String(new char[r.nextInt(30) * 10]));
                        prep.execute();
                    } else {
                        prep2.setString(1, new String(new char[r.nextInt(30) * 10]));
                        prep2.execute();
                    }
                } else {
                    if (r.nextBoolean()) {
                        conn.createStatement().execute(
                                "UPDATE TEST SET NAME = NULL");
                    } else {
                        conn.createStatement().execute(
                                "UPDATE TEST2 SET NAME = NULL");
                    }
                }
            }
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            System.out.println("#Fail: " + e.toString());
        }
    }

}
