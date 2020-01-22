/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.h2.store.FileLister;
import org.h2.test.TestBase;
import org.h2.test.utils.SelfDestructor;

/**
 * Test application for TestKill.
 */
public class TestKillProcess {

    private TestKillProcess() {
        // utility class
    }

    /**
     * This method is called when executing this application.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) {
        SelfDestructor.startCountdown(60);
        try {
            Class.forName("org.h2.Driver");
            String url = args[0], user = args[1], password = args[2];
            String baseDir = args[3];
            int accounts = Integer.parseInt(args[4]);

            Random random = new Random();
            Connection conn1 = DriverManager.getConnection(
                    url, user, password);

            PreparedStatement prep1a = conn1.prepareStatement(
                    "INSERT INTO LOG(ACCOUNTID, AMOUNT) VALUES(?, ?)");
            PreparedStatement prep1b = conn1.prepareStatement(
                    "UPDATE ACCOUNT SET SUM=SUM+? WHERE ID=?");
            conn1.setAutoCommit(false);
            long time = System.nanoTime();
            String d = null;
            for (int i = 0;; i++) {
                long t = System.nanoTime();
                if (t > time + TimeUnit.SECONDS.toNanos(1)) {
                    ArrayList<String> list = FileLister.getDatabaseFiles(
                            baseDir, "kill", true);
                    System.out.println("inserting... i:" + i + " d:" + d +
                            " files:" + list.size());
                    time = t;
                }
                if (i > 10000) {
                    // System.out.println("halt");
                    // Runtime.getRuntime().halt(0);
                    // conn.createStatement().execute("SHUTDOWN IMMEDIATELY");
                    // System.exit(0);
                }
                int account = random.nextInt(accounts);
                int value = random.nextInt(100);
                prep1a.setInt(1, account);
                prep1a.setInt(2, value);
                prep1a.execute();
                prep1b.setInt(1, value);
                prep1b.setInt(2, account);
                prep1b.execute();
                conn1.commit();
                if (random.nextInt(100) < 2) {
                    d = "D" + random.nextInt(1000);
                    account = random.nextInt(accounts);
                    conn1.createStatement().execute(
                            "UPDATE TEST_A SET DATA='" + d +
                            "' WHERE ID=" + account);
                    conn1.createStatement().execute(
                            "UPDATE TEST_B SET DATA='" + d +
                            "' WHERE ID=" + account);
                }
            }
        } catch (Throwable e) {
            TestBase.logError("error", e);
        }
    }

}
