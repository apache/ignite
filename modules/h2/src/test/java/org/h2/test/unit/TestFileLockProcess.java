/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import org.h2.test.TestBase;
import org.h2.test.utils.SelfDestructor;
import org.h2.util.New;

/**
 * Tests database file locking.
 * A new process is started.
 */
public class TestFileLockProcess extends TestBase {

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        SelfDestructor.startCountdown(60);
        if (args.length == 0) {
            TestBase.createCaller().init().test();
            return;
        }
        String url = args[0];
        execute(url);
    }

    private static void execute(String url) {
        org.h2.Driver.load();
        try {
            Class.forName("org.h2.Driver");
            Connection conn = DriverManager.getConnection(url);
            System.out.println("!");
            conn.close();
        } catch (Exception e) {
            // failed - expected
        }
    }

    @Override
    public void test() throws Exception {
        if (config.codeCoverage || config.networked) {
            return;
        }
        if (getBaseDir().indexOf(':') > 0) {
            return;
        }
        deleteDb("lock");
        String url = "jdbc:h2:"+getBaseDir()+"/lock";

        println("socket");
        test(4, url + ";file_lock=socket");

        println("fs");
        test(4, url + ";file_lock=fs");

        println("default");
        test(50, url);

        deleteDb("lock");
    }

    private void test(int count, String url) throws Exception {
        url = getURL(url, true);
        Connection conn = getConnection(url);
        String selfDestruct = SelfDestructor.getPropertyString(60);
        String[] procDef = { "java", selfDestruct,
                "-cp", getClassPath(),
                getClass().getName(), url };
        ArrayList<Process> processes = New.arrayList();
        for (int i = 0; i < count; i++) {
            Thread.sleep(100);
            if (i % 10 == 0) {
                println(i + "/" + count);
            }
            Process proc = Runtime.getRuntime().exec(procDef);
            processes.add(proc);
        }
        for (int i = 0; i < count; i++) {
            Process proc = processes.get(i);
            StringBuilder buff = new StringBuilder();
            while (true) {
                int ch = proc.getErrorStream().read();
                if (ch < 0) {
                    break;
                }
                System.out.print((char) ch);
                buff.append((char) ch);
            }
            while (true) {
                int ch = proc.getInputStream().read();
                if (ch < 0) {
                    break;
                }
                System.out.print((char) ch);
                buff.append((char) ch);
            }
            proc.waitFor();

            // The travis build somehow generates messages like this from javac.
            // No idea where it is coming from.
            String processOutput = buff.toString();
            processOutput = processOutput.replaceAll("Picked up _JAVA_OPTIONS: -Xmx2048m -Xms512m", "").trim();

            assertEquals(0, proc.exitValue());
            assertTrue(i + ": " + buff.toString(), processOutput.isEmpty());
        }
        Thread.sleep(100);
        conn.close();
    }

}
