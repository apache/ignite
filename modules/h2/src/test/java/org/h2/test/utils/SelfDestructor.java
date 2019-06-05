/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.utils;

import java.sql.Timestamp;
import org.h2.util.ThreadDeadlockDetector;

/**
 * This is a self-destructor class to kill a long running process automatically
 * after a pre-defined time. The class reads the number of minutes from the
 * system property 'h2.selfDestruct' and starts a countdown thread to kill the
 * virtual machine if it still runs then.
 */
public class SelfDestructor {
    private static final String PROPERTY_NAME = "h2.selfDestruct";

    /**
     * Start the countdown. If the self-destruct system property is set, this
     * value is used, otherwise the given default value is used.
     *
     * @param defaultMinutes the default number of minutes after which the
     *            current process is killed.
     */
    public static void startCountdown(int defaultMinutes) {
        final int minutes = Integer.parseInt(
                System.getProperty(PROPERTY_NAME, "" + defaultMinutes));
        if (minutes == 0) {
            return;
        }
        Thread thread = new Thread() {
            @Override
            public void run() {
                for (int i = minutes; i >= 0; i--) {
                    while (true) {
                        try {
                            String name = "SelfDestructor " + i + " min";
                            setName(name);
                            break;
                        } catch (OutOfMemoryError e) {
                            // ignore
                        }
                    }
                    try {
                        Thread.sleep(60 * 1000);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
                try {
                    String time = new Timestamp(
                            System.currentTimeMillis()).toString();
                    System.out.println(time + " Killing the process after " +
                            minutes + " minute(s)");
                    try {
                        ThreadDeadlockDetector.dumpAllThreadsAndLocks(
                                "SelfDestructor timed out", System.err);
                        try {
                            Thread.sleep(1000);
                        } catch (Exception e) {
                            // ignore
                        }
                        int activeCount = Thread.activeCount();
                        Thread[] threads = new Thread[activeCount + 100];
                        int len = Thread.enumerate(threads);
                        for (int i = 0; i < len; i++) {
                            Thread t = threads[i];
                            if (t != Thread.currentThread()) {
                                t.interrupt();
                            }
                        }
                    } catch (Throwable t) {
                        t.printStackTrace();
                        // ignore
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        // ignore
                    }
                    System.out.println("Killing the process now");
                } catch (Throwable t) {
                    try {
                        t.printStackTrace(System.out);
                    } catch (Throwable t2) {
                        // ignore (out of memory)
                    }
                }
                Runtime.getRuntime().halt(1);
            }
        };
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Get the string to be added when starting the Java process.
     *
     * @param minutes the countdown time in minutes
     * @return the setting
     */
    public static String getPropertyString(int minutes) {
        return "-D" + PROPERTY_NAME + "=" + minutes;
    }

}
