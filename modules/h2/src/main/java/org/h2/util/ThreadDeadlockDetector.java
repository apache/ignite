/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import org.h2.engine.SysProperties;
import org.h2.mvstore.db.MVTable;

/**
 * Detects deadlocks between threads. Prints out data in the same format as the
 * CTRL-BREAK handler, but includes information about table locks.
 */
public class ThreadDeadlockDetector {

    private static final String INDENT = "    ";

    private static ThreadDeadlockDetector detector;

    private final ThreadMXBean threadBean;

    // a daemon thread
    private final Timer threadCheck = new Timer("ThreadDeadlockDetector", true);

    private ThreadDeadlockDetector() {
        this.threadBean = ManagementFactory.getThreadMXBean();
        // delay: 10 ms
        // period: 10000 ms (100 seconds)
        threadCheck.schedule(new TimerTask() {
            @Override
            public void run() {
                checkForDeadlocks();
            }
        }, 10, 10_000);
    }

    /**
     * Initialize the detector.
     */
    public static synchronized void init() {
        if (detector == null) {
            detector = new ThreadDeadlockDetector();
        }
    }

    /**
     * Checks if any threads are deadlocked. If any, print the thread dump
     * information.
     */
    void checkForDeadlocks() {
        long[] deadlockedThreadIds = threadBean.findDeadlockedThreads();
        if (deadlockedThreadIds == null) {
            return;
        }
        dumpThreadsAndLocks("ThreadDeadlockDetector - deadlock found :",
                threadBean, deadlockedThreadIds, System.out);
    }

    /**
     * Dump all deadlocks (if any).
     *
     * @param msg the message
     */
    public static void dumpAllThreadsAndLocks(String msg) {
        dumpAllThreadsAndLocks(msg, System.out);
    }

    /**
     * Dump all deadlocks (if any).
     *
     * @param msg the message
     * @param out the output
     */
    public static void dumpAllThreadsAndLocks(String msg, PrintStream out) {
        final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        final long[] allThreadIds = threadBean.getAllThreadIds();
        dumpThreadsAndLocks(msg, threadBean, allThreadIds, out);
    }

    private static void dumpThreadsAndLocks(String msg, ThreadMXBean threadBean,
            long[] threadIds, PrintStream out) {
        final StringWriter stringWriter = new StringWriter();
        final PrintWriter print = new PrintWriter(stringWriter);

        print.println(msg);

        final HashMap<Long, String> tableWaitingForLockMap;
        final HashMap<Long, ArrayList<String>> tableExclusiveLocksMap;
        final HashMap<Long, ArrayList<String>> tableSharedLocksMap;
        if (SysProperties.THREAD_DEADLOCK_DETECTOR) {
            tableWaitingForLockMap = MVTable.WAITING_FOR_LOCK
                    .getSnapshotOfAllThreads();
            tableExclusiveLocksMap = MVTable.EXCLUSIVE_LOCKS
                    .getSnapshotOfAllThreads();
            tableSharedLocksMap = MVTable.SHARED_LOCKS
                    .getSnapshotOfAllThreads();
        } else {
            tableWaitingForLockMap = new HashMap<>();
            tableExclusiveLocksMap = new HashMap<>();
            tableSharedLocksMap = new HashMap<>();
        }

        final ThreadInfo[] infos = threadBean.getThreadInfo(threadIds, true,
                true);
        for (ThreadInfo ti : infos) {
            printThreadInfo(print, ti);
            printLockInfo(print, ti.getLockedSynchronizers(),
                    tableWaitingForLockMap.get(ti.getThreadId()),
                    tableExclusiveLocksMap.get(ti.getThreadId()),
                    tableSharedLocksMap.get(ti.getThreadId()));
        }

        print.flush();
        // Dump it to system.out in one block, so it doesn't get mixed up with
        // other stuff when we're using a logging subsystem.
        out.println(stringWriter.getBuffer());
        out.flush();
    }

    private static void printThreadInfo(PrintWriter print, ThreadInfo ti) {
        // print thread information
        printThread(print, ti);

        // print stack trace with locks
        StackTraceElement[] stackTrace = ti.getStackTrace();
        MonitorInfo[] monitors = ti.getLockedMonitors();
        for (int i = 0; i < stackTrace.length; i++) {
            StackTraceElement e = stackTrace[i];
            print.println(INDENT + "at " + e.toString());
            for (MonitorInfo mi : monitors) {
                if (mi.getLockedStackDepth() == i) {
                    print.println(INDENT + "  - locked " + mi);
                }
            }
        }
        print.println();
    }

    private static void printThread(PrintWriter print, ThreadInfo ti) {
        print.print("\"" + ti.getThreadName() + "\"" + " Id="
                + ti.getThreadId() + " in " + ti.getThreadState());
        if (ti.getLockName() != null) {
            print.append(" on lock=").append(ti.getLockName());
        }
        if (ti.isSuspended()) {
            print.append(" (suspended)");
        }
        if (ti.isInNative()) {
            print.append(" (running in native)");
        }
        print.println();
        if (ti.getLockOwnerName() != null) {
            print.println(INDENT + " owned by " + ti.getLockOwnerName() + " Id="
                    + ti.getLockOwnerId());
        }
    }

    private static void printLockInfo(PrintWriter print, LockInfo[] locks,
            String tableWaitingForLock,
            ArrayList<String> tableExclusiveLocks,
            ArrayList<String> tableSharedLocksMap) {
        print.println(INDENT + "Locked synchronizers: count = " + locks.length);
        for (LockInfo li : locks) {
            print.println(INDENT + "  - " + li);
        }
        if (tableWaitingForLock != null) {
            print.println(INDENT + "Waiting for table: " + tableWaitingForLock);
        }
        if (tableExclusiveLocks != null) {
            print.println(INDENT + "Exclusive table locks: count = " + tableExclusiveLocks.size());
            for (String name : tableExclusiveLocks) {
                print.println(INDENT + "  - " + name);
            }
        }
        if (tableSharedLocksMap != null) {
            print.println(INDENT + "Shared table locks: count = " + tableSharedLocksMap.size());
            for (String name : tableSharedLocksMap) {
                print.println(INDENT + "  - " + name);
            }
        }
        print.println();
    }

}