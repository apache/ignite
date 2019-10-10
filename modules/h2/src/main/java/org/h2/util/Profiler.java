/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.instrument.Instrumentation;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A simple CPU profiling tool similar to java -Xrunhprof. It can be used
 * in-process (to profile the current application) or as a standalone program
 * (to profile a different process, or files containing full thread dumps).
 */
public class Profiler implements Runnable {

    private static Instrumentation instrumentation;
    private static final String LINE_SEPARATOR =
            System.getProperty("line.separator", "\n");
    private static final int MAX_ELEMENTS = 1000;

    public int interval = 2;
    public int depth = 48;
    public boolean paused;
    public boolean sumClasses;
    public boolean sumMethods;

    private int pid;

    private final String[] ignoreLines = (
            "java," +
            "sun," +
            "com.sun.," +
            "com.google.common.," +
            "com.mongodb.," +
            "org.bson.,"
            ).split(",");
    private final String[] ignorePackages = (
            "java," +
            "sun," +
            "com.sun.," +
            "com.google.common.," +
            "com.mongodb.," +
            "org.bson"
            ).split(",");
    private final String[] ignoreThreads = (
            "java.lang.Object.wait," +
            "java.lang.Thread.dumpThreads," +
            "java.lang.Thread.getThreads," +
            "java.lang.Thread.sleep," +
            "java.lang.UNIXProcess.waitForProcessExit," +
            "java.net.PlainDatagramSocketImpl.receive0," +
            "java.net.PlainSocketImpl.accept," +
            "java.net.PlainSocketImpl.socketAccept," +
            "java.net.SocketInputStream.socketRead," +
            "java.net.SocketOutputStream.socketWrite," +
            "org.eclipse.jetty.io.nio.SelectorManager$SelectSet.doSelect," +
            "sun.awt.windows.WToolkit.eventLoop," +
            "sun.misc.Unsafe.park," +
            "sun.nio.ch.EPollArrayWrapper.epollWait," +
            "sun.nio.ch.KQueueArrayWrapper.kevent0," +
            "sun.nio.ch.ServerSocketChannelImpl.accept," +
            "dalvik.system.VMStack.getThreadStackTrace," +
            "dalvik.system.NativeStart.run"
            ).split(",");

    private volatile boolean stop;
    private final HashMap<String, Integer> counts =
            new HashMap<>();

    /**
     * The summary (usually one entry per package, unless sumClasses is enabled,
     * in which case it's one entry per class).
     */
    private final HashMap<String, Integer> summary =
            new HashMap<>();
    private int minCount = 1;
    private int total;
    private Thread thread;
    private long start;
    private long time;
    private int threadDumps;

    /**
     * This method is called when the agent is installed.
     *
     * @param agentArgs the agent arguments
     * @param inst the instrumentation object
     */
    public static void premain(@SuppressWarnings("unused") String agentArgs, Instrumentation inst) {
        instrumentation = inst;
    }

    /**
     * Get the instrumentation object if started as an agent.
     *
     * @return the instrumentation, or null
     */
    public static Instrumentation getInstrumentation() {
        return instrumentation;
    }

    /**
     * Run the command line version of the profiler. The JDK (jps and jstack)
     * need to be in the path.
     *
     * @param args the process id of the process - if not set the java processes
     *        are listed
     */
    public static void main(String... args) {
        new Profiler().run(args);
    }

    private void run(String... args) {
        if (args.length == 0) {
            System.out.println("Show profiling data");
            System.out.println("Usage: java " + getClass().getName() +
                    " <pid> | <stackTraceFileNames>");
            System.out.println("Processes:");
            String processes = exec("jps", "-l");
            System.out.println(processes);
            return;
        }
        start = System.nanoTime();
        if (args[0].matches("\\d+")) {
            pid = Integer.parseInt(args[0]);
            long last = 0;
            while (true) {
                tick();
                long t = System.nanoTime();
                if (t - last > TimeUnit.SECONDS.toNanos(5)) {
                    time = System.nanoTime() - start;
                    System.out.println(getTopTraces(3));
                    last = t;
                }
            }
        }
        try {
            for (String arg : args) {
                if (arg.startsWith("-")) {
                    if ("-classes".equals(arg)) {
                        sumClasses = true;
                    } else if ("-methods".equals(arg)) {
                        sumMethods = true;
                    } else if ("-packages".equals(arg)) {
                        sumClasses = false;
                        sumMethods = false;
                    } else {
                        throw new IllegalArgumentException(arg);
                    }
                    continue;
                }
                try (Reader reader = new InputStreamReader(new FileInputStream(arg), "CP1252")) {
                    LineNumberReader r = new LineNumberReader(reader);
                    while (true) {
                        String line = r.readLine();
                        if (line == null) {
                            break;
                        } else if (line.startsWith("Full thread dump")) {
                            threadDumps++;
                        }
                    }
                }
                try (Reader reader = new InputStreamReader(new FileInputStream(arg), "CP1252")) {
                    LineNumberReader r = new LineNumberReader(reader);
                    processList(readStackTrace(r));
                }
            }
            System.out.println(getTopTraces(5));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<Object[]> getRunnableStackTraces() {
        ArrayList<Object[]> list = new ArrayList<>();
        Map<Thread, StackTraceElement[]> map = Thread.getAllStackTraces();
        for (Map.Entry<Thread, StackTraceElement[]> entry : map.entrySet()) {
            Thread t = entry.getKey();
            if (t.getState() != Thread.State.RUNNABLE) {
                continue;
            }
            StackTraceElement[] dump = entry.getValue();
            if (dump == null || dump.length == 0) {
                continue;
            }
            list.add(dump);
        }
        return list;
    }

    private static List<Object[]> readRunnableStackTraces(int pid) {
        try {
            String jstack = exec("jstack", "" + pid);
            LineNumberReader r = new LineNumberReader(
                    new StringReader(jstack));
            return readStackTrace(r);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<Object[]> readStackTrace(LineNumberReader r)
            throws IOException {
        ArrayList<Object[]> list = new ArrayList<>();
        while (true) {
            String line = r.readLine();
            if (line == null) {
                break;
            }
            if (!line.startsWith("\"")) {
                // not a thread
                continue;
            }
            line = r.readLine();
            if (line == null) {
                break;
            }
            line = line.trim();
            if (!line.startsWith("java.lang.Thread.State: RUNNABLE")) {
                continue;
            }
            ArrayList<String> stack = new ArrayList<>();
            while (true) {
                line = r.readLine();
                if (line == null) {
                    break;
                }
                line = line.trim();
                if (line.startsWith("- ")) {
                    continue;
                }
                if (!line.startsWith("at ")) {
                    break;
                }
                line = line.substring(3).trim();
                stack.add(line);
            }
            if (!stack.isEmpty()) {
                String[] s = stack.toArray(new String[0]);
                list.add(s);
            }
        }
        return list;
    }

    private static String exec(String... args) {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            Process p = Runtime.getRuntime().exec(args);
            copyInThread(p.getInputStream(), out);
            copyInThread(p.getErrorStream(), err);
            p.waitFor();
            String e = new String(err.toByteArray(), StandardCharsets.UTF_8);
            if (e.length() > 0) {
                throw new RuntimeException(e);
            }
            return new String(out.toByteArray(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void copyInThread(final InputStream in,
            final OutputStream out) {
        new Thread("Profiler stream copy") {
            @Override
            public void run() {
                byte[] buffer = new byte[4096];
                try {
                    while (true) {
                        int len = in.read(buffer, 0, buffer.length);
                        if (len < 0) {
                            break;
                        }
                        out.write(buffer, 0, len);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }.start();
    }

    /**
     * Start collecting profiling data.
     *
     * @return this
     */
    public Profiler startCollecting() {
        thread = new Thread(this, "Profiler");
        thread.setDaemon(true);
        thread.start();
        return this;
    }

    /**
     * Stop collecting.
     *
     * @return this
     */
    public Profiler stopCollecting() {
        stop = true;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
            thread = null;
        }
        return this;
    }

    @Override
    public void run() {
        start = System.nanoTime();
        while (!stop) {
            try {
                tick();
            } catch (Throwable t) {
                break;
            }
        }
        time = System.nanoTime() - start;
    }

    private void tick() {
        if (interval > 0) {
            if (paused) {
                return;
            }
            try {
                Thread.sleep(interval);
            } catch (Exception e) {
                // ignore
            }
        }

        List<Object[]> list;
        if (pid != 0) {
            list = readRunnableStackTraces(pid);
        } else {
            list = getRunnableStackTraces();
        }
        threadDumps++;
        processList(list);
    }

    private void processList(List<Object[]> list) {
        for (Object[] dump : list) {
            if (startsWithAny(dump[0].toString(), ignoreThreads)) {
                continue;
            }
            StringBuilder buff = new StringBuilder();
            // simple recursive calls are ignored
            String last = null;
            boolean packageCounts = false;
            for (int j = 0, i = 0; i < dump.length && j < depth; i++) {
                String el = dump[i].toString();
                if (!el.equals(last) && !startsWithAny(el, ignoreLines)) {
                    last = el;
                    buff.append("at ").append(el).append(LINE_SEPARATOR);
                    if (!packageCounts && !startsWithAny(el, ignorePackages)) {
                        packageCounts = true;
                        int index = 0;
                        for (; index < el.length(); index++) {
                            char c = el.charAt(index);
                            if (c == '(' || Character.isUpperCase(c)) {
                                break;
                            }
                        }
                        if (index > 0 && el.charAt(index - 1) == '.') {
                            index--;
                        }
                        if (sumClasses) {
                            int m = el.indexOf('.', index + 1);
                            index = m >= 0 ? m : index;
                        }
                        if (sumMethods) {
                            int m = el.indexOf('(', index + 1);
                            index = m >= 0 ? m : index;
                        }
                        String groupName = el.substring(0, index);
                        increment(summary, groupName, 0);
                    }
                    j++;
                }
            }
            if (buff.length() > 0) {
                minCount = increment(counts, buff.toString().trim(), minCount);
                total++;
            }
        }
    }

    private static boolean startsWithAny(String s, String[] prefixes) {
        for (String p : prefixes) {
            if (p.length() > 0 && s.startsWith(p)) {
                return true;
            }
        }
        return false;
    }

    private static int increment(HashMap<String, Integer> map, String trace,
            int minCount) {
        Integer oldCount = map.get(trace);
        if (oldCount == null) {
            map.put(trace, 1);
        } else {
            map.put(trace, oldCount + 1);
        }
        while (map.size() > MAX_ELEMENTS) {
            for (Iterator<Map.Entry<String, Integer>> ei =
                    map.entrySet().iterator(); ei.hasNext();) {
                Map.Entry<String, Integer> e = ei.next();
                if (e.getValue() <= minCount) {
                    ei.remove();
                }
            }
            if (map.size() > MAX_ELEMENTS) {
                minCount++;
            }
        }
        return minCount;
    }

    /**
     * Get the top stack traces.
     *
     * @param count the maximum number of stack traces
     * @return the stack traces.
     */
    public String getTop(int count) {
        stopCollecting();
        return getTopTraces(count);
    }

    private String getTopTraces(int count) {
        StringBuilder buff = new StringBuilder();
        buff.append("Profiler: top ").append(count).append(" stack trace(s) of ");
        if (time > 0) {
            buff.append(" of ").append(TimeUnit.NANOSECONDS.toMillis(time)).append(" ms");
        }
        if (threadDumps > 0) {
            buff.append(" of ").append(threadDumps).append(" thread dumps");
        }
        buff.append(":").append(LINE_SEPARATOR);
        if (counts.size() == 0) {
            buff.append("(none)").append(LINE_SEPARATOR);
        }
        HashMap<String, Integer> copy = new HashMap<>(counts);
        appendTop(buff, copy, count, total, false);
        buff.append("summary:").append(LINE_SEPARATOR);
        copy = new HashMap<>(summary);
        appendTop(buff, copy, count, total, true);
        buff.append('.');
        return buff.toString();
    }

    private static void appendTop(StringBuilder buff,
            HashMap<String, Integer> map, int count, int total, boolean table) {
        for (int x = 0, min = 0;;) {
            int highest = 0;
            Map.Entry<String, Integer> best = null;
            for (Map.Entry<String, Integer> el : map.entrySet()) {
                if (el.getValue() > highest) {
                    best = el;
                    highest = el.getValue();
                }
            }
            if (best == null) {
                break;
            }
            map.remove(best.getKey());
            if (++x >= count) {
                if (best.getValue() < min) {
                    break;
                }
                min = best.getValue();
            }
            int c = best.getValue();
            int percent = 100 * c / Math.max(total, 1);
            if (table) {
                if (percent > 1) {
                    buff.append(percent).
                        append("%: ").append(best.getKey()).
                        append(LINE_SEPARATOR);
                }
            } else {
                buff.append(c).append('/').append(total).append(" (").
                    append(percent).
                    append("%):").append(LINE_SEPARATOR).
                    append(best.getKey()).
                    append(LINE_SEPARATOR);
            }
        }
    }

}
