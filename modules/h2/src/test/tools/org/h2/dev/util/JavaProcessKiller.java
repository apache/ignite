/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.util;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * Allows to kill a certain Java process.
 */
public class JavaProcessKiller {

    /**
     * Kill a certain Java process. The JDK (jps) needs to be in the path.
     *
     * @param args the Java process name as listed by jps -l. If not set the
     *            Java processes are listed
     */

    public static void main(String... args) {
        new JavaProcessKiller().run(args);
    }

    private void run(String... args) {
        TreeMap<Integer, String> map = getProcesses();
        System.out.println("Processes:");
        System.out.println(map);
        if (args.length == 0) {
            System.out.println("Kill a Java process");
            System.out.println("Usage: java " + getClass().getName() + " <name>");
            return;
        }
        String processName = args[0];
        int killCount = 0;
        for (Entry<Integer, String> e : map.entrySet()) {
            String name = e.getValue();
            if (name.equals(processName)) {
                int pid = e.getKey();
                System.out.println("Killing pid " + pid + "...");
                // Windows
                try {
                    exec("taskkill", "/pid", "" + pid, "/f");
                } catch (Exception e2) {
                    // ignore
                }
                // Unix
                try {
                    exec("kill", "-9", "" + pid);
                } catch (Exception e2) {
                    // ignore
                }
                System.out.println("done.");
                killCount++;
            }
        }
        if (killCount == 0) {
            System.out.println("Process " + processName + " not found");
        }
        map = getProcesses();
        System.out.println("Processes now:");
        System.out.println(map);
    }

    private static TreeMap<Integer, String> getProcesses() {
        String processList = exec("jps", "-l");
        String[] processes = processList.split("\n");
        TreeMap<Integer, String> map = new TreeMap<>();
        for (int i = 0; i < processes.length; i++) {
            String p = processes[i].trim();
            int idx = p.indexOf(' ');
            if (idx > 0) {
                int pid = Integer.parseInt(p.substring(0, idx));
                String n = p.substring(idx + 1);
                map.put(pid, n);
            }
        }
        return map;
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
            String output = new String(out.toByteArray(), StandardCharsets.UTF_8);
            return output;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void copyInThread(final InputStream in, final OutputStream out) {
        new Thread("Stream copy") {
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

}
