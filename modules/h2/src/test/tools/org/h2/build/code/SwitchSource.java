/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.code;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

/**
 * Switched source code to a specific Java version, automatically to the current
 * version, or enable / disable other blocks of source code in Java files.
 */
public class SwitchSource {

    private final ArrayList<String> enable = new ArrayList<>();
    private final ArrayList<String> disable = new ArrayList<>();

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws IOException {
        new SwitchSource().run(args);
    }

    private void run(String... args) throws IOException {
        String dir = null;
        String version = null;
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if ("-dir".equals(a)) {
                dir = args[++i];
            } else if ("-auto".equals(a)) {
                enable.add("AWT");
                version = System.getProperty("java.specification.version");
            } else if ("-version".equals(a)) {
                version = args[++i];
            } else if (a.startsWith("-")) {
                String x = a.substring(1);
                disable.add(x);
                enable.remove(x);
            } else if (a.startsWith("+")) {
                String x = a.substring(1);
                enable.add(x);
                disable.remove(x);
            } else {
                showUsage();
                return;
            }
        }
        if (version == null) {
            // ok
        } else if ("1.5".equals(version)) {
            disable.add("Java 1.6");
            disable.add("Java 1.7");
        } else if ("1.6".equals(version)) {
            enable.add("Java 1.6");
            disable.add("Java 1.7");
        } else if (version.compareTo("1.7") >= 0) {
            enable.add("Java 1.6");
            enable.add("Java 1.7");
        } else {
            throw new IllegalArgumentException("version: " + version);
        }
        if (dir == null) {
            showUsage();
        } else {
            process(new File(dir));
        }
    }

    private void showUsage() {
        System.out.println("Switched source code to a specific Java version.");
        System.out.println("java "+getClass().getName() + "\n" +
            " -dir <dir>  The target directory\n" +
            " [-version]   Use the specified Java version (1.4 or newer)\n" +
            " [-auto]      Auto-detect Java version (1.4 or newer)\n" +
            " [+MODE]     Enable code labeled MODE\n" +
            " [-MODE]     Disable code labeled MODE");
    }

    private void process(File f) throws IOException {
        String name = f.getName();
        if (name.startsWith(".svn")) {
            return;
        } else if (name.endsWith(".java")) {
            processFile(f);
        } else if (f.isDirectory()) {
            for (File file : f.listFiles()) {
                process(file);
            }
        }
    }

    private void processFile(File f) throws IOException {
        byte[] buffer;
        try (RandomAccessFile read = new RandomAccessFile(f, "r")) {
            long len = read.length();
            if (len >= Integer.MAX_VALUE) {
                throw new IOException("Files bigger than Integer.MAX_VALUE are not supported");
            }
            buffer = new byte[(int) len];
            read.readFully(buffer);
        }
        boolean found = false;
        // check for ## without creating a string
        for (int i = 0; i < buffer.length - 1; i++) {
            if (buffer[i] == '#' && buffer[i + 1] == '#') {
                found = true;
                break;
            }
        }
        if (!found) {
            return;
        }
        String source = new String(buffer);
        String target = source;
        for (String x : enable) {
            target = replaceAll(target, "/*## " + x + " ##", "//## " + x + " ##");
        }
        for (String x : disable) {
            target = replaceAll(target, "//## " + x + " ##", "/*## " + x + " ##");
        }
        if (!source.equals(target)) {
            String name = f.getPath();
            File fileNew = new File(name + ".new");
            FileWriter write = new FileWriter(fileNew);
            write.write(target);
            write.close();
            File fileBack = new File(name + ".bak");
            fileBack.delete();
            f.renameTo(fileBack);
            File fileCopy = new File(name);
            if (!fileNew.renameTo(fileCopy)) {
                throw new IOException("Could not rename "
                        + fileNew.getAbsolutePath() + " to " + name);
            }
            if (!fileBack.delete()) {
                throw new IOException("Could not delete " + fileBack.getAbsolutePath());
            }
            // System.out.println(name);
        }
    }

    private static String replaceAll(String s, String before, String after) {
        int index = 0;
        while (true) {
            int next = s.indexOf(before, index);
            if (next < 0) {
                return s;
            }
            s = s.substring(0, next) + after + s.substring(next + before.length());
            index = next + after.length();
        }
    }

}
