/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.ArrayList;
import java.util.regex.Pattern;

/**
 * A tool that removes uninteresting lines from stack traces.
 */
public class ThreadDumpCleaner {

    private static final String[] PATTERN = {
        "\"Concurrent Mark-Sweep GC Thread\".*\n",

        "\"Exception Catcher Thread\".*\n",

        "JNI global references:.*\n\n",

        "\".*?\".*?\n   java.lang.Thread.State:.*\n\n",

        "\".*?\".*\n\n",

        "\\$\\$YJP\\$\\$",

        "\"(Attach|Service|VM|GC|DestroyJavaVM|Signal|AWT|AppKit|C2 |Low Mem|" +
                "process reaper|YJPAgent-).*?\"(?s).*?\n\n",

        "   Locked ownable synchronizers:(?s).*?\n\n",

        "\".*?\".*?\n   java.lang.Thread.State: (TIMED_)?WAITING(?s).*?\n\n",

        "\".*?\".*?\n   java.lang.Thread.State:.*\n\t" +
                "at sun.nio.ch.KQueueArrayWrapper.kevent0(?s).*?\n\n",

        "\".*?\".*?\n   java.lang.Thread.State:.*\n\t" +
                "at java.io.FileInputStream.readBytes(?s).*?\n\n",

        "\".*?\".*?\n   java.lang.Thread.State:.*\n\t" +
                "at sun.nio.ch.ServerSocketChannelImpl.accept(?s).*?\n\n",

        "\".*?\".*?\n   java.lang.Thread.State:.*\n\t" +
                "at java.net.DualStackPlainSocketImpl.accept0(?s).*\n\n",

        "\".*?\".*?\n   java.lang.Thread.State:.*\n\t" +
                "at sun.nio.ch.EPollArrayWrapper.epollWait(?s).*?\n\n",

        "\".*?\".*?\n   java.lang.Thread.State:.*\n\t" +
                "at java.lang.Object.wait(?s).*?\n\n",

        "\".*?\".*?\n   java.lang.Thread.State:.*\n\t" +
                "at java.net.PlainSocketImpl.socketAccept(?s).*?\n\n",

        "\".*?\".*?\n   java.lang.Thread.State:.*\n\t" +
                "at java.net.SocketInputStream.socketRead0(?s).*?\n\n",

        "\".*?\".*?\n   java.lang.Thread.State:.*\n\t" +
                "at sun.nio.ch.WindowsSelectorImpl\\$SubSelector.poll0(?s).*?\n\n",

    };

    private final ArrayList<Pattern> patterns = new ArrayList<>();

    {
        for (String s : PATTERN) {
            patterns.add(Pattern.compile(s));
        }
    }

    /**
     * Run the tool.
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws IOException {
        String inFile = null, outFile = null;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-in")) {
                inFile = args[++i];
            } else if (args[i].equals("-out")) {
                outFile = args[++i];
            }
        }
        if (args.length == 0) {
            outFile = "-";
        }
        if (outFile == null) {
            outFile = inFile + ".clean.txt";
        }
        PrintWriter writer;
        if ("-".equals(outFile)) {
            writer = new PrintWriter(System.out);
        } else {
            writer = new PrintWriter(new BufferedWriter(new FileWriter(outFile)));
        }
        Reader r;
        if (inFile != null) {
            r = new FileReader(inFile);
        } else {
            r = new InputStreamReader(System.in);
        }
        new ThreadDumpCleaner().run(
                new LineNumberReader(new BufferedReader(r)),
                writer);
        writer.close();
        r.close();
    }

    private void run(LineNumberReader reader, PrintWriter writer) throws IOException {
        StringBuilder buff = new StringBuilder();
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }
            buff.append(line).append('\n');
            if (line.trim().length() == 0) {
                writer.print(filter(buff.toString()));
                buff = new StringBuilder();
            }
        }
        writer.println(filter(buff.toString()));
    }

    private String filter(String s) {
        for (Pattern p : patterns) {
            s = p.matcher(s).replaceAll("");
        }
        return s;
    }

}
