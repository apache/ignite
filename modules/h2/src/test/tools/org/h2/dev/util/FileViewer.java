/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.h2.message.DbException;
import org.h2.util.Tool;

/**
 * A text file viewer that support very large files.
 */
public class FileViewer extends Tool {

    /**
     * Run the viewer.
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws SQLException {
        new FileViewer().runTool(args);
    }

    @Override
    protected void showUsage() {
        out.println("A text file viewer that support very large files.");
        out.println("java "+getClass().getName() + "\n" +
                " -file <file>     The name of the file to view\n" +
                " [-find <text>]   Find a string and display the next lines\n" +
                " [-start <x>]     Start at the given position\n" +
                " [-head]          Display the first lines\n" +
                " [-tail]          Display the last lines\n" +
                " [-lines <x>]     Display only x lines (default: 30)\n" +
                " [-quiet]         Do not print progress information");
    }

    @Override
    public void runTool(String... args) throws SQLException {
        String file = null;
        String find = null;
        boolean head = false, tail = false;
        int lines = 30;
        boolean quiet = false;
        long start = 0;
        for (int i = 0; args != null && i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("-file")) {
                file = args[++i];
            } else if (arg.equals("-find")) {
                find = args[++i];
            } else if (arg.equals("-start")) {
                start = Long.decode(args[++i]).longValue();
            } else if (arg.equals("-head")) {
                head = true;
            } else if (arg.equals("-tail")) {
                tail = true;
            } else if (arg.equals("-lines")) {
                lines = Integer.decode(args[++i]).intValue();
            } else if (arg.equals("-quiet")) {
                quiet = true;
            } else if (arg.equals("-help") || arg.equals("-?")) {
                showUsage();
                return;
            } else {
                showUsageAndThrowUnsupportedOption(arg);
            }
        }
        if (file == null) {
            showUsage();
            return;
        }
        if (!head && !tail && find == null) {
            head = true;
        }
        try {
            process(file, find, head, tail, start, lines, quiet);
        } catch (IOException e) {
            throw DbException.toSQLException(e);
        }
    }

    private static void process(String fileName, String find,
            boolean head, boolean tail, long start, int lines,
            boolean quiet) throws IOException {
        RandomAccessFile file = new RandomAccessFile(fileName, "r");
        long length = file.length();
        if (head) {
            file.seek(start);
            list(start, "Head", readLines(file, lines));
        }
        if (find != null) {
            file.seek(start);
            long pos = find(file, find.getBytes(), quiet);
            if (pos >= 0) {
                file.seek(pos);
                list(pos, "Found " + find, readLines(file, lines));
            }
        }
        if (tail) {
            long pos = length - 100L * lines;
            ArrayList<String> list = null;
            while (pos > 0) {
                file.seek(pos);
                list = readLines(file, Integer.MAX_VALUE);
                if (list.size() > lines) {
                    break;
                }
                pos -= 100L * lines;
            }
            // remove the first (maybe partial) line
            list.remove(0);
            while (list.size() > lines) {
                list.remove(0);
            }
            list(pos, "Tail", list);
        }
    }

    private static long find(RandomAccessFile file, byte[] find, boolean quiet)
            throws IOException {
        long pos = file.getFilePointer();
        long length = file.length();
        int bufferSize = 4 * 1024;
        byte[] data = new byte[bufferSize * 2];
        long last = System.nanoTime();
        while (pos < length) {
            System.arraycopy(data, bufferSize, data, 0, bufferSize);
            if (pos + bufferSize > length) {
                file.readFully(data, bufferSize, (int) (length - pos));
                return find(data, find, (int) (bufferSize + length - pos - find.length));
            }
            if (!quiet) {
                long now = System.nanoTime();
                if (now > last + TimeUnit.SECONDS.toNanos(5)) {
                    System.out.println((100 * pos / length) + "%");
                    last = now;
                }
            }
            file.readFully(data, bufferSize, bufferSize);
            int f = find(data, find, bufferSize);
            if (f >= 0) {
                return f + pos - bufferSize;
            }
            pos += bufferSize;
        }
        return -1;
    }

    private static int find(byte[] data, byte[] find, int max) {
        outer:
        for (int i = 0; i < max; i++) {
            for (int j = 0; j < find.length; j++) {
                if (data[i + j] != find[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    private static void list(long pos, String header, ArrayList<String> list) {
        System.out.println("-----------------------------------------------");
        System.out.println("[" + pos + "]: " + header);
        System.out.println("-----------------------------------------------");
        for (String l : list) {
            System.out.println(l);
        }
        System.out.println("-----------------------------------------------");
    }

    private static ArrayList<String> readLines(RandomAccessFile file,
            int maxLines) throws IOException {
        ArrayList<String> lines = new ArrayList<>();
        ByteArrayOutputStream buff = new ByteArrayOutputStream(100);
        boolean lastNewline = false;
        while (maxLines > 0) {
            int x = file.read();
            if (x < 0) {
                break;
            }
            if (x == '\r' || x == '\n') {
                if (!lastNewline) {
                    maxLines--;
                    lastNewline = true;
                    byte[] data = buff.toByteArray();
                    String s = new String(data);
                    lines.add(s);
                    buff.reset();
                }
                continue;
            }
            if (lastNewline) {
                lastNewline = false;
            }
            buff.write(x);
        }
        byte[] data = buff.toByteArray();
        if (data.length > 0) {
            String s = new String(data);
            lines.add(s);
        }
        return lines;
    }

}
