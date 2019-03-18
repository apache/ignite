/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import org.h2.util.IOUtils;

/**
 * Catches the output of another process.
 */
public class OutputCatcher extends Thread {
    private final InputStream in;
    private final LinkedList<String> list = new LinkedList<>();

    public OutputCatcher(InputStream in) {
        this.in = in;
    }

    /**
     * Read a line from the output.
     *
     * @param wait the maximum number of milliseconds to wait
     * @return the line
     */
    public String readLine(long wait) {
        long start = System.nanoTime();
        while (true) {
            synchronized (list) {
                if (list.size() > 0) {
                    return list.removeFirst();
                }
                try {
                    list.wait(wait);
                } catch (InterruptedException e) {
                    // ignore
                }
                long time = System.nanoTime() - start;
                if (time >= TimeUnit.MILLISECONDS.toNanos(wait)) {
                    return null;
                }
            }
        }
    }

    @Override
    public void run() {
        final StringBuilder buff = new StringBuilder();
        try {
            while (true) {
                try {
                    int x = in.read();
                    if (x < 0) {
                        break;
                    }
                    if (x < ' ') {
                        if (buff.length() > 0) {
                            String s = buff.toString();
                            buff.setLength(0);
                            synchronized (list) {
                                list.add(s);
                                list.notifyAll();
                            }
                        }
                    } else {
                        buff.append((char) x);
                    }
                } catch (IOException e) {
                    break;
                }
            }
            IOUtils.closeSilently(in);
        } finally {
            // just in case something goes wrong, make sure we store any partial output we got
            if (buff.length() > 0) {
                synchronized (list) {
                    list.add(buff.toString());
                    list.notifyAll();
                }
            }
        }
    }
}
