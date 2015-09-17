/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class for debugging.
 */
public class GridDebug {
    /** */
    private static final AtomicReference<ConcurrentLinkedQueue<Item>> que =
        new AtomicReference<>(new ConcurrentLinkedQueue<Item>());

    /** */
    private static final SimpleDateFormat DEBUG_DATE_FMT = new SimpleDateFormat("HH:mm:ss,SSS");

    /** */
    private static final FileOutputStream out;

    /** */
    private static final Charset charset = Charset.forName("UTF-8");

    /** */
    private static volatile long start;

    /**
     * On Ubuntu:
     * sudo mkdir /ramdisk
     * sudo mount -t tmpfs -o size=2048M tmpfs /ramdisk
     */
    private static final String LOGS_PATH = null;// "/ramdisk/";

    /** */
    private static boolean allowLog;

    /** */
    static {
        if (LOGS_PATH != null) {
            File log = new File(LOGS_PATH + new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss-").format(new Date()) +
                    ManagementFactory.getRuntimeMXBean().getName() + ".log");

            assert !log.exists();

            try {
                out = new FileOutputStream(log, false);
            }
            catch (FileNotFoundException e) {
                throw new IllegalStateException(e);
            }
        }
        else
            out = null;
    }

    /**
     * Gets collected debug items queue.
     *
     * @return Items queue.
     */
    public static ConcurrentLinkedQueue<Item> queue() {
        return que.get();
    }

    /**
     * @param allow Write log.
     */
    public static synchronized void allowWriteLog(boolean allow) {
        allowLog = allow;
    }

    /**
     * Writes to log file which should reside on ram disk.
     *
     * @param x Data to log.
     */
    public static synchronized void write(Object ... x) {
        if (!allowLog)
            return;

        Thread th = Thread.currentThread();

        try {
            out.write((formatEntry(System.currentTimeMillis(), th.getName(), th.getId(), x) + "\n").getBytes(charset));
            out.flush();
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Add the data to debug queue.
     *
     * @param x Debugging data.
     */
    public static void debug(Object ... x) {
        ConcurrentLinkedQueue<Item> q = que.get();

        if (q != null)
            q.add(new Item(x));
    }

    /**
     * Hangs for 5 minutes if stopped.
     */
    public static void hangIfStopped() {
        if (que.get() == null)
            try {
                Thread.sleep(300000);
            }
            catch (InterruptedException ignored) {
                // No-op.
            }
    }

    /**
     * Sets starting time after which {@link #timing(String)} measurements can be done.
     */
    public static void start() {
        start = U.currentTimeMillis();
    }

    /**
     * Print timing after the {@link #start()} call.
     *
     * @param label Label.
     */
    public static void timing(String label) {
        X.println(label + ' ' + (U.currentTimeMillis() - start) + " ms");
    }

    /**
     * @return Object which will dump thread stack on toString call.
     */
    public static Object dumpStack() {
        final Throwable t = new Throwable();

        return new Object() {
            @Override public String toString() {
                StringWriter errors = new StringWriter();

                t.printStackTrace(new PrintWriter(errors));

                return errors.toString();
            }
        };
    }

    /**
     * Dumps given number of last events.
     *
     * @param n Number of last elements to dump.
     */
    public static void dumpLastAndStop(int n) {
        ConcurrentLinkedQueue<Item> q = que.getAndSet(null);

        if (q == null)
            return;

        int size = q.size();

        while (size-- > n)
            q.poll();

        dump(q);
    }

    /**
     * Dump given queue to stdout.
     *
     * @param que Queue.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    public static void dump(Collection<Item> que) {
        if (que == null)
            return;

        int start = -1;// que.size() - 5000;

        int x = 0;

        for (Item i : que) {
            if (x++ > start)
                System.out.println(i);
        }
    }

    /**
     * Dump existing queue to stdout and atomically replace it with null so that no subsequent logging is possible.
     *
     * @param x Parameters.
     * @return Empty string (useful for assertions like {@code assert x == 0 : D.dumpWithStop();} ).
     */
    public static String dumpWithStop(Object... x) {
        debug(x);
        return dumpWithReset(null, null);
    }

    /**
     * Dump existing queue to stdout and atomically replace it with new queue.
     *
     * @return Empty string (useful for assertions like {@code assert x == 0 : D.dumpWithReset();} ).
     */
    public static String dumpWithReset() {
        return dumpWithReset(new ConcurrentLinkedQueue<Item>(), null);
    }

    /**
     * Dump existing queue to stdout and atomically replace it with given.
     *
     * @param q2 Queue.
     * @param filter Filter for logged debug items.
     * @return Empty string.
     */
    public static String dumpWithReset(
        @Nullable ConcurrentLinkedQueue<Item> q2,
        @Nullable IgnitePredicate<Item> filter
    ) {
        ConcurrentLinkedQueue<Item> q;

        do {
            q = que.get();

            if (q == null)
                break; // Stopped.
        }
        while (!que.compareAndSet(q, q2));

        Collection<Item> col = null;

        if (filter == null)
            col = q;
        else if (q != null) {
            col = new ArrayList<>();

            for (Item item : q) {
                if (filter.apply(item))
                    col.add(item);
            }
        }

        dump(col);

        return "";
    }

    /**
     * Reset queue to empty one.
     */
    public static void reset() {
        ConcurrentLinkedQueue<Item> old = que.get();

        if (old != null) // Was not stopped.
            que.compareAndSet(old, new ConcurrentLinkedQueue<Item>());
    }

    /**
     * Formats log entry string.
     *
     * @param ts Timestamp.
     * @param threadName Thread name.
     * @param threadId Thread ID.
     * @param data Data.
     * @return String.
     */
    private static String formatEntry(long ts, String threadName, long threadId, Object... data) {
        return "<" + DEBUG_DATE_FMT.format(new Date(ts)) + "><~DBG~><" + threadName + " id:" + threadId + "> " +
            Arrays.deepToString(data);
    }

    /**
     * Debug info queue item.
     */
    @SuppressWarnings({"PublicInnerClass", "PublicField"})
    public static class Item {
        /** */
        public final long ts = System.currentTimeMillis();

        /** */
        public final String threadName;

        /** */
        public final long threadId;

        /** */
        public final Object[] data;

        /**
         * Constructor.
         *
         * @param data Debugging data.
         */
        public Item(Object[] data) {
            this.data = data;
            Thread th = Thread.currentThread();

            threadName = th.getName();
            threadId = th.getId();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return formatEntry(ts, threadName, threadId, data);
        }
    }
}