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

package org.apache.ignite.internal.igfs.common;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jsr166.ConcurrentHashMap8;
import org.jsr166.ConcurrentLinkedDeque8;

/**
 * IGFS client logger writing data to the file.
 */
public final class IgfsLogger {
    /** Field delimiter. */
    public static final String DELIM_FIELD = ";";

    /** Field values delimiter. */
    public static final String DELIM_FIELD_VAL = ",";

    /** Pre-defined header string. */
    public static final String HDR = "Timestamp" + DELIM_FIELD + "ThreadID" + DELIM_FIELD + "PID" + DELIM_FIELD +
        "Type" + DELIM_FIELD + "Path" + DELIM_FIELD + "Mode" + DELIM_FIELD + "StreamId" + DELIM_FIELD + "BufSize" +
        DELIM_FIELD + "DataLen" + DELIM_FIELD + "Append" + DELIM_FIELD + "Overwrite" + DELIM_FIELD + "Replication" +
        DELIM_FIELD + "BlockSize" + DELIM_FIELD + "Position" + DELIM_FIELD + "ReadLen" + DELIM_FIELD + "SkipCnt" +
        DELIM_FIELD + "ReadLimit" + DELIM_FIELD + "UserTime" + DELIM_FIELD + "SystemTime" + DELIM_FIELD +
        "TotalBytes" + DELIM_FIELD + "DestPath" + DELIM_FIELD + "Recursive" + DELIM_FIELD + "List";

    /** File open. */
    public static final int TYPE_OPEN_IN = 0;

    /** File create or append. */
    public static final int TYPE_OPEN_OUT = 1;

    /** Random read. */
    public static final int TYPE_RANDOM_READ = 2;

    /** Seek. */
    public static final int TYPE_SEEK = 3;

    /** Skip. */
    public static final int TYPE_SKIP = 4;

    /** Mark. */
    public static final int TYPE_MARK = 5;

    /** Reset. */
    public static final int TYPE_RESET = 6;

    /** Close input stream. */
    public static final int TYPE_CLOSE_IN = 7;

    /** Close output stream. */
    public static final int TYPE_CLOSE_OUT = 8;

    /** Directory creation. */
    public static final int TYPE_DIR_MAKE = 9;

    /** Directory listing. */
    public static final int TYPE_DIR_LIST = 10;

    /** Rename. */
    public static final int TYPE_RENAME = 11;

    /** Delete. */
    public static final int TYPE_DELETE = 12;

    /** Counter for stream identifiers. */
    private static final AtomicLong CNTR = new AtomicLong();

    /** Loggers. */
    private static final ConcurrentHashMap8<String, IgfsLogger> loggers =
        new ConcurrentHashMap8<>();

    /** Lock for atomic logger adds/removals. */
    private static final ReadWriteLock logLock = new ReentrantReadWriteLock();

    /** Predefined disabled logger. */
    private static final IgfsLogger disabledLogger = new IgfsLogger();

    /** Logger enabled flag. */
    private boolean enabled;

    /** Endpoint. */
    private String endpoint;

    /** Batch size. */
    private int batchSize;

    /** File to which data is to be written. */
    private File file;

    /** Read/write lock for concurrent entries collection modification. */
    private ReadWriteLock rwLock;

    /** Flush lock. */
    private Lock flushLock;

    /** Flush condition. */
    private Condition flushCond;

    /** Logged data flusher. */
    private Thread flushWorker;

    /** Process ID. */
    private int pid;

    /** Entries. */
    private Collection<Entry> entries;

    /** Entries counter in order to avoid concurrent collection size checks. */
    private AtomicInteger cnt;

    /** Logger usage counter. */
    private AtomicInteger useCnt;

    /**
     * Get next stream ID.
     *
     * @return Stream ID.
     */
    public static long nextId() {
        return CNTR.incrementAndGet();
    }

    /**
     * Get disabled logger.
     *
     * @return Disable logger instance.
     */
    public static IgfsLogger disabledLogger() {
        return disabledLogger;
    }

    /**
     * Get logger instance for the given endpoint.
     *
     * @param endpoint Endpoint.
     * @param dir Path.
     * @param batchSize Batch size.
     *
     * @return Logger instance.
     */
    public static IgfsLogger logger(String endpoint, String igfsName, String dir, int batchSize) {
        if (endpoint == null)
            endpoint = "";

        logLock.readLock().lock();

        try {
            IgfsLogger log = loggers.get(endpoint);

            if (log == null) {
                log = new IgfsLogger(endpoint, igfsName, dir, batchSize);

                IgfsLogger log0 = loggers.putIfAbsent(endpoint, log);

                if (log0 != null)
                    log = log0;
            }

            log.useCnt.incrementAndGet();

            return log;
        }
        finally {
            logLock.readLock().unlock();
        }
    }

    /**
     * Construct disabled file logger.
     */
    private IgfsLogger() {
        // No-op.
    }

    /**
     * Construct normal file logger.
     *
     * @param endpoint Endpoint.
     * @param igfsName IGFS name.
     * @param dir Log file path.
     * @param batchSize Batch size.
     */
    private IgfsLogger(String endpoint, String igfsName, String dir, int batchSize) {
        A.notNull(endpoint, "endpoint cannot be null");
        A.notNull(dir, "dir cannot be null");
        A.ensure(batchSize > 0, "batch size cannot be negative");

        enabled = true;

        this.endpoint = endpoint;
        this.batchSize = batchSize;

        pid = U.jvmPid();

        File dirFile = new File(dir);

        A.ensure(dirFile.isDirectory(), "dir must point to a directory");
        A.ensure(dirFile.exists(), "dir must exist");

        file = new File(dirFile, "igfs-log-" + igfsName + "-" + pid + ".csv");

        entries = new ConcurrentLinkedDeque8<>();

        cnt = new AtomicInteger();
        useCnt = new AtomicInteger();

        rwLock = new ReentrantReadWriteLock();
        flushLock = new ReentrantLock();
        flushCond = flushLock.newCondition();

        flushWorker = new Thread(new FlushWorker());

        flushWorker.setDaemon(true);

        flushWorker.start();
    }

    /**
     * Check whether logging is enabled.
     *
     * @return {@code True} in case logging is enabled.
     */
    public boolean isLogEnabled() {
        return enabled;
    }

    /**
     * Log file open event.
     *
     * @param streamId Stream ID.
     * @param path Path.
     * @param mode Mode.
     * @param bufSize Buffer size.
     * @param dataLen Data length.
     */
    public void logOpen(long streamId, IgfsPath path, IgfsMode mode, int bufSize, long dataLen) {
        addEntry(new Entry(TYPE_OPEN_IN, path.toString(), mode, streamId, bufSize, dataLen, null, null, null, null,
            null, null, null, null, null, null, null, null, null, null));
    }

    /**
     * Log file create event.
     *
     * @param streamId Stream ID.
     * @param path Path.
     * @param mode Mode.
     * @param overwrite Overwrite flag.
     * @param bufSize Buffer size.
     * @param replication Replication factor.
     * @param blockSize Block size.
     */
    public void logCreate(long streamId, IgfsPath path, IgfsMode mode, boolean overwrite, int bufSize,
        int replication, long blockSize) {
        addEntry(new Entry(TYPE_OPEN_OUT, path.toString(), mode, streamId, bufSize, null, false, overwrite, replication,
            blockSize, null, null, null, null, null, null, null, null, null, null));
    }

    /**
     * Log file append event.
     *
     * @param streamId Stream ID.
     * @param path Path.
     * @param mode Mode.
     * @param bufSize Buffer size.
     */
    public void logAppend(long streamId, IgfsPath path, IgfsMode mode, int bufSize) {
        addEntry(new Entry(TYPE_OPEN_OUT, path.toString(), mode, streamId, bufSize, null, true, null, null, null, null,
            null, null, null, null, null, null, null, null, null));
    }

    /**
     * Log random read event.
     *
     * @param streamId Stream ID.
     * @param pos Position.
     * @param readLen Read bytes count.
     */
    public void logRandomRead(long streamId, long pos, int readLen) {
        addEntry(new Entry(TYPE_RANDOM_READ, null, null, streamId, null, null, null, null, null, null, pos, readLen,
            null, null, null, null, null, null, null, null));
    }

    /**
     * Log seek event.
     *
     * @param streamId Stream ID.
     * @param pos Position.
     */
    public void logSeek(long streamId, long pos) {
        addEntry(new Entry(TYPE_SEEK, null, null, streamId, null, null, null, null, null, null, pos, null, null, null,
            null, null, null, null, null, null));
    }

    /**
     * Log skip event.
     *
     * @param streamId Stream ID.
     * @param skipCnt Skip bytes count.
     */
    public void logSkip(long streamId, long skipCnt) {
        addEntry(new Entry(TYPE_SKIP, null, null, streamId, null, null, null, null, null, null, null, null, skipCnt,
            null, null, null, null, null, null, null));
    }

    /**
     * Log mark event.
     *
     * @param streamId Stream ID.
     * @param readLimit Read limit.
     */
    public void logMark(long streamId, long readLimit) {
        addEntry(new Entry(TYPE_MARK, null, null, streamId, null, null, null, null, null, null, null, null, null,
            readLimit, null, null, null, null, null, null));
    }

    /**
     * Log reset event.
     *
     * @param streamId Stream ID.
     */
    public void logReset(long streamId) {
        addEntry(new Entry(TYPE_RESET, null, null, streamId, null, null, null, null, null, null, null, null, null, null,
            null, null, null, null, null, null));
    }

    /**
     * Log input stream close event.
     *
     * @param streamId Stream ID.
     * @param userTime User time.
     * @param readTime Read time.
     * @param total Total bytes read.
     */
    public void logCloseIn(long streamId, long userTime, long readTime, long total) {
        addEntry(new Entry(TYPE_CLOSE_IN, null, null, streamId, null, null, null, null, null, null, null, null, null,
            null, userTime, readTime, total ,null, null, null));
    }

    /**
     * Log output stream close event.
     *
     * @param streamId Stream ID.
     * @param userTime User time.
     * @param writeTime Read time.
     * @param total Total bytes read.
     */
    public void logCloseOut(long streamId, long userTime, long writeTime, long total) {
        addEntry(new Entry(TYPE_CLOSE_OUT, null, null, streamId, null, null, null, null, null, null, null, null, null,
            null, userTime, writeTime, total, null, null, null));
    }

    /**
     * Log directory creation event.
     *
     * @param path Path.
     * @param mode Mode.
     */
    public void logMakeDirectory(IgfsPath path, IgfsMode mode) {
        addEntry(new Entry(TYPE_DIR_MAKE, path.toString(), mode, null, null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null));
    }

    /**
     * Log directory listing event.
     *
     * @param path Path.
     * @param mode Mode.
     * @param files Files.
     */
    public void logListDirectory(IgfsPath path, IgfsMode mode, String[] files) {
        addEntry(new Entry(TYPE_DIR_LIST, path.toString(), mode, null, null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, files));
    }

    /**
     * Log rename event.
     *
     * @param path Path.
     * @param mode Mode.
     * @param destPath Destination path.
     */
    public void logRename(IgfsPath path, IgfsMode mode, IgfsPath destPath) {
        addEntry(new Entry(TYPE_RENAME, path.toString(), mode, null, null, null, null, null, null, null, null, null,
            null, null, null, null, null, destPath.toString(), null, null));
    }

    /**
     * Log delete event.
     *
     * @param path Path.
     * @param mode Mode.
     * @param recursive Recursive flag.
     */
    public void logDelete(IgfsPath path, IgfsMode mode, boolean recursive) {
        addEntry(new Entry(TYPE_DELETE, path.toString(), mode, null, null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, recursive, null));
    }

    /**
     * Close logger.
     */
    public void close() {
        boolean close = false;

        if (useCnt.decrementAndGet() == 0) {
            logLock.writeLock().lock();

            try {
                if (useCnt.get() == 0) {
                    loggers.remove(endpoint);

                    close = true;
                }
            }
            finally {
                logLock.writeLock().unlock();
            }
        }

        if (close) {
            U.interrupt(flushWorker);

            try {
                U.join(flushWorker);
            }
            catch (IgniteInterruptedCheckedException ignore) {
                // No-op.
            }

            entries.clear();
        }
    }

    /**
     * Add new log entry.
     *
     * @param entry Entry.
     */
    @SuppressWarnings("SignalWithoutCorrespondingAwait")
    private void addEntry(Entry entry) {
        assert entry != null;

        rwLock.readLock().lock();

        try {
            entries.add(entry);
        }
        finally {
            rwLock.readLock().unlock();
        }

        if (cnt.incrementAndGet() >= batchSize) {
            if (flushLock.tryLock()) {
                try {
                    flushCond.signalAll();
                }
                finally {
                    flushLock.unlock();
                }
            }
        }
    }

    /**
     * Logged entry.
     */
    private class Entry {
        /** Thread ID. */
        private final long threadId;

        /** Timestamp. */
        private final long ts;

        /** Event type. */
        private final int type;

        /** File/dir path. */
        private final String path;

        /** Path mode. */
        private IgfsMode mode;

        /** Stream ID. */
        private final long streamId;

        /** Buffer size. Available only for OPEN_IN/OPEN_OUT events */
        private final int bufSize;

        /** Length of data available to read. Available only for OPEN_IN event. */
        private final long dataLen;

        /** Append flag. Available only for OPEN_OUT event. */
        private final Boolean append;

        /** Overwrite flag. Available only for OPEN_OUT event. */
        private final Boolean overwrite;

        /** Replication. Available only for OPEN_OUT event. */
        private final int replication;

        /** Block size. Available only for OPEN_OUT event. */
        private final long blockSize;

        /** Position of data being randomly read or seek. Available only for RANDOM_READ or SEEK events. */
        private final long pos;

        /** Length of data being randomly read. Available only for RANDOM_READ event. */
        private final int readLen;

        /** Amount of skipped bytes. Available only for SKIP event. */
        private final long skipCnt;

        /** Read limit. Available only for MARK event. */
        private final long readLimit;

        /** User time. Available only for CLOSE_IN/CLOSE_OUT events. */
        private final long userTime;

        /** System time (either read or write). Available only for CLOSE_IN/CLOSE_OUT events. */
        private final long sysTime;

        /** Total amount of read or written bytes. Available only for CLOSE_IN/CLOSE_OUT events.*/
        private final long total;

        /** Destination path. Available only for RENAME event. */
        private final String destPath;

        /** Recursive flag. Available only for DELETE event. */
        private final Boolean recursive;

        /** Directory listing. Available only for LIST event. */
        private final String[] list;

        /**
         * Constructor.
         *
         * @param type Event type.
         * @param path Path.
         * @param mode Path mode.
         * @param streamId Stream ID.
         * @param bufSize Buffer size.
         * @param dataLen Data length.
         * @param append Append flag.
         * @param overwrite Overwrite flag.
         * @param replication Replication.
         * @param blockSize Block size.
         * @param pos Position.
         * @param readLen Read length.
         * @param skipCnt Skip count.
         * @param readLimit Read limit.
         * @param userTime User time.
         * @param sysTime System time.
         * @param total Read or written bytes.
         * @param destPath Destination path.
         * @param recursive Recursive flag.
         * @param list Listed directories.
         */
        Entry(int type, String path, IgfsMode mode, Long streamId, Integer bufSize, Long dataLen, Boolean append,
            Boolean overwrite, Integer replication, Long blockSize, Long pos, Integer readLen, Long skipCnt,
            Long readLimit, Long userTime, Long sysTime, Long total, String destPath, Boolean recursive,
            String[] list) {
            threadId = Thread.currentThread().getId();
            ts = U.currentTimeMillis();

            this.type = type;
            this.path = path;
            this.mode = mode;
            this.streamId = streamId != null ? streamId : -1;
            this.bufSize = bufSize != null ? bufSize : -1;
            this.dataLen = dataLen != null ? dataLen : -1;
            this.append = append;
            this.overwrite = overwrite;
            this.replication = replication != null ? replication : -1;
            this.blockSize = blockSize != null ? blockSize : -1;
            this.pos = pos != null ? pos : -1;
            this.readLen = readLen != null ? readLen : -1;
            this.skipCnt = skipCnt != null ? skipCnt : -1;
            this.readLimit = readLimit != null ? readLimit : -1;
            this.userTime = userTime != null ? userTime : -1;
            this.sysTime = sysTime != null ? sysTime : -1;
            this.total = total != null ? total : -1;
            this.destPath = destPath;
            this.recursive = recursive;
            this.list = list;
        }

        /**
         * Return suitable representation of long value.
         *
         * @param val Value.
         * @return String representation.
         */
        private String string(int val) {
            return val != -1 ? String.valueOf(val) : "";
        }

        /**
         * Return suitable representation of long value.
         *
         * @param val Value.
         * @return String representation.
         */
        private String string(long val) {
            return val != -1 ? String.valueOf(val) : "";
        }

        /**
         * Return suitable representation of the object.
         *
         * @param val Object.
         * @return String representation.
         */
        private String string(Object val) {
            if (val == null)
                return "";
            else if (val instanceof Boolean)
                return ((Boolean) val) ? "1" : "0";
            else if (val instanceof String)
                return ((String)val).replace(';', '~');
            else if (val instanceof String[]) {
                String[] val0 = (String[])val;

                SB buf = new SB();

                boolean first = true;

                for (String str : val0) {
                    if (first)
                        first = false;
                    else
                        buf.a(DELIM_FIELD_VAL);

                    buf.a(str.replace(';', '~'));
                }

                return buf.toString();
            }
            else
                return val.toString();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            SB res = new SB();

            res.a(ts).a(DELIM_FIELD).a(threadId).a(DELIM_FIELD).a(pid).a(DELIM_FIELD).a(type).a(DELIM_FIELD)
                .a(string(path)).a(DELIM_FIELD).a(string(mode)).a(DELIM_FIELD).a(string(streamId)).a(DELIM_FIELD)
                .a(string(bufSize)).a(DELIM_FIELD).a(string(dataLen)).a(DELIM_FIELD).a(string(append)).a(DELIM_FIELD)
                .a(string(overwrite)).a(DELIM_FIELD).a(string(replication)).a(DELIM_FIELD).a(string(blockSize))
                .a(DELIM_FIELD).a(string(pos)).a(DELIM_FIELD).a(string(readLen)).a(DELIM_FIELD).a(string(skipCnt))
                .a(DELIM_FIELD).a(string(readLimit)).a(DELIM_FIELD).a(string(userTime)).a(DELIM_FIELD)
                .a(string(sysTime)).a(DELIM_FIELD).a(string(total)).a(DELIM_FIELD).a(string(destPath)).a(DELIM_FIELD)
                .a(string(recursive)).a(DELIM_FIELD).a(string(list));

            return res.toString();
        }
    }

    /**
     * Data flush worker.
     */
    private class FlushWorker implements Runnable {
        /** {@inheritDoc} */
        @Override public void run() {
            Thread t = Thread.currentThread();

            // We clear interrupted flag here in order to let the final flush proceed normally with IO operations.
            while (!Thread.interrupted()) {
                flushLock.lock();

                try {
                    while (cnt.get() < batchSize && !t.isInterrupted()) {
                        try {
                            U.await(flushCond, 1000L, TimeUnit.MILLISECONDS);
                        }
                        catch (IgniteInterruptedCheckedException ignore) {
                            t.interrupt();

                            break;
                        }
                    }
                }
                finally {
                    flushLock.unlock();
                }

                if (!t.isInterrupted())
                    flush();
            }

            // Flush remaining entries.
            flush();
        }

        /**
         * Flush buffered entries to disk.
         */
        @SuppressWarnings("TooBroadScope")
        private void flush() {
            Collection<Entry> entries0;

            rwLock.writeLock().lock();

            try {
                entries0 = entries;

                entries = new ConcurrentLinkedDeque8<>();
            }
            finally {
                rwLock.writeLock().unlock();
            }

            // We could lost some increments here, but this is not critical if the new batch will exceed maximum
            // size by several items.
            cnt.set(0);

            if (!entries0.isEmpty()) {
                boolean addHdr = !file.exists();

                FileOutputStream fos = null;
                OutputStreamWriter osw = null;
                BufferedWriter bw = null;

                try {
                    fos = new FileOutputStream(file, true);
                    osw = new OutputStreamWriter(fos);
                    bw = new BufferedWriter(osw);

                    if (addHdr)
                        bw.write(HDR + U.nl());

                    for (Entry entry : entries0)
                        bw.write(entry + U.nl());
                }
                catch (IOException e) {
                    U.error(null, "Failed to flush logged entries to a disk due to an IO exception.", e);
                }
                finally {
                    U.closeQuiet(bw);
                    U.closeQuiet(osw);
                    U.closeQuiet(fos);
                }
            }
        }
    }
}