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

package org.apache.ignite.internal.processors.performancestatistics;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.jetbrains.annotations.Nullable;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.nativeOrder;
import static java.nio.file.Files.walkFileTree;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_START;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CHECKPOINT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.JOB;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.PAGES_WRITE_THROTTLE;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY_PROPERTY;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY_READS;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY_ROWS;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.SYSTEM_VIEW_ROW;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.SYSTEM_VIEW_SCHEMA;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TASK;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TX_COMMIT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.VERSION;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.cacheOperation;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.cacheRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.checkpointRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.jobRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.pagesWriteThrottleRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.queryReadsRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.readCacheStartRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.readQueryPropertyRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.readQueryRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.readQueryRowsRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.readTaskRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.transactionOperation;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.transactionRecordSize;

/**
 * Walker over the performance statistics file.
 *
 * @see FilePerformanceStatisticsWriter
 */
public class FilePerformanceStatisticsReader {
    /** Default file read buffer size. */
    private static final int DFLT_READ_BUFFER_SIZE = (int)(8 * U.MB);

    /** Uuid as string pattern. */
    private static final String UUID_STR_PATTERN =
        "[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}";

    /** File name pattern. */
    private static final Pattern FILE_NODE_ID_PATTERN = Pattern.compile("^node-(" + UUID_STR_PATTERN + ")[^.]*\\.prf$");

    /** No-op handler. */
    private static final PerformanceStatisticsHandler[] NOOP_HANDLER = {};

    /** IO factory. */
    private final RandomAccessFileIOFactory ioFactory = new RandomAccessFileIOFactory();

    /** Current file I/O. */
    private FileIO fileIo;

    /** Buffer. */
    private final ByteBuffer buf;

    /** Handlers to process deserialized operations. */
    private final PerformanceStatisticsHandler[] handlers;

    /** Current handlers. */
    private PerformanceStatisticsHandler[] curHnd;

    /** Cached strings by hashcodes. */
    private final Map<Integer, String> knownStrs = new HashMap<>();

    /** Forward read mode. */
    private ForwardRead forwardRead;

    /** Reads system view records. */
    private SystemViewEntry sysViewEntry;

    /** @param handlers Handlers to process deserialized operations. */
    public FilePerformanceStatisticsReader(PerformanceStatisticsHandler... handlers) {
        this(DFLT_READ_BUFFER_SIZE, handlers);
    }

    /**
     * @param bufSize Buffer size.
     * @param handlers Handlers to process deserialized operations.
     */
    FilePerformanceStatisticsReader(int bufSize, PerformanceStatisticsHandler... handlers) {
        A.notEmpty(handlers, "At least one handler expected.");

        buf = allocateDirect(bufSize).order(nativeOrder());
        this.handlers = handlers;
        curHnd = handlers;
    }

    /**
     * Walks over performance statistics files.
     *
     * @param filesOrDirs Files or directories.
     * @throws IOException If read failed.
     */
    public void read(List<File> filesOrDirs) throws IOException {
        List<File> files = resolveFiles(filesOrDirs);

        if (files.isEmpty())
            return;

        for (File file : files) {
            buf.clear();

            UUID nodeId = nodeId(file);

            try (FileIO io = ioFactory.create(file)) {
                fileIo = io;
                boolean first = true;

                while (true) {
                    if (io.read(buf) <= 0) {
                        if (forwardRead == null)
                            break;

                        io.position(forwardRead.nextRecPos);

                        buf.clear();

                        curHnd = handlers;

                        forwardRead = null;

                        continue;
                    }

                    buf.flip();

                    buf.mark();

                    while (deserialize(buf, nodeId, first)) {
                        first = false;
                        if (forwardRead != null && forwardRead.found) {
                            if (forwardRead.resetBuf) {
                                buf.limit(0);

                                io.position(forwardRead.curRecPos);
                            }
                            else
                                buf.position(forwardRead.bufPos);

                            curHnd = handlers;

                            forwardRead = null;
                        }

                        buf.mark();
                    }

                    buf.reset();

                    if (forwardRead != null)
                        forwardRead.resetBuf = true;

                    buf.compact();
                }
            }

            knownStrs.clear();
            forwardRead = null;
        }
    }

    /**
     * @param buf Buffer.
     * @param nodeId Node id.
     * @param firstRecord Is it first record in the file.
     * @return {@code True} if operation deserialized. {@code False} if not enough bytes.
     */
    private boolean deserialize(ByteBuffer buf, UUID nodeId, boolean firstRecord) throws IOException {
        if (buf.remaining() < 1)
            return false;

        byte opTypeByte = buf.get();

        OperationType opType = OperationType.of(opTypeByte);

        if (firstRecord && opType != VERSION)
            throw new IgniteException("Unsupported file format");

        if (opType == VERSION) {
            if (buf.remaining() < OperationType.versionRecordSize())
                return false;

            short ver = buf.getShort();

            if (ver != FilePerformanceStatisticsWriter.FILE_FORMAT_VERSION) {
                throw new IgniteException("Unsupported file format version [fileVer=" + ver + ", supportedVer=" +
                    FilePerformanceStatisticsWriter.FILE_FORMAT_VERSION + ']');
            }

            return true;
        }
        else if (cacheOperation(opType)) {
            if (buf.remaining() < cacheRecordSize())
                return false;

            int cacheId = buf.getInt();
            long startTime = buf.getLong();
            long duration = buf.getLong();

            for (PerformanceStatisticsHandler hnd : curHnd)
                hnd.cacheOperation(nodeId, opType, cacheId, startTime, duration);

            return true;
        }
        else if (transactionOperation(opType)) {
            if (buf.remaining() < 4)
                return false;

            int cacheIdsCnt = buf.getInt();

            if (buf.remaining() < transactionRecordSize(cacheIdsCnt) - 4)
                return false;

            GridIntList cacheIds = new GridIntList(cacheIdsCnt);

            for (int i = 0; i < cacheIdsCnt; i++)
                cacheIds.add(buf.getInt());

            long startTime = buf.getLong();
            long duration = buf.getLong();

            for (PerformanceStatisticsHandler hnd : curHnd)
                hnd.transaction(nodeId, cacheIds, startTime, duration, opType == TX_COMMIT);

            return true;
        }
        else if (opType == QUERY) {
            ForwardableString text = readString(buf);

            if (text == null || buf.remaining() < readQueryRecordSize())
                return false;

            GridCacheQueryType qryType = GridCacheQueryType.fromOrdinal(buf.get());
            long id = buf.getLong();
            long startTime = buf.getLong();
            long duration = buf.getLong();
            boolean success = buf.get() != 0;

            forwardRead(text);

            for (PerformanceStatisticsHandler hnd : curHnd)
                hnd.query(nodeId, qryType, text.str, id, startTime, duration, success);

            return true;
        }
        else if (opType == SYSTEM_VIEW_SCHEMA) {
            ForwardableString viewName = readString(buf);
            if (viewName == null)
                return false;

            ForwardableString walkerName = readString(buf);
            if (walkerName == null)
                return false;

            assert viewName.str != null : "Views are written by single thread, no string cache misses are possible";
            assert walkerName.str != null : "Views are written by single thread, no string cache misses are possible";

            try {
                sysViewEntry = new SystemViewEntry(viewName.str, walkerName.str);
            }
            catch (ReflectiveOperationException e) {
                throw new IOException("Could not find walker: " + walkerName);
            }
            return true;
        }
        else if (opType == SYSTEM_VIEW_ROW) {
            List<Object> row = sysViewEntry.nextRow();

            if (row == null)
                return false;

            for (PerformanceStatisticsHandler hnd : curHnd)
                hnd.systemView(nodeId, sysViewEntry.viewName, sysViewEntry.schema, row);
            return true;
        }
        else if (opType == QUERY_READS) {
            if (buf.remaining() < queryReadsRecordSize())
                return false;

            GridCacheQueryType qryType = GridCacheQueryType.fromOrdinal(buf.get());
            UUID uuid = readUuid(buf);
            long id = buf.getLong();
            long logicalReads = buf.getLong();
            long physicalReads = buf.getLong();

            for (PerformanceStatisticsHandler hnd : curHnd)
                hnd.queryReads(nodeId, qryType, uuid, id, logicalReads, physicalReads);

            return true;
        }
        else if (opType == QUERY_ROWS) {
            ForwardableString action = readString(buf);

            if (action == null || buf.remaining() < readQueryRowsRecordSize())
                return false;

            GridCacheQueryType qryType = GridCacheQueryType.fromOrdinal(buf.get());
            UUID uuid = readUuid(buf);
            long id = buf.getLong();
            long rows = buf.getLong();

            forwardRead(action);

            for (PerformanceStatisticsHandler hnd : curHnd)
                hnd.queryRows(nodeId, qryType, uuid, id, action.str, rows);

            return true;
        }
        else if (opType == QUERY_PROPERTY) {
            ForwardableString name = readString(buf);

            if (name == null)
                return false;

            ForwardableString val = readString(buf);

            if (val == null)
                return false;

            if (buf.remaining() < readQueryPropertyRecordSize())
                return false;

            GridCacheQueryType qryType = GridCacheQueryType.fromOrdinal(buf.get());
            UUID uuid = readUuid(buf);
            long id = buf.getLong();

            forwardRead(name);
            forwardRead(val);

            for (PerformanceStatisticsHandler hnd : curHnd)
                hnd.queryProperty(nodeId, qryType, uuid, id, name.str, val.str);

            return true;
        }
        else if (opType == TASK) {
            ForwardableString taskName = readString(buf);

            if (taskName == null || buf.remaining() < readTaskRecordSize())
                return false;

            IgniteUuid sesId = readIgniteUuid(buf);
            long startTime = buf.getLong();
            long duration = buf.getLong();
            int affPartId = buf.getInt();

            forwardRead(taskName);

            for (PerformanceStatisticsHandler hnd : curHnd)
                hnd.task(nodeId, sesId, taskName.str, startTime, duration, affPartId);

            return true;
        }
        else if (opType == JOB) {
            if (buf.remaining() < jobRecordSize())
                return false;

            IgniteUuid sesId = readIgniteUuid(buf);
            long queuedTime = buf.getLong();
            long startTime = buf.getLong();
            long duration = buf.getLong();
            boolean timedOut = buf.get() != 0;

            for (PerformanceStatisticsHandler hnd : curHnd)
                hnd.job(nodeId, sesId, queuedTime, startTime, duration, timedOut);

            return true;
        }
        else if (opType == CACHE_START) {
            ForwardableString cacheName = readString(buf);

            if (cacheName == null || buf.remaining() < readCacheStartRecordSize())
                return false;

            int cacheId = buf.getInt();

            forwardRead(cacheName);

            for (PerformanceStatisticsHandler hnd : curHnd)
                hnd.cacheStart(nodeId, cacheId, cacheName.str);

            return true;
        }
        else if (opType == CHECKPOINT) {
            if (buf.remaining() < checkpointRecordSize())
                return false;

            long beforeLockDuration = buf.getLong();
            long lockWaitDuration = buf.getLong();
            long listenersExecDuration = buf.getLong();
            long markDuration = buf.getLong();
            long lockHoldDuration = buf.getLong();
            long pagesWriteDuration = buf.getLong();
            long fsyncDuration = buf.getLong();
            long walCpRecordFsyncDuration = buf.getLong();
            long writeCheckpointEntryDuration = buf.getLong();
            long splitAndSortCpPagesDuration = buf.getLong();
            long recoveryDataWriteDuration = buf.getLong();
            long totalDuration = buf.getLong();
            long cpStartTime = buf.getLong();
            int pagesSize = buf.getInt();
            int dataPagesWritten = buf.getInt();
            int cowPagesWritten = buf.getInt();

            for (PerformanceStatisticsHandler hnd : curHnd) {
                hnd.checkpoint(nodeId,
                    beforeLockDuration,
                    lockWaitDuration,
                    listenersExecDuration,
                    markDuration,
                    lockHoldDuration,
                    pagesWriteDuration,
                    fsyncDuration,
                    walCpRecordFsyncDuration,
                    writeCheckpointEntryDuration,
                    splitAndSortCpPagesDuration,
                    recoveryDataWriteDuration,
                    totalDuration,
                    cpStartTime,
                    pagesSize,
                    dataPagesWritten,
                    cowPagesWritten);
            }

            return true;
        }
        else if (opType == PAGES_WRITE_THROTTLE) {
            if (buf.remaining() < pagesWriteThrottleRecordSize())
                return false;

            long endTime = buf.getLong();
            long duration = buf.getLong();

            for (PerformanceStatisticsHandler hnd : curHnd)
                hnd.pagesWriteThrottle(nodeId, endTime, duration);

            return true;
        }
        else
            throw new IgniteException("Unknown operation type id [typeId=" + opTypeByte + ']');
    }

    /**
     * Enables forward read mode when  {@link ForwardableString#str} is null.
     * @see ForwardableString
     */
    private void forwardRead(ForwardableString forwardableStr) throws IOException {
        if (forwardableStr.str != null)
            return;

        if (forwardRead != null)
            return;

        int pos = buf.position();

        long nextRecPos = fileIo.position() - buf.remaining();

        buf.reset();

        int bufPos = buf.position();

        long curRecPos = fileIo.position() - buf.remaining();

        buf.position(pos);

        curHnd = NOOP_HANDLER;

        forwardRead = new ForwardRead(forwardableStr.hash, curRecPos, nextRecPos, bufPos);
    }

    /** Resolves performance statistics files. */
    static List<File> resolveFiles(List<File> filesOrDirs) throws IOException {
        if (filesOrDirs == null || filesOrDirs.isEmpty())
            return Collections.emptyList();

        List<File> files = new LinkedList<>();

        for (File file : filesOrDirs) {
            if (file.isDirectory()) {
                walkFileTree(file.toPath(), EnumSet.noneOf(FileVisitOption.class), 1,
                    new SimpleFileVisitor<Path>() {
                        @Override public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
                            if (nodeId(path.toFile()) != null)
                                files.add(path.toFile());

                            return FileVisitResult.CONTINUE;
                        }
                    });

                continue;
            }

            if (nodeId(file) != null)
                files.add(file);
        }

        return files;
    }

    /** @return UUID node of file. {@code Null} if this is not a statistics file. */
    @Nullable private static UUID nodeId(File file) {
        Matcher matcher = FILE_NODE_ID_PATTERN.matcher(file.getName());

        if (matcher.matches())
            return UUID.fromString(matcher.group(1));

        return null;
    }

    /**
     * Reads cacheable string from byte buffer.
     *
     * @return {@link ForwardableString} with result of reading or {@code null} in case of buffer underflow.
     */
    private ForwardableString readString(ByteBuffer buf) {
        if (buf.remaining() < 1 + 4)
            return null;

        boolean cached = buf.get() != 0;

        if (cached) {
            int hash = buf.getInt();

            String str = knownStrs.get(hash);

            return new ForwardableString(str, hash);
        }

        int textLen = buf.getInt();

        if (buf.remaining() < textLen)
            return null;

        byte[] bytes = new byte[textLen];

        buf.get(bytes);

        String str = new String(bytes);

        knownStrs.putIfAbsent(str.hashCode(), str);

        if (forwardRead != null && forwardRead.hash == str.hashCode())
            forwardRead.found = true;

        return new ForwardableString(str, str.hashCode());
    }

    /**
     * Result of reading string from buffer that may be cached.
     * Call {@link #forwardRead(ForwardableString)} after reading the entire record to enable forward read mode.
     */
    private static class ForwardableString {
        /** Can be {@code null} if the string is cached and there is no such {@link #hash} in {@link #knownStrs}. */
        @Nullable final String str;

        /** */
        final int hash;

        /** */
        ForwardableString(@Nullable String str, int hash) {
            this.str = str;
            this.hash = hash;
        }
    }

    /** Reads {@link UUID} from buffer. */
    private static UUID readUuid(ByteBuffer buf) {
        return new UUID(buf.getLong(), buf.getLong());
    }

    /** Reads {@link IgniteUuid} from buffer. */
    private static IgniteUuid readIgniteUuid(ByteBuffer buf) {
        UUID globalId = new UUID(buf.getLong(), buf.getLong());

        return new IgniteUuid(globalId, buf.getLong());
    }

    /** Forward read mode info. */
    private static class ForwardRead {
        /** Hashcode. */
        final int hash;

        /** Absolute current record position. */
        final long curRecPos;

        /** Absolute next record position. */
        final long nextRecPos;

        /** Current record buffer position. */
        final int bufPos;

        /** String found flag. */
        boolean found;

        /** {@code True} if the data in the buffer was overwritten during the search. */
        boolean resetBuf;

        /**
         * @param hash Hashcode.
         * @param curRecPos Absolute current record position.
         * @param nextRecPos Absolute next record position.
         * @param bufPos Buffer position.
         */
        private ForwardRead(int hash, long curRecPos, long nextRecPos, int bufPos) {
            this.hash = hash;
            this.curRecPos = curRecPos;
            this.nextRecPos = nextRecPos;
            this.bufPos = bufPos;
        }
    }

    /** Reads views from buf. */
    private class SystemViewEntry {
        /** */
        private final String viewName;

        /** Attribute names of system view. */
        private final List<String> schema;

        /**  */
        private final SystemViewRowAttributeWalker<?> walker;

        /**  */
        private final RowReaderVisitor rowVisitor;

        /**
         * @param viewName System view name.
         * @param walkerName Name of walker to visist system view attributes.
         */
        public SystemViewEntry(String viewName, String walkerName) throws ReflectiveOperationException {
            walker = (SystemViewRowAttributeWalker<?>)Class.forName(walkerName).getConstructor().newInstance();

            this.viewName = viewName;

            List<String> schemaList = new ArrayList<>();

            walker.visitAll(new SystemViewRowAttributeWalker.AttributeVisitor() {
                @Override public <T> void accept(int idx, String name, Class<T> clazz) {
                    schemaList.add(name);
                }
            });

            schema = Collections.unmodifiableList(schemaList);

            rowVisitor = new RowReaderVisitor(schema.size());
        }

        /**
         * @return System view row.
         */
        public List<Object> nextRow() {
            rowVisitor.clear();
            walker.visitAll(rowVisitor);
            return rowVisitor.row();
        }
    }

    /** Write schema of system view to file. */
    private class RowReaderVisitor implements SystemViewRowAttributeWalker.AttributeVisitor {
        /** Number of system view attributes. */
        private final int size;

        /** Row. */
        private List<Object> row;

        /** Not enough bytes. */
        private boolean notEnoughBytes;

        public RowReaderVisitor(int size) {
            this.size = size;

            row = new ArrayList<>(size);
        }

        /**  */
        public List<Object> row() {
            if (notEnoughBytes)
                return null;

            return Collections.unmodifiableList(row);
        }

        /** */
        public void clear() {
            row = new ArrayList<>(size);

            notEnoughBytes = false;
        }

        /** {@inheritDoc} */
        @Override public <T> void accept(int idx, String name, Class<T> clazz) {
            if (notEnoughBytes)
                return;

            if (clazz == int.class) {
                if (buf.remaining() < 4) {
                    notEnoughBytes = true;
                    return;
                }
                row.add(buf.getInt());
            }
            else if (clazz == byte.class) {
                if (buf.remaining() < 1) {
                    notEnoughBytes = true;
                    return;
                }
                row.add(buf.get());
            }
            else if (clazz == short.class) {
                if (buf.remaining() < 2) {
                    notEnoughBytes = true;
                    return;
                }
                row.add(buf.getShort());
            }
            else if (clazz == long.class) {
                if (buf.remaining() < 8) {
                    notEnoughBytes = true;
                    return;
                }
                row.add(buf.getLong());
            }
            else if (clazz == float.class) {
                if (buf.remaining() < 4) {
                    notEnoughBytes = true;
                    return;
                }
                row.add(buf.getFloat());
            }
            else if (clazz == double.class) {
                if (buf.remaining() < 8) {
                    notEnoughBytes = true;
                    return;
                }
                row.add(buf.getDouble());
            }
            else if (clazz == char.class) {
                if (buf.remaining() < 2) {
                    notEnoughBytes = true;
                    return;
                }
                row.add(buf.getChar());
            }
            else if (clazz == boolean.class) {
                if (buf.remaining() < 1) {
                    notEnoughBytes = true;
                    return;
                }
                row.add(buf.get() != 0);
            }
            else {
                ForwardableString str = readString(buf);
                if (str != null)
                    row.add(str.str);
            }
        }
    }
}
