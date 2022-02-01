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
import org.jetbrains.annotations.Nullable;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.nativeOrder;
import static java.nio.file.Files.walkFileTree;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_START;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CHECKPOINT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.JOB;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.PAGES_WRITE_THROTTLE;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY_READS;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TASK;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TX_COMMIT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.cacheOperation;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.cacheRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.cacheStartRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.checkpointRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.jobRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.pagesWriteThrottleRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.queryReadsRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.queryRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.taskRecordSize;
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
    private static final Pattern FILE_PATTERN = Pattern.compile("^node-(" + UUID_STR_PATTERN + ")(-\\d+)?.prf$");

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

                    while (deserialize(buf, nodeId)) {
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
     * @return {@code True} if operation deserialized. {@code False} if not enough bytes.
     */
    private boolean deserialize(ByteBuffer buf, UUID nodeId) throws IOException {
        if (buf.remaining() < 1)
            return false;

        byte opTypeByte = buf.get();

        OperationType opType = OperationType.of(opTypeByte);

        if (cacheOperation(opType)) {
            if (buf.remaining() < cacheRecordSize())
                return false;

            int cacheId = buf.getInt();
            long startTime = buf.getLong();
            long duration = buf.getLong();

            for (PerformanceStatisticsHandler handler : curHnd)
                handler.cacheOperation(nodeId, opType, cacheId, startTime, duration);

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

            for (PerformanceStatisticsHandler handler : curHnd)
                handler.transaction(nodeId, cacheIds, startTime, duration, opType == TX_COMMIT);

            return true;
        }
        else if (opType == QUERY) {
            if (buf.remaining() < 1)
                return false;

            boolean cached = buf.get() != 0;

            String text;
            int hash = 0;

            if (cached) {
                if (buf.remaining() < 4)
                    return false;

                hash = buf.getInt();

                text = knownStrs.get(hash);

                if (buf.remaining() < queryRecordSize(0, true) - 1 - 4)
                    return false;
            }
            else {
                if (buf.remaining() < 4)
                    return false;

                int textLen = buf.getInt();

                if (buf.remaining() < queryRecordSize(textLen, false) - 1 - 4)
                    return false;

                text = readString(buf, textLen);
            }

            GridCacheQueryType queryType = GridCacheQueryType.fromOrdinal(buf.get());
            long id = buf.getLong();
            long startTime = buf.getLong();
            long duration = buf.getLong();
            boolean success = buf.get() != 0;

            if (text == null)
                forwardRead(hash);

            for (PerformanceStatisticsHandler handler : curHnd)
                handler.query(nodeId, queryType, text, id, startTime, duration, success);

            return true;
        }
        else if (opType == QUERY_READS) {
            if (buf.remaining() < queryReadsRecordSize())
                return false;

            GridCacheQueryType queryType = GridCacheQueryType.fromOrdinal(buf.get());
            UUID uuid = readUuid(buf);
            long id = buf.getLong();
            long logicalReads = buf.getLong();
            long physicalReads = buf.getLong();

            for (PerformanceStatisticsHandler handler : curHnd)
                handler.queryReads(nodeId, queryType, uuid, id, logicalReads, physicalReads);

            return true;
        }
        else if (opType == TASK) {
            if (buf.remaining() < 1)
                return false;

            boolean cached = buf.get() != 0;

            String taskName;
            int hash = 0;

            if (cached) {
                if (buf.remaining() < 4)
                    return false;

                hash = buf.getInt();

                taskName = knownStrs.get(hash);

                if (buf.remaining() < taskRecordSize(0, true) - 1 - 4)
                    return false;
            }
            else {
                if (buf.remaining() < 4)
                    return false;

                int nameLen = buf.getInt();

                if (buf.remaining() < taskRecordSize(nameLen, false) - 1 - 4)
                    return false;

                taskName = readString(buf, nameLen);
            }

            IgniteUuid sesId = readIgniteUuid(buf);
            long startTime = buf.getLong();
            long duration = buf.getLong();
            int affPartId = buf.getInt();

            if (taskName == null)
                forwardRead(hash);

            for (PerformanceStatisticsHandler handler : curHnd)
                handler.task(nodeId, sesId, taskName, startTime, duration, affPartId);

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

            for (PerformanceStatisticsHandler handler : curHnd)
                handler.job(nodeId, sesId, queuedTime, startTime, duration, timedOut);

            return true;
        }
        else if (opType == CACHE_START) {
            if (buf.remaining() < 1)
                return false;

            boolean cached = buf.get() != 0;

            String cacheName;
            int hash = 0;

            if (cached) {
                if (buf.remaining() < 4)
                    return false;

                hash = buf.getInt();

                cacheName = knownStrs.get(hash);

                if (buf.remaining() < cacheStartRecordSize(0, true) - 1 - 4)
                    return false;
            }
            else {
                if (buf.remaining() < 4)
                    return false;

                int nameLen = buf.getInt();

                if (buf.remaining() < cacheStartRecordSize(nameLen, false) - 1 - 4)
                    return false;

                cacheName = readString(buf, nameLen);
            }

            int cacheId = buf.getInt();

            for (PerformanceStatisticsHandler handler : curHnd)
                handler.cacheStart(nodeId, cacheId, cacheName);

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
            long totalDuration = buf.getLong();
            long cpStartTime = buf.getLong();
            int pagesSize = buf.getInt();
            int dataPagesWritten = buf.getInt();
            int cowPagesWritten = buf.getInt();

            for (PerformanceStatisticsHandler handler : curHnd) {
                handler.checkpoint(nodeId,
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

            for (PerformanceStatisticsHandler handler : curHnd)
                handler.pagesWriteThrottle(nodeId, endTime, duration);

            return true;
        }
        else
            throw new IgniteException("Unknown operation type id [typeId=" + opTypeByte + ']');
    }

    /** Turns on forward read mode. */
    private void forwardRead(int hash) throws IOException {
        if (forwardRead != null)
            return;

        int pos = buf.position();

        long nextRecPos = fileIo.position() - buf.remaining();

        buf.reset();

        int bufPos = buf.position();

        long curRecPos = fileIo.position() - buf.remaining();

        buf.position(pos);

        curHnd = NOOP_HANDLER;

        forwardRead = new ForwardRead(hash, curRecPos, nextRecPos, bufPos);
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
        Matcher matcher = FILE_PATTERN.matcher(file.getName());

        if (matcher.matches())
            return UUID.fromString(matcher.group(1));

        return null;
    }

    /** Reads string from byte buffer. */
    private String readString(ByteBuffer buf, int size) {
        byte[] bytes = new byte[size];

        buf.get(bytes);

        String str = new String(bytes);

        knownStrs.putIfAbsent(str.hashCode(), str);

        if (forwardRead != null && forwardRead.hash == str.hashCode())
            forwardRead.found = true;

        return str;
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
}
