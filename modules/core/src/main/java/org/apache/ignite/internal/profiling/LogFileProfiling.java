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

package org.apache.ignite.internal.profiling;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentedRingByteBuffer;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.GridIntIterator;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.logger.java.JavaLogger;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.thread.IgniteThread;

import static org.apache.ignite.internal.profiling.LogFileProfiling.OperationType.PROFILING_START;
import static org.apache.ignite.internal.util.IgniteUtils.sleep;

/** */
public class LogFileProfiling implements IgniteProfiling {
    /** 16 GBytes */
    public static final long DEFAULT_FILE_MAX_SIZE = 16 * 1024 * 1024 * 1024L;

    /** Default file write buffer size in bytes. */
    public static final int DFLT_BUFFER_SIZE = 16 * 1024 * 1024;

    /** Empty byte array. */
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    /** File write buffer. */
    private final SegmentedRingByteBuffer ringByteBuffer;

    /** Factory to provide I/O interface for profiling file. */
    private final FileIOFactory fileIoFactory = new RandomAccessFileIOFactory();

    /** */
    private volatile FileIO fileIo;

    /** */
    private final IgniteLogger log;

    /** */
    public LogFileProfiling(GridKernalContext ctx) {
        ringByteBuffer = new SegmentedRingByteBuffer(DFLT_BUFFER_SIZE, Long.MAX_VALUE, SegmentedRingByteBuffer.BufferMode.DIRECT);

        log = ctx.log(getClass());
    }

    /** */
    public void onManagerStart(GridKernalContext ctx) throws IgniteSpiException {
        ringByteBuffer.init(0);

        FileSyncer syncerWorker = new FileSyncer(ctx.igniteInstanceName(), new JavaLogger());

        new IgniteThread(syncerWorker).start();

        try {
            String igniteWorkDir = U.workDirectory(ctx.config().getWorkDirectory(), ctx.config().getIgniteHome());

            File profilingDir = U.resolveWorkDirectory(igniteWorkDir, "profiling", false);

            File file = new File(profilingDir, "profiling-" + ctx.localNodeId() + ".log");

            fileIo = fileIoFactory.create(file);

            fileIo.position(0);
        }
        catch (Exception e) {
            throw new IgniteSpiException("Failed to start profiling.", e);
        }

        profilingStart(ctx.localNodeId(), ctx.igniteInstanceName(), IgniteVersionUtils.VER_STR, U.currentTimeMillis());
    }

    /** */
    public void onManagerStop() throws IgniteSpiException {
        try {
            // todo stop receive operations and flush buffer.
            fileIo.force();

            fileIo.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    /** {@inheritDoc} */
    @Override public void cacheOperation(CacheOperationType type, int cacheId, long startTime, long duration) {
        int size = /*type*/ 1 +
            /*cacheId*/ 4 +
            /*startTime*/ 8 +
            /*duration*/ 8;

        SegmentedRingByteBuffer.WriteSegment segment = reserveBuffer(OperationType.CACHE_OPERATION, size);

        ByteBuffer buf = segment.buffer();

        buf.put((byte)type.ordinal());
        buf.putInt(cacheId);
        buf.putLong(startTime);
        buf.putLong(duration);

        segment.release();
    }

    /** {@inheritDoc} */
    @Override public void transaction(GridIntList cacheIds, long startTime, long duration, boolean commit) {
        int size = /*cacheIds*/ 4 + cacheIds.size() * 4 +
            /*startTime*/ 8 +
            /*duration*/ 8 +
            /*commit*/ 1;

        SegmentedRingByteBuffer.WriteSegment segment = reserveBuffer(OperationType.TRANSACTION, size);

        ByteBuffer buf = segment.buffer();

        buf.putInt(cacheIds.size());

        GridIntIterator iter = cacheIds.iterator();

        while (iter.hasNext())
            buf.putInt(iter.next());

        buf.putLong(startTime);
        buf.putLong(duration);
        buf.put(commit ? (byte)1 : 0);

        segment.release();
    }

    /** {@inheritDoc} */
    @Override public void query(GridCacheQueryType type, String text, UUID queryNodeId, long id, long startTime,
        long duration, boolean success) {
        byte[] textBytes = text.getBytes();

        int size = /*type*/ 1 +
            /*text*/ 4 + textBytes.length +
            /*queryNodeId*/ 16 +
            /*id*/ 8 +
            /*startTime*/ 8 +
            /*duration*/ 8 +
            /*startTime*/ 1;

        SegmentedRingByteBuffer.WriteSegment segment = reserveBuffer(OperationType.QUERY, size);

        ByteBuffer buf = segment.buffer();

        buf.put((byte)type.ordinal());
        buf.putInt(textBytes.length);
        buf.put(textBytes);
        writeUuid(buf, queryNodeId);
        buf.putLong(id);
        buf.putLong(startTime);
        buf.putLong(duration);
        buf.put(success ? (byte)1 : 0);

        segment.release();
    }

    /** {@inheritDoc} */
    @Override public void queryReads(GridCacheQueryType type, UUID queryNodeId, long id, long logicalReads,
        long physicalReads) {
        int size = /*type*/ 1 +
            /*queryNodeId*/ 16 +
            /*id*/ 8 +
            /*logicalReads*/ 8 +
            /*physicalReads*/ 8;

        SegmentedRingByteBuffer.WriteSegment segment = reserveBuffer(OperationType.QUERY_READS, size);

        ByteBuffer buf = segment.buffer();

        buf.put((byte)type.ordinal());
        writeUuid(buf, queryNodeId);
        buf.putLong(id);
        buf.putLong(logicalReads);
        buf.putLong(physicalReads);

        segment.release();
    }

    /** {@inheritDoc} */
    @Override public void task(IgniteUuid sesId, String taskName, long startTime, long duration, int affPartId) {
        byte[] taskNameBytes = taskName.getBytes();

        int size = /*sesId*/ 24 +
            /*taskName*/ 4 + taskNameBytes.length +
            /*startTime*/ 8 +
            /*duration*/ 8 +
            /*affPartId*/ 4;

        SegmentedRingByteBuffer.WriteSegment segment = reserveBuffer(OperationType.TASK, size);

        ByteBuffer buf = segment.buffer();

        writeIgniteUuid(buf, sesId);
        buf.putInt(taskNameBytes.length);
        buf.put(taskNameBytes);
        buf.putLong(startTime);
        buf.putLong(duration);
        buf.putInt(affPartId);

        segment.release();
    }

    /** {@inheritDoc} */
    @Override public void job(IgniteUuid sesId, long queuedTime, long startTime, long duration, boolean timedOut) {
        int size = /*sesId*/ 24 +
            /*queuedTime*/ 8 +
            /*startTime*/ 8 +
            /*duration*/ 8 +
            /*timedOut*/ 1;

        SegmentedRingByteBuffer.WriteSegment segment = reserveBuffer(OperationType.JOB, size);

        ByteBuffer buf = segment.buffer();

        writeIgniteUuid(buf, sesId);
        buf.putLong(queuedTime);
        buf.putLong(startTime);
        buf.putLong(duration);
        buf.put(timedOut ? (byte)1 : 0);

        segment.release();
    }

    /** {@inheritDoc} */
    @Override public void cacheStart(int cacheId, long startTime, String cacheName, String groupName,
        boolean userCache) {
        byte[] cacheNameBytes = cacheName.getBytes();
        byte[] groupNameBytes = groupName == null ? EMPTY_BYTE_ARRAY : groupName.getBytes();

        int size = /*cacheId*/ 4 +
            /*startTime*/ 8 +
            /*cacheName*/ 4 + cacheNameBytes.length +
            /*groupName*/ 4 + groupNameBytes.length +
            /*userCacheFlag*/ 1;

        SegmentedRingByteBuffer.WriteSegment segment = reserveBuffer(OperationType.CACHE_START, size);

        ByteBuffer buf = segment.buffer();

        buf.putInt(cacheId);
        buf.putLong(startTime);

        buf.putInt(cacheNameBytes.length);
        buf.put(cacheNameBytes);

        if (groupNameBytes == null)
            buf.putInt(0);
        else {
            buf.putInt(groupNameBytes.length);
            buf.put(groupNameBytes);
        }

        buf.put(userCache ? (byte)1 : 0);

        segment.release();
    }

    /** {@inheritDoc} */
    @Override public void profilingStart(UUID nodeId, String igniteInstanceName, String igniteVersion, long startTime) {
        byte[] nameBytes = igniteInstanceName.getBytes();
        byte[] versionBytes = igniteVersion.getBytes();

        int size = /*nodeId*/ 16 +
            /*igniteInstanceName*/ 4 + nameBytes.length +
            /*version*/ 4 + versionBytes.length +
            /*profilingStartTime*/ 8;

        SegmentedRingByteBuffer.WriteSegment segment = reserveBuffer(PROFILING_START, size);

        ByteBuffer buf = segment.buffer();

        writeUuid(buf, nodeId);
        buf.putInt(nameBytes.length);
        buf.put(nameBytes);
        buf.putInt(versionBytes.length);
        buf.put(versionBytes);
        buf.putLong(System.currentTimeMillis());

        segment.release();
    }

    /** */
    private SegmentedRingByteBuffer.WriteSegment reserveBuffer(OperationType type, int size) {
        for (; ; ) {
            SegmentedRingByteBuffer.WriteSegment seg = ringByteBuffer.offer(size + /*type*/ 1);

            if (seg == null) {
                LT.warn(log, "Buffer size is too small.");

                continue;
            }

            ByteBuffer buf = seg.buffer();

            buf.put((byte)type.ordinal());

            return seg;
        }
    }

    /** */
    public static void writeUuid(ByteBuffer buf, UUID uuid) {
        buf.putLong(uuid.getMostSignificantBits());
        buf.putLong(uuid.getLeastSignificantBits());
    }

    /** */
    public static UUID readUuid(ByteBuffer buf) {
        return new UUID(buf.getLong(), buf.getLong());
    }

    /** */
    public static void writeIgniteUuid(ByteBuffer buf, IgniteUuid uuid) {
        buf.putLong(uuid.globalId().getMostSignificantBits());
        buf.putLong(uuid.globalId().getLeastSignificantBits());
        buf.putLong(uuid.localId());
    }

    /** */
    public static IgniteUuid readIgniteUuid(ByteBuffer buf) {
        UUID globalId = new UUID(buf.getLong(), buf.getLong());

        return new IgniteUuid(globalId, buf.getLong());
    }

    /**
     * Syncs WAL segment file.
     */
    private class FileSyncer extends GridWorker {
        /** Sync timeout. */
        private static final long SYNC_TIMEOUT = 100L;

        /**
         * @param igniteInstanceName Ignite instance name.
         * @param log Logger.
         */
        private FileSyncer(String igniteInstanceName, IgniteLogger log) {
            super(igniteInstanceName, "profiling-syncer", log);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            while (!isCancelled()) {
                sleep(SYNC_TIMEOUT);

                updateHeartbeat();

                List<SegmentedRingByteBuffer.ReadSegment> segs = ringByteBuffer.poll();

                if (segs == null)
                    continue;

                for (int i = 0; i < segs.size(); i++) {
                    SegmentedRingByteBuffer.ReadSegment seg = segs.get(i);

                    try {
                        writeBuffer(seg.buffer());
                    }
                    catch (Throwable e) {
                        log.error("Exception in profiling writer thread:", e);
                        //Critical error.
                    }
                    finally {
                        seg.release();
                    }
                }
            }
        }

        private void writeBuffer(ByteBuffer buffer) throws IOException {
            fileIo.writeFully(buffer);

            fileIo.force();
        }

        /** Shutted down the worker. */
        private void shutdown() {
            synchronized (this) {
                U.cancel(this);
            }

            U.join(this, log);
        }
    }

    /** Operation type. */
    public enum OperationType {
        /** Cache operation. */
        CACHE_OPERATION,

        /** Transaction. */
        TRANSACTION,

        /** Query. */
        QUERY,

        /** Query reads. */
        QUERY_READS,

        /** Task. */
        TASK,

        /** Job. */
        JOB,

        /** Cache start. */
        CACHE_START,

        /** Profiling start. */
        PROFILING_START;

        /** */
        private static final OperationType[] VALS = values();

        /** */
        public static OperationType fromOrdinal(byte idx) {
            return idx < 0 || idx >= VALS.length ? null : VALS[idx];
        }
    }
}
