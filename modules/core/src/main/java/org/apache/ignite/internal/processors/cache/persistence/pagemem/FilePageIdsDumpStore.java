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
package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BooleanSupplier;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIO;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.pagemem.PageIdUtils.PART_ID_MASK;

/**
 *
 */
public class FilePageIdsDumpStore implements PageIdsDumpStore {
    /** File name length. */
    private static final int FILE_NAME_LENGTH = 16;

    /** Dump file extension. */
    private static final String DUMP_FILE_EXT = ".dump";

    /** File name pattern. */
    private static final Pattern FILE_NAME_PATTERN = Pattern.compile("[0-9A-Fa-f]{" + FILE_NAME_LENGTH + "}\\" + DUMP_FILE_EXT);

    /** File filter. */
    private static final FileFilter FILE_FILTER = file -> !file.isDirectory() && FILE_NAME_PATTERN.matcher(file.getName()).matches();

    /** Comparator by partition ID. */
    private static final Comparator<Partition> BY_PART_ID = Comparator.comparingInt(Partition::id);

    /** Small page count threshold. */
    private static final int SMALL_PAGE_COUNT_THRESHOLD = 4;

    /** Partition block overhead: cacheId(4) + partId(2) + count(4). */
    private static final int PARTITION_BLOCK_OVERHEAD = 10;

    /** Partition item size: pageIdx(4). */
    private static final int PARTITION_ITEM_SIZE = 4;

    /** Cache block overhead: zeroCacheId(4) + cacheId(4) + count(4). */
    private static final int CACHE_BLOCK_OVERHEAD = 12;

    /** Cache item size: partId(2) + pageIdx(4). */
    private static final int CACHE_ITEM_SIZE = 6;

    /** Page IDs dump directory. */
    private final File dumpDir;

    /** */
    private final IgniteLogger log;

    /** Dump context. */
    private volatile DumpContext dumpCtx;

    /**
     * @param dumpDir Page IDs dump directory.
     */
    public FilePageIdsDumpStore(File dumpDir, IgniteLogger log) {
        this.dumpDir = dumpDir;
        this.log = log.getLogger(FilePageIdsDumpStore.class);
    }

    /** {@inheritDoc} */
    @Override public String createDump() {
        if (dumpCtx != null)
            throw new IllegalStateException("Attempt to create new dump while other one still active");

        long ts = U.currentTimeMillis();

        while (true) {
            String dumpId = toDumpId(ts);

            if (new File(dumpDir, dumpId + ".dump").exists()) {
                ts += 10;

                continue;
            }

            try {
                dumpCtx = new DumpContext(dumpId, true);

                return dumpId;
            }
            catch (IOException e) {
                throw new IgniteException("Dump creation failed", e);
            }
        }
    }

    /**
     * @param val Value.
     */
    private static String toDumpId(long val) {
        SB b = new SB();

        String keyHex = Long.toHexString(val);

        for (int i = keyHex.length(); i < FILE_NAME_LENGTH; i++)
            b.a('0');

        return b.a(keyHex).toString();
    }

    /** {@inheritDoc} */
    @Override public void finishDump() {
        DumpContext dumpCtx = this.dumpCtx;

        if (dumpCtx == null)
            throw new IllegalStateException("No active dump found");

        dumpCtx.finish();

        this.dumpCtx = null;
    }

    /** {@inheritDoc} */
    @Override public void save(Iterable<Partition> partitions) {
        DumpContext dumpCtx = this.dumpCtx;

        if (dumpCtx == null)
            throw new IllegalStateException("No active dump found");

        Map<Integer, int[]> cacheSmallPartsCounts = cacheSmallPartsCounts(partitions);

        Map<Integer, List<Partition>> cacheSmallParts = new HashMap<>(cacheSmallPartsCounts.size());

        try {
            for (Partition part : partitions) {
                int pageCnt = part.pageIndexes().length;

                if (pageCnt == 0)
                    continue;

                if (pageCnt <= SMALL_PAGE_COUNT_THRESHOLD && cacheSmallPartsCounts.containsKey(part.cacheId())) {
                    List<Partition> smallParts = cacheSmallParts.computeIfAbsent(
                        part.cacheId(),
                        cacheId -> new ArrayList<>(totalPartsCount(cacheSmallPartsCounts.get(cacheId))));

                    smallParts.add(part);

                    continue;
                }

                dumpCtx.writeInt(part.cacheId());
                dumpCtx.writeShort((short)part.id());
                dumpCtx.writeInt(part.pageIndexes().length);

                for (int pageIdx : part.pageIndexes())
                    dumpCtx.writeInt(pageIdx);
            }

            for (Map.Entry<Integer, List<Partition>> entry : cacheSmallParts.entrySet()) {
                List<Partition> cacheParts = entry.getValue();

                cacheParts.sort(BY_PART_ID);

                int cachePageIdsCnt = cacheParts.stream().mapToInt(p -> p.pageIndexes().length).sum();

                dumpCtx.writeInt(0);
                dumpCtx.writeInt(entry.getKey());
                dumpCtx.writeInt(cachePageIdsCnt);

                for (Partition part : cacheParts) {
                    for (int pageIdx : part.pageIndexes()) {
                        dumpCtx.writeShort((short)part.id());
                        dumpCtx.writeInt(pageIdx);
                    }
                }
            }

            dumpCtx.flush();
        }
        catch (IOException e) {
            throw new IgniteException("Failed to write dump data", e);
        }
    }

    /**
     * @param partitions Partitions.
     * @return Map which keys are cache IDs and values are arrays of counts of partitions
     * with page counts from 1 (first array value) to {@link #SMALL_PAGE_COUNT_THRESHOLD} (last array value) accordingly.
     */
    private Map<Integer, int[]> cacheSmallPartsCounts(Iterable<Partition> partitions) {
        ConcurrentMap<Integer, int[]> cacheSmallPartsCounts = new ConcurrentHashMap<>();

        for (Partition part : partitions) {
            int pageCnt = part.pageIndexes().length;

            if (pageCnt == 0 || pageCnt > SMALL_PAGE_COUNT_THRESHOLD)
                continue;

            int[] counts = cacheSmallPartsCounts.computeIfAbsent(part.cacheId(), key -> new int[SMALL_PAGE_COUNT_THRESHOLD]);

            counts[pageCnt - 1]++;
        }

        cacheSmallPartsCounts.values().removeIf(FilePageIdsDumpStore::usePartitionBlocks);

        return cacheSmallPartsCounts;
    }

    /**
     * @param pageCnt Page count.
     */
    private static int partitionBlockSize(int pageCnt) {
        return PARTITION_BLOCK_OVERHEAD + pageCnt * PARTITION_ITEM_SIZE;
    }

    /**
     * @param pageCnt Page count.
     */
    private static int cacheBlockSize(int pageCnt) {
        return CACHE_BLOCK_OVERHEAD + pageCnt * CACHE_ITEM_SIZE;
    }

    /**
     * @param stats Stats.
     */
    private static int totalPartsCount(int[] stats) {
        int res = 0;

        for (int i = 0; i < stats.length; i++)
            res += (i + 1) * stats[i];

        return res;
    }

    /**
     * @param stats Stats.
     */
    private static boolean usePartitionBlocks(int[] stats) {
        int pageCntTotal = 0;
        int partBlocksTotalSize = 0;

        for (int i = 0; i < stats.length; i++) {
            pageCntTotal += stats[i] * (i + 1);

            partBlocksTotalSize += partitionBlockSize(i + 1) * stats[i];
        }

        return partBlocksTotalSize <= cacheBlockSize(pageCntTotal);
    }

    /** {@inheritDoc} */
    @Override public void forEach(FullPageIdConsumer consumer, BooleanSupplier breakCond) {
        File[] dumpFiles = dumpDir.listFiles(FILE_FILTER);

        if (dumpFiles == null || dumpFiles.length == 0) {
            if (log.isInfoEnabled())
                log.info("Saved dump files not found!");

            return;
        }

        if (log.isInfoEnabled())
            log.info("Saved dump files found: " + dumpFiles.length);

        File latestDumpFile = Collections.max(Arrays.asList(dumpFiles), Comparator.comparing(File::getName));

        forEach(latestDumpFile, consumer, breakCond);
    }

    /** {@inheritDoc} */
    @Override public void forEach(String dumpId, FullPageIdConsumer consumer, BooleanSupplier breakCond) {
        forEach(new File(dumpDir, dumpId + DUMP_FILE_EXT), consumer, breakCond);
    }

    /**
     * @param consumer Consumer.
     * @param dumpFile Dump file.
     */
    private void forEach(File dumpFile, FullPageIdConsumer consumer, BooleanSupplier breakCond) {
        try (DumpContext dumpCtx = new DumpContext(dumpFile)) {
            String errorMsg = "Corrupted dump file [name" + dumpFile.getName() + "]";

            do {
                if (breakCond != null && breakCond.getAsBoolean())
                    break;

                if (dumpCtx.availableBytes() < 10)
                    throw new IOException(errorMsg);

                int cacheId = dumpCtx.readInt();

                if (cacheId == 0) { // Read cache
                    cacheId = dumpCtx.readInt();

                    int cnt = dumpCtx.readInt();

                    if (dumpCtx.availableBytes() < cnt * (Short.BYTES + Integer.BYTES))
                        throw new IOException(errorMsg);

                    for (int i = 0; i < cnt; i++) {
                        short partId = dumpCtx.readShort();
                        int pageIdx = dumpCtx.readInt();

                        long pageId = PageIdUtils.pageId(partId & (int)PART_ID_MASK, pageIdx);

                        consumer.accept(cacheId, pageId);
                    }
                }
                else { // Read partition
                    short partId = dumpCtx.readShort();

                    int cnt = dumpCtx.readInt();

                    if (dumpCtx.availableBytes() < cnt * Integer.BYTES)
                        throw new IOException(errorMsg);

                    for (int i = 0; i < cnt; i++) {
                        int pageIdx = dumpCtx.readInt();

                        long pageId = PageIdUtils.pageId(partId & (int)PART_ID_MASK, pageIdx);

                        consumer.accept(cacheId, pageId);
                    }
                }
            }
            while (dumpCtx.availableBytes() > 0);
        }
        catch (IOException e) {
            throw new IgniteException("Failed to read dump file [name=" + dumpFile.getName() + "]", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        // TODO
    }

    /**
     * @param segFile Partition file.
     */
    private int[] loadPageIndexes(File segFile) throws IOException {
        try (FileIO io = new RandomAccessFileIO(segFile, StandardOpenOption.READ)) {
            int[] pageIdxArr = new int[(int)(io.size() / Integer.BYTES)];

            byte[] intBytes = new byte[Integer.BYTES];

            for (int i = 0; i < pageIdxArr.length; i++) {
                io.read(intBytes, 0, intBytes.length);

                pageIdxArr[i] = U.bytesToInt(intBytes, 0);
            }

            return pageIdxArr;
        }
    }

    /**
     *
     */
    private class DumpContext implements AutoCloseable {
        /** New file extension. */
        private static final String NEW_FILE_EXT = ".new";

        /** */
        private final String dumpId;

        /** */
        private final File dumpFile;

        /** */
        private final FileIO dumpIO;

        /** */
        private final byte[] chunk;

        /** */
        private int chunkPtr;

        /** */
        private int ioPtr;

        /**
         * @param dumpId Dump id.
         * @param isNew Is new.
         */
        DumpContext(String dumpId, boolean isNew) throws IOException {
            this.dumpId = dumpId;

            dumpFile = new File(dumpDir, dumpId + (isNew ? DUMP_FILE_EXT + NEW_FILE_EXT : DUMP_FILE_EXT));

            dumpIO = isNew ?
                new RandomAccessFileIO(dumpFile,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING) :
                new RandomAccessFileIO(dumpFile, StandardOpenOption.READ);

            chunk = new byte[isNew ? Integer.BYTES * 1024 : Integer.BYTES];
        }

        /**
         * @param dumpFile Dump file.
         */
        DumpContext(File dumpFile) throws IOException {
            this.dumpFile = dumpFile;

            dumpId = dumpFile.getName().substring(0, dumpFile.getName().indexOf(DUMP_FILE_EXT));

            dumpIO = new RandomAccessFileIO(dumpFile, StandardOpenOption.READ);

            chunk = new byte[Integer.BYTES];
        }

        /**
         *
         */
        int readInt() throws IOException {
            int bytesRead = dumpIO.read(chunk, 0, Integer.BYTES);

            ioPtr += bytesRead;

            if (bytesRead != Integer.BYTES)
                throw new IOException("Failed to read dump file [id=" + dumpId + "]");

            return U.bytesToInt(chunk, 0);
        }

        /**
         * @param val Value.
         */
        void writeInt(int val) throws IOException {
            if (chunkPtr + Integer.BYTES > chunk.length)
                flushChunk();

            chunkPtr = U.intToBytes(val, chunk, chunkPtr);
        }

        /**
         *
         */
        short readShort() throws IOException {
            int bytesRead = dumpIO.read(chunk, 0, Short.BYTES);

            ioPtr += bytesRead;

            if (bytesRead != Short.BYTES)
                throw new IOException("Failed to read dump file [id=" + dumpId + "]");

            return U.bytesToShort(chunk, 0);
        }

        /**
         * @param val Value.
         */
        void writeShort(short val) throws IOException {
            if (chunkPtr + Short.BYTES > chunk.length)
                flushChunk();

            chunkPtr = U.shortToBytes(val, chunk, chunkPtr);
        }

        /**
         *
         */
        long availableBytes() throws IOException {
            return dumpIO.size() - ioPtr;
        }

        /**
         *
         */
        void flush() throws IOException {
            if (chunkPtr > 0)
                flushChunk();

            dumpIO.force();
        }

        /**
         *
         */
        private void flushChunk() throws IOException {
            dumpIO.write(chunk, 0, chunkPtr);

            chunkPtr = 0;
        }

        /**
         *
         */
        void finish() {
            try {
                flush();

                dumpIO.close();
            }
            catch (IOException e) {
                throw new IgniteException("Dump finish failed", e);
            }

            assert dumpFile != null
                && dumpFile.exists()
                && dumpFile.getName().endsWith(NEW_FILE_EXT);

            String dumpFileName = dumpFile.getName();

            if (!dumpFile.renameTo(new File(dumpDir, dumpId + DUMP_FILE_EXT)))
                log.warning("Failed rename of dump file [name=" + dumpFileName + "]");
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            dumpIO.close();
        }
    }
}
