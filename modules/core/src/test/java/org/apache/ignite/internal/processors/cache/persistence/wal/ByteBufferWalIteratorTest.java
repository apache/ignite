/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.RolloverType;
import org.apache.ignite.internal.pagemem.wal.record.SnapshotRecord;
import org.apache.ignite.internal.pagemem.wal.record.TimeStampRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactoryImpl;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.wal.record.RecordUtils;
import org.apache.ignite.testframework.wal.record.UnsupportedWalRecord;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 *
 */
public class ByteBufferWalIteratorTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = GridCommonAbstractTest.DEFAULT_CACHE_NAME;

    /** */
    private IgniteEx ig;

    /** */
    private GridCacheSharedContext<Object, Object> sharedCtx;

    /** */
    private GridCacheContext<Object, Object> cctx;

    /** */
    private RecordSerializer serializer;

    /** */
    private int idx;

    /** */
    private @Nullable IgniteInternalCache<Object, Object> cache;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        idx = new Random().nextInt();

        ig = startGrid(0);

        ig.cluster().state(ClusterState.ACTIVE);

        sharedCtx = ig.context().cache().context();

        cache = sharedCtx.cache().cache(CACHE_NAME);

        cctx = cache.context();

        RecordSerializerFactory serializerFactory = new RecordSerializerFactoryImpl(sharedCtx);

        serializer = serializerFactory.createSerializer(RecordSerializerFactory.LATEST_SERIALIZER_VERSION);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();

        cleanPersistenceDir();
    }

    /** */
    private void writeRecord(ByteBuffer byteBuf,
        WALRecord walRecord) throws IgniteCheckedException {
        log.info("Writing " + walRecord.type());

        int segment = idx;

        int fileOff = byteBuf.position();

        int size = serializer.size(walRecord);

        walRecord.size(size);

        WALPointer walPointer = new WALPointer(segment, fileOff, size);

        walRecord.position(walPointer);

        serializer.writeRecord(walRecord, byteBuf);
    }

    /** */
    private static boolean dataEntriesEqual(DataEntry x, DataEntry y) {
        if (x == y)
            return true;

        if (x == null || y == null)
            return false;

        return x.cacheId() == y.cacheId()
            && x.op() == y.op()
            && Objects.equals(x.key(), y.key());
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(new CacheConfiguration<>(CACHE_NAME));

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        return cfg;
    }

    /** */
    @Test
    public void testDataRecordsRead() throws Exception {
        ByteBuffer byteBuf = ByteBuffer.allocate(1024 * 1024).order(ByteOrder.nativeOrder());

        final int cnt = 10;

        List<DataEntry> entries = generateEntries(cctx, cnt);

        for (int i = 0; i < entries.size(); i++) {
            writeRecord(byteBuf, new DataRecord(entries.get(i)));
        }

        byteBuf.flip();

        WALIterator walIter = new ByteBufferWalIterator(log, sharedCtx, byteBuf, RecordSerializerFactory.LATEST_SERIALIZER_VERSION);

        Iterator<DataEntry> dataEntriesIter = entries.iterator();

        while (walIter.hasNext()) {
            assertTrue(dataEntriesIter.hasNext());

            WALRecord record = walIter.next().get2();

            assertTrue(record instanceof DataRecord);

            DataEntry dataEntry = dataEntriesIter.next();

            assertTrue(dataEntriesEqual(
                ((DataRecord)record).get(0),
                dataEntry));
        }

        assertFalse(dataEntriesIter.hasNext());
    }

    /** */
    @Test
    public void testWalRecordsRead() throws Exception {
        ByteBuffer byteBuf = ByteBuffer.allocate(1024 * 1024).order(ByteOrder.nativeOrder());

        List<WALRecord> records = Arrays.stream(WALRecord.RecordType.values())
            .filter(t -> t != WALRecord.RecordType.SWITCH_SEGMENT_RECORD)
            .map(RecordUtils::buildWalRecord)
            .filter(Objects::nonNull)
            .filter(r -> !(r instanceof UnsupportedWalRecord))
            .collect(Collectors.toList());

        final int cnt = records.size();

        for (WALRecord record : records)
            writeRecord(byteBuf, record);

        byteBuf.flip();

        WALIterator walIter = new ByteBufferWalIterator(log, sharedCtx, byteBuf, RecordSerializerFactory.LATEST_SERIALIZER_VERSION);

        Iterator<WALRecord> recordsIter = records.iterator();

        while (walIter.hasNext()) {
            assertTrue(recordsIter.hasNext());

            WALRecord actualRec = walIter.next().get2();

            WALRecord expectedRec = recordsIter.next();

            assertTrue("Records of type " + expectedRec.type() + " are different:\n" +
                    "\tExpected:\t" + expectedRec + "\n" +
                    "\tActual  :\t" + actualRec,
                recordsEqual(
                    expectedRec,
                    actualRec));
        }

        assertFalse(recordsIter.hasNext());
    }

    /** */
    private boolean recordsEqual(WALRecord x, WALRecord y) {
        if (x == y)
            return true;

        if (x == null || y == null)
            return false;

        log.info("Comparing " + x.type() + " and " + y.type());

        return Objects.equals(x.type(), y.type())
            && Objects.equals(x.position(), y.position())
            && x.size() == y.size()
            && (x instanceof TimeStampRecord ? ((TimeStampRecord)x).timestamp() == ((TimeStampRecord)y).timestamp() : true);
    }

    /** */
    @Test
    public void testReadFiltered() throws Exception {
        ByteBuffer byteBuf = ByteBuffer.allocate(1024 * 1024).order(ByteOrder.nativeOrder());

        List<WALRecord> physicalRecords = Arrays.stream(WALRecord.RecordType.values())
            .filter(t -> t.purpose() == WALRecord.RecordPurpose.PHYSICAL)
            .map(RecordUtils::buildWalRecord)
            .filter(Objects::nonNull)
            .filter(r -> !(r instanceof UnsupportedWalRecord))
            .collect(Collectors.toList());

        final int cnt = physicalRecords.size();

        List<DataEntry> entries = generateEntries(cctx, cnt);

        for (int i = 0; i < entries.size(); i++) {
            writeRecord(byteBuf, new DataRecord(entries.get(i)));

            writeRecord(byteBuf, physicalRecords.get(i));
        }

        byteBuf.flip();

        WALIterator walIter = new ByteBufferWalIterator(log, sharedCtx, byteBuf,
            RecordSerializerFactory.LATEST_SERIALIZER_VERSION,
            (t, p) -> t.purpose() == WALRecord.RecordPurpose.LOGICAL);

        Iterator<DataEntry> dataEntriesIter = entries.iterator();

        while (walIter.hasNext()) {
            assertTrue(dataEntriesIter.hasNext());

            WALRecord record = walIter.next().get2();

            assertTrue(record instanceof DataRecord);

            DataEntry dataEntry = dataEntriesIter.next();

            assertTrue(dataEntriesEqual(
                ((DataRecord)record).get(0),
                dataEntry));
        }

        assertFalse(dataEntriesIter.hasNext());
    }

    /** */
    @NotNull private List<DataEntry> generateEntries(GridCacheContext<Object, Object> cctx, int cnt) {
        List<DataEntry> entries = new ArrayList<>(cnt);

        for (int i = 0; i < cnt; i++) {
            GridCacheOperation op = i % 2 == 0 ? GridCacheOperation.UPDATE : GridCacheOperation.DELETE;

            KeyCacheObject key = cctx.toCacheKeyObject(i);

            CacheObject val = null;

            if (op != GridCacheOperation.DELETE)
                val = cctx.toCacheObject("value-" + i);

            entries.add(
                new DataEntry(cctx.cacheId(), key, val, op, null, cctx.cache().nextVersion(),
                    0L,
                    cctx.affinity().partition(i), i, DataEntry.EMPTY_FLAGS));
        }
        return entries;
    }

    /** */
    @Test
    public void testBrokenTail() throws Exception {
        ByteBuffer byteBuf = ByteBuffer.allocate(1024 * 1024).order(ByteOrder.nativeOrder());

        List<DataEntry> entries = generateEntries(cctx, 3);

        for (int i = 0; i < 2; i++)
            writeRecord(byteBuf, new DataRecord(entries.get(i)));

        int position1 = byteBuf.position();

        writeRecord(byteBuf, new DataRecord(entries.get(2)));

        int position2 = byteBuf.position();

        byteBuf.flip();

        byteBuf.limit((position1 + position2) >> 1);

        WALIterator walIter = new ByteBufferWalIterator(log, sharedCtx, byteBuf, RecordSerializerFactory.LATEST_SERIALIZER_VERSION);

        assertTrue(walIter.hasNext());

        walIter.next();

        assertTrue(walIter.hasNext());

        walIter.next();

        try {
            walIter.hasNext();

            fail("hasNext() expected to fail");
        }
        catch (IgniteException e) {
            assertTrue(X.hasCause(e, IOException.class));
        }
    }

    /** */
    @Test
    public void testEmptyBuffer() throws Exception {
        ByteBuffer byteBuf = ByteBuffer.allocate(1024 * 1024).order(ByteOrder.nativeOrder());

        byteBuf.flip();

        WALIterator walIter = new ByteBufferWalIterator(log, sharedCtx, byteBuf, RecordSerializerFactory.LATEST_SERIALIZER_VERSION);

        assertFalse(walIter.hasNext());

        try {
            walIter.next();

            fail("next() expected to fail");
        }
        catch (NoSuchElementException e) {
            // This is expected.
        }
    }

    /** */
    @Test
    public void testWalSegmentReadFromDisk() throws Exception {
        FileDescriptor[] archiveFiles = generateWalFiles(20, 10_000);

        for (int i = 0; i < archiveFiles.length; i++)
            checkIteratorFromDisk(archiveFiles[i]);
    }

    /** */
    private void checkIteratorFromDisk(FileDescriptor fd) throws IOException, IgniteCheckedException {
        log.info("Checking " + fd.file());

        ByteBuffer byteBuf = loadFile(fd);

        checkByteBuffer(byteBuf);
    }

    /** */
    private void checkByteBuffer(ByteBuffer byteBuf) throws IgniteCheckedException {
        log.info("Bytes count " + byteBuf.limit());

        int p0 = byteBuf.position();

        int shift = -1;

        ByteBufferWalIterator walIterator = new ByteBufferWalIterator(log, sharedCtx, byteBuf,
            RecordSerializerFactory.LATEST_SERIALIZER_VERSION);

        Map<WALRecord.RecordType, Integer> counts = new TreeMap<>();

        while (walIterator.hasNext()) {
            int p1 = byteBuf.position();

            IgniteBiTuple<WALPointer, WALRecord> next = walIterator.next();

            if (log.isDebugEnabled())
                log.debug("Got " + next.get2().type() + " at " + next.get1());

            if (shift >= 0)
                assertEquals("WalPointer offset check failed", p0 + shift, next.get1().fileOffset());
            else
                shift = next.get1().fileOffset() - p0;

            assertEquals("WalPointer length check failed", p1 - p0, next.get1().length());

            assertEquals("WalPointers comparison failed", next.get1(), next.get2().position());

            assertEquals("WalPointers length comparison failed", next.get1().length(), next.get2().position().length());

            p0 = p1;

            counts.merge(next.get2().type(), 1, (x, y) -> x + y);

            assertTrue(next != null);
        }

        assertFalse("ByteBuffer has some unprocessed bytes", byteBuf.hasRemaining());

        printStats(counts);
    }

    /** */
    private void printStats(Map<WALRecord.RecordType, Integer> counts) {
        if (counts.isEmpty()) {
            log.info("No record");
            return;
        }

        ArrayList<WALRecord.RecordType> types = new ArrayList<>(counts.keySet());

        types.sort((x, y) -> -counts.get(x).compareTo(counts.get(y)));

        int len = types.stream().map(x -> x.toString().length()).max(Integer::compare).get();

        char[] spaces = new char[len];

        Arrays.fill(spaces, ' ');

        StringBuilder sb = new StringBuilder("Statistics:");

        types.forEach(x -> sb.append("\n\t")
            .append(x)
            .append(spaces, 0, len - x.toString().length())
            .append("\t")
            .append(counts.get(x)));

        log.info(sb.toString());
    }

    /** */
    @NotNull private ByteBuffer loadFile(FileDescriptor fd) throws IOException {
        File file = fd.file();

        int size = (int)file.length();

        FileInputStream fileInputStream = new FileInputStream(file);

        final byte[] bytes = new byte[size];

        int length = fileInputStream.read(bytes);

        assertTrue(length == size);

        ByteBuffer byteBuf = ByteBuffer.wrap(bytes);

        byteBuf.order(ByteOrder.nativeOrder());

        return byteBuf;
    }

    /** */
    private FileDescriptor[] generateWalFiles(int files, int size) throws IgniteCheckedException {
        Random random = new Random();

        IgniteCacheDatabaseSharedManager sharedMgr = ig.context().cache().context().database();

        IgniteWriteAheadLogManager walMgr = ig.context().cache().context().wal();

        for (int fileNo = 0; fileNo < files; fileNo++) {
            for (int i = 0; i < size; i++) {
                switch (random.nextInt(2)) {
                    case 0:
                        cache.put(random.nextInt(100), "Cache value " + random.nextInt());
                        break;
                    case 1:
                        cache.remove(random.nextInt(100));
                        break;
                }
            }

            sharedMgr.checkpointReadLock();

            try {
                walMgr.log(new SnapshotRecord(fileNo, false), RolloverType.NEXT_SEGMENT);
            }
            finally {
                sharedMgr.checkpointReadUnlock();
            }
        }

        while (true) {
            FileDescriptor[] archiveFiles = ((FileWriteAheadLogManager)walMgr).walArchiveFiles();

            if (archiveFiles.length >= files)
                return archiveFiles;

            LockSupport.parkNanos(10_000_000);
        }
    }

    /** */
    @Test
    public void testPartialWalSegmentReadFromDisk() throws Exception {
        FileDescriptor[] archiveFiles = generateWalFiles(1, 100);

        for (int i = 0; i < archiveFiles.length; i++)
            checkPartialIteratorFromDisk(archiveFiles[i]);
    }

    /** */
    private void checkPartialIteratorFromDisk(FileDescriptor fd) throws IOException, IgniteCheckedException {
        log.info("Checking " + fd.file());

        ByteBuffer byteBuf = loadFile(fd);

        log.info("Bytes count " + byteBuf.limit());

        List<Integer> positions = new ArrayList<>();

        positions.add(byteBuf.position());

        ByteBufferWalIterator walIterator = new ByteBufferWalIterator(log, sharedCtx, byteBuf,
            RecordSerializerFactory.LATEST_SERIALIZER_VERSION);

        positions.add(byteBuf.position());

        positions.addAll(
            StreamSupport.stream(walIterator.spliterator(), false)
                .map(x -> byteBuf.position())
                .collect(Collectors.toList()));

        Random random = new Random();

        int size = positions.size();

        assertTrue("Size shouild be at least 10 for this test", size >= 10);

        int n1 = (int)((0.1 + 0.4 * random.nextDouble()) * size);

        int n2 = (int)((0.5 + 0.4 * random.nextDouble()) * size);

        // With header.
        checkByteBufferPart(byteBuf, positions, 0, n1);

        // Middle part.
        checkByteBufferPart(byteBuf, positions, n1, n2);

        // Empty buffer.
        checkByteBufferPart(byteBuf, positions, n2, n2);

        // With tail.
        checkByteBufferPart(byteBuf, positions, n2, size - 1);
    }

    /** */
    private void checkByteBufferPart(ByteBuffer byteBuf, List<Integer> positions, int fromRec, int toRec)
        throws IgniteCheckedException {
        int fromPos = positions.get(fromRec);

        int toPos = positions.get(toRec);

        log.info(("Checking ByteBuffer from " + fromRec + "(" + fromPos + ") to " + toRec + "(" + toPos + ")"));

        int len = toPos - fromPos;

        byteBuf.position(fromPos).limit(toPos);

        byte[] array = byteBuf.array();

        byteBuf = ByteBuffer.allocate(len).order(ByteOrder.nativeOrder());

        System.arraycopy(array, fromPos, byteBuf.array(), 0, len);

        checkByteBuffer(byteBuf);
    }
}
