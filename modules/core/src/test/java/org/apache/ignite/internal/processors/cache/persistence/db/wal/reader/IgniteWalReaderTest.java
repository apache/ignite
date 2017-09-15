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

package org.apache.ignite.internal.processors.cache.persistence.db.wal.reader;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.xml.bind.DatatypeConverter;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.events.WalSegmentArchivedEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryCachingMetadataHandler;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.LazyDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.binary.GridCacheBinaryObjectsAbstractDataStreamerSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.apache.ignite.events.EventType.EVT_WAL_SEGMENT_ARCHIVED;
import static org.mockito.Mockito.when;

/**
 * Test suite for WAL segments reader and event generator.
 */
public class IgniteWalReaderTest extends GridCommonAbstractTest {
    /** Wal segments count */
    private static final int WAL_SEGMENTS = 10;

    /** Cache name. */
    private static final String CACHE_NAME = "cache0";

    /** Fill wal with some data before iterating. Should be true for non local run */
    private static final boolean fillWalBeforeTest = true;

    /** Delete DB dir before test. */
    private static final boolean deleteBefore = true;

    /** Delete DB dir after test. */
    private static final boolean deleteAfter = false;

    /** Dump records to logger. Should be false for non local run */
    private static final boolean dumpRecords = false;

    /** Page size to set */
    public static final int PAGE_SIZE = 4 * 1024;

    /**
     * Field for transferring setting from test to getConfig method
     * Archive incomplete segment after inactivity milliseconds.
     */
    private int archiveIncompleteSegmentAfterInactivityMs = 0;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(gridName);

        final CacheConfiguration<Integer, IgniteWalReaderTest.IndexedObject> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg.setIndexedTypes(Integer.class, IgniteWalReaderTest.IndexedObject.class);

        cfg.setCacheConfiguration(ccfg);

        cfg.setIncludeEventTypes(EventType.EVT_WAL_SEGMENT_ARCHIVED);

        final MemoryConfiguration dbCfg = new MemoryConfiguration();

        dbCfg.setPageSize(PAGE_SIZE);

        final MemoryPolicyConfiguration memPlcCfg = new MemoryPolicyConfiguration();

        memPlcCfg.setName("dfltMemPlc");
        memPlcCfg.setInitialSize(1024 * 1024 * 1024);
        memPlcCfg.setMaxSize(1024 * 1024 * 1024);

        dbCfg.setMemoryPolicies(memPlcCfg);
        dbCfg.setDefaultMemoryPolicyName("dfltMemPlc");

        cfg.setMemoryConfiguration(dbCfg);

        final PersistentStoreConfiguration pCfg = new PersistentStoreConfiguration();
        pCfg.setWalHistorySize(1);
        pCfg.setWalSegmentSize(1024 * 1024);
        pCfg.setWalSegments(WAL_SEGMENTS);
        pCfg.setWalMode(WALMode.BACKGROUND);

        if (archiveIncompleteSegmentAfterInactivityMs > 0)
            pCfg.setWalAutoArchiveAfterInactivity(archiveIncompleteSegmentAfterInactivityMs);

        cfg.setPersistentStoreConfiguration(pCfg);

        final BinaryConfiguration binCfg = new BinaryConfiguration();

        binCfg.setCompactFooter(false);

        cfg.setBinaryConfiguration(binCfg);
        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        stopAllGrids();

        if (deleteBefore)
            deleteWorkFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        if (deleteAfter)
            deleteWorkFiles();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void deleteWorkFiles() throws IgniteCheckedException {
        if (fillWalBeforeTest)
            deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false));
    }

    /**
     * @throws Exception if failed.
     */
    public void testFillWalAndReadRecords() throws Exception {
        final int cacheObjectsToWrite = 10000;

        if (fillWalBeforeTest) {
            final Ignite ignite0 = startGrid("node0");

            ignite0.active(true);

            putDummyRecords(ignite0, cacheObjectsToWrite);

            stopGrid("node0");
        }

        final File db = U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false);
        final File wal = new File(db, "wal");
        final File walArchive = new File(wal, "archive");
        final String consistentId = "127_0_0_1_47500";

        final FileIOFactory fileIOFactory = getConfiguration("").getPersistentStoreConfiguration().getFileIOFactory();
        final MockWalIteratorFactory mockItFactory = new MockWalIteratorFactory(log, fileIOFactory, PAGE_SIZE, consistentId, WAL_SEGMENTS);
        final WALIterator it = mockItFactory.iterator(wal, walArchive);
        final int cntUsingMockIter = iterateAndCount(it);

        log.info("Total records loaded " + cntUsingMockIter);
        assert cntUsingMockIter > 0;
        assert cntUsingMockIter > cacheObjectsToWrite;

        final File walArchiveDirWithConsistentId = new File(walArchive, consistentId);
        final File walWorkDirWithConsistentId = new File(wal, consistentId);

        final IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log, fileIOFactory, PAGE_SIZE);
        final int cntArchiveDir = iterateAndCount(factory.iteratorArchiveDirectory(walArchiveDirWithConsistentId));

        log.info("Total records loaded using directory : " + cntArchiveDir);

        final int cntArchiveFileByFile = iterateAndCount(
            factory.iteratorArchiveFiles(
                walArchiveDirWithConsistentId.listFiles(FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER)));

        log.info("Total records loaded using archive directory (file-by-file): " + cntArchiveFileByFile);

        assert cntArchiveFileByFile > cacheObjectsToWrite;
        assert cntArchiveDir > cacheObjectsToWrite;
        assert cntArchiveDir == cntArchiveFileByFile;
        //really count2 may be less because work dir correct loading is not supported yet
        assert cntUsingMockIter >= cntArchiveDir
            : "Mock based reader loaded " + cntUsingMockIter + " records but standalone has loaded only " + cntArchiveDir;

        final File[] workFiles = walWorkDirWithConsistentId.listFiles(FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER);
        int cntWork = 0;

        try (WALIterator stIt = factory.iteratorWorkFiles(workFiles)) {
            while (stIt.hasNextX()) {
                IgniteBiTuple<WALPointer, WALRecord> next = stIt.nextX();
                if (dumpRecords)
                    log.info("Work. Record: " + next.get2());
                cntWork++;
            }
        }
        log.info("Total records loaded from work: " + cntWork);

        assert cntWork + cntArchiveFileByFile == cntUsingMockIter
            : "Work iterator loaded [" + cntWork + "] " +
            "Archive iterator loaded [" + cntArchiveFileByFile + "]; " +
            "mock iterator [" + cntUsingMockIter + "]";
    }

    /**
     * @param walIter iterator to count, will be closed
     * @return count of records
     * @throws IgniteCheckedException if failed to iterate
     */
    private int iterateAndCount(WALIterator walIter) throws IgniteCheckedException {
        int cntUsingMockIter = 0;

        try (WALIterator it = walIter) {
            while (it.hasNextX()) {
                IgniteBiTuple<WALPointer, WALRecord> next = it.nextX();
                if (dumpRecords)
                    log.info("Record: " + next.get2());
                cntUsingMockIter++;
            }
        }
        return cntUsingMockIter;
    }

    /**
     * Tests archive completed event is fired
     *
     * @throws Exception if failed
     */
    public void testArchiveCompletedEventFired() throws Exception {
        final AtomicBoolean evtRecorded = new AtomicBoolean();

        final Ignite ignite = startGrid("node0");

        ignite.active(true);

        final IgniteEvents evts = ignite.events();

        if (!evts.isEnabled(EVT_WAL_SEGMENT_ARCHIVED))
            return; //nothing to test

        evts.localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event e) {
                WalSegmentArchivedEvent archComplEvt = (WalSegmentArchivedEvent)e;
                long idx = archComplEvt.getAbsWalSegmentIdx();
                log.info("Finished archive for segment [" + idx + ", " +
                    archComplEvt.getArchiveFile() + "]: [" + e + "]");

                evtRecorded.set(true);
                return true;
            }
        }, EVT_WAL_SEGMENT_ARCHIVED);

        putDummyRecords(ignite, 150);

        stopGrid("node0");
        assert evtRecorded.get();
    }

    /**
     * Puts provided number of records to fill WAL
     *
     * @param ignite ignite instance
     * @param recordsToWrite count
     */
    private void putDummyRecords(Ignite ignite, int recordsToWrite) {
        IgniteCache<Object, Object> cache0 = ignite.cache(CACHE_NAME);

        for (int i = 0; i < recordsToWrite; i++)
            cache0.put(i, new IndexedObject(i));
    }

    /**
     * Puts provided number of records to fill WAL under transactions
     *
     * @param ignite ignite instance
     * @param recordsToWrite count
     * @param txCnt transactions to run. If number is less then records count, txCnt records will be written
     */
    private void txPutDummyRecords(Ignite ignite, int recordsToWrite, int txCnt) {
        IgniteCache<Object, Object> cache0 = ignite.cache(CACHE_NAME);
        int keysPerTx = recordsToWrite / txCnt;
        if (keysPerTx == 0)
            keysPerTx = 1;
        for (int t = 0; t < txCnt; t++) {
            try (Transaction tx = ignite.transactions().txStart()) {
                for (int i = t * keysPerTx; i < (t + 1) * keysPerTx; i++)
                    cache0.put(i, new IndexedObject(i));

                tx.commit();
            }
        }
    }

    /**
     * Tests time out based WAL segment archiving
     *
     * @throws Exception if failure occurs
     */
    public void testArchiveIncompleteSegmentAfterInactivity() throws Exception {
        final AtomicBoolean waitingForEvt = new AtomicBoolean();
        final CountDownLatch archiveSegmentForInactivity = new CountDownLatch(1);

        archiveIncompleteSegmentAfterInactivityMs = 1000;

        final Ignite ignite = startGrid("node0");

        ignite.active(true);

        final IgniteEvents evts = ignite.events();

        evts.localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event e) {
                WalSegmentArchivedEvent archComplEvt = (WalSegmentArchivedEvent)e;
                long idx = archComplEvt.getAbsWalSegmentIdx();
                log.info("Finished archive for segment [" + idx + ", " +
                    archComplEvt.getArchiveFile() + "]: [" + e + "]");

                if (waitingForEvt.get())
                    archiveSegmentForInactivity.countDown();
                return true;
            }
        }, EVT_WAL_SEGMENT_ARCHIVED);

        putDummyRecords(ignite, 100);
        waitingForEvt.set(true); //flag for skipping regular log() and rollOver()

        log.info("Wait for archiving segment for inactive grid started");

        boolean recordedAfterSleep =
            archiveSegmentForInactivity.await(archiveIncompleteSegmentAfterInactivityMs + 1001, TimeUnit.MILLISECONDS);

        stopGrid("node0");
        assert recordedAfterSleep;
    }

    /**
     * @throws Exception if failed.
     */
    public void testTxFillWalAndExtractDataRecords() throws Exception {
        final int cacheObjectsToWrite = 100;
        final int txCnt = 100;

        if (fillWalBeforeTest) {
            final Ignite ignite0 = startGrid("node0");

            ignite0.active(true);

            txPutDummyRecords(ignite0, cacheObjectsToWrite, txCnt);

            stopGrid("node0");
        }

        final File db = U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false);
        final File wal = new File(db, "wal");
        final File walArchive = new File(wal, "archive");
        final String consistentId = "127_0_0_1_47500";

        final FileIOFactory fileIOFactory = getConfiguration("").getPersistentStoreConfiguration().getFileIOFactory();

        final File walArchiveDirWithConsistentId = new File(walArchive, consistentId);
        final File walWorkDirWithConsistentId = new File(wal, consistentId);

        final IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log, fileIOFactory, PAGE_SIZE);
        final WALIterator iter = factory.iteratorArchiveFiles(
            walArchiveDirWithConsistentId.listFiles(FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER));
        final int cntArchiveFileByFile = iterateAndCountLogical(iter);

        log.info("Total records loaded using archive directory (file-by-file): " + cntArchiveFileByFile);

        final File[] workFiles = walWorkDirWithConsistentId.listFiles(FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER);

        WALIterator tuples = factory.iteratorWorkFiles(workFiles);
        int cntWork = iterateAndCountLogical(tuples);
        log.info("Total records loaded from work: " + cntWork);

    }

    private int iterateAndCountLogical(WALIterator walIterator) throws IgniteCheckedException {
        int cntLogical = 0;

        try (WALIterator stIt = walIterator) {
            while (stIt.hasNextX()) {
                final IgniteBiTuple<WALPointer, WALRecord> next = stIt.nextX();
                final WALRecord walRecord = next.get2();

                if (walRecord.type() == WALRecord.RecordType.DATA_RECORD && walRecord instanceof DataRecord) {
                    final DataRecord dataRecord = (DataRecord)walRecord;
                    final List<DataEntry> entries = dataRecord.writeEntries();

                    for (DataEntry entry : entries) {
                        final GridCacheVersion globalTxId = entry.nearXidVersion();
                        log.info("//Entry operation " + entry.op() + "; cache Id" + entry.cacheId() + "; " +
                            "under transaction: " + globalTxId + "; entry " + entry);

                        final GridKernalContext ctx = new StandaloneGridKernalContext(log);

                        if (entry instanceof LazyDataEntry) {
                            final LazyDataEntry lazyDataEntry = (LazyDataEntry)entry;
                            final CacheObjectBinaryProcessorImpl processor = new CacheObjectBinaryProcessorImpl(ctx);
                            final byte[] bytes = lazyDataEntry.getValBytes();
                            final byte type = lazyDataEntry.getValType();

                            log.info("Entry value binary:" + DatatypeConverter.printHexBinary(bytes));
                            log.info("Entry type:" + type);

                            final BinaryContext bctx = new BinaryContext(BinaryCachingMetadataHandler.create(), ctx.config(), new NullLogger()) {
                                @Override public int typeId(String typeName) {
                                    return typeName.hashCode();
                                }
                            };
                            final CacheObject val = toCacheObject(ctx, processor, bctx, bytes, type);

                            log.info("Entry value:" + val);
                        }
                    }
                    cntLogical++;
                }
                else if (walRecord.type() == WALRecord.RecordType.TX_RECORD && walRecord instanceof TxRecord) {
                    final TxRecord txRecord = (TxRecord)walRecord;
                    final GridCacheVersion globalTxId = txRecord.nearXidVersion();

                    log.info("//Tx Record, action: " + txRecord.action() +
                        "; nearTxVersion" + globalTxId);
                }
            }
        }
        return cntLogical;
    }

    public void testParseBinary() throws IgniteCheckedException {
        final GridKernalContext ctx = new StandaloneGridKernalContext(log) {
            @Override public GridDiscoveryManager discovery() {
                GridDiscoveryManager mock = Mockito.mock(GridDiscoveryManager.class);


                //todo set consistent ID, work directory from outside
                when(mock.consistentId()).thenReturn("127_0_0_1_47501");
                return mock;
            }

            @Override public GridIoManager io() {
                return Mockito.mock(GridIoManager.class);
            }
        };

        CacheObjectBinaryProcessorImpl processor = new CacheObjectBinaryProcessorImpl(ctx);
        processor.start();


        IgniteConfiguration cfg = ctx.config();

        BinaryContext bctx3 = new BinaryContext(BinaryCachingMetadataHandler.create(), cfg, new NullLogger()) {
            @Override public int typeId(String typeName) {
                return typeName.hashCode();
            }
        };

        BinaryContext bctx = processor.binaryContext();
        byte[] bytes = DatatypeConverter.parseHexBinary(
            "67010B00055724EC162AF496540000005F121E934A00000003000000000C280000004142434445464748494A4142434445464748494A4142434445464748494A4142434445464748494A1882310018AAEF2E001D");
        byte type = 100;
        log.info("Entry value binary:" + DatatypeConverter.printHexBinary(bytes));

        log.info("Entry type:" + type);

        CacheObject obj = toCacheObject(ctx, processor, bctx, bytes, type);
        log.info("Cache Object: " + obj);
        byte b = obj.fieldsCount();
        log.info("Cache Object fields: " + b);
        BinaryObject binaryObject = (BinaryObject)obj;
        int id = binaryObject.type().typeId();


        log.info("Cache Object type id: " + id);
        log.info("IndexedObjec hash code: " + IndexedObject.class.getName().hashCode());
        //assert IndexedObject.class.getName().hashCode() == id;
        String s = binaryObject.type().typeName();

        log.info("Cache Object type name: " + s);
    }

    private CacheObject toCacheObject(GridKernalContext ctx, CacheObjectBinaryProcessorImpl processor,
        BinaryContext bctx,
        byte[] bytes, byte type) {
       /* if (type == BinaryObjectImpl.TYPE_BINARY)
            return new BinaryObjectImpl(bctx, bytes, 0);
        else if (type == BinaryObjectImpl.TYPE_BINARY_ENUM)
            return new BinaryEnumObjectImpl(bctx, bytes);
        else
                */
       return processor.toCacheObject(null,
                type,
                bytes);
    }

    /** Test object for placing into grid in this test */
    private static class IndexedObject {
        /** */
        @QuerySqlField(index = true)
        private int iVal;

        /** Data filled with recognizable pattern */
        private byte[] data;

        /**
         * @param iVal Integer value.
         */
        private IndexedObject(int iVal) {
            this.iVal = iVal;
            int sz = 40;
            data = new byte[sz];
            for (int i = 0; i < sz; i++)
                data[i] = (byte)('A' + (i % 10));
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            IndexedObject obj = (IndexedObject)o;

            if (iVal != obj.iVal)
                return false;
            return Arrays.equals(data, obj.data);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = iVal;
            res = 31 * res + Arrays.hashCode(data);
            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IgniteWalReaderTest.IndexedObject.class, this);
        }
    }
}
