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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.binary.BinaryObject;
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
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_WAL_SEGMENT_ARCHIVED;

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
    private static final boolean deleteAfter = true;

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

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
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

        final String consistentId;
        if (fillWalBeforeTest) {
            final Ignite ignite0 = startGrid("node0");

            ignite0.active(true);

            consistentId = U.maskForFileName(ignite0.cluster().localNode().consistentId().toString());

            putDummyRecords(ignite0, cacheObjectsToWrite);

            stopGrid("node0");
        }
        else
            consistentId = "127_0_0_1_47500";

        final String workDir = U.defaultWorkDirectory();
        final File db = U.resolveWorkDirectory(workDir, "db", false);
        final File wal = new File(db, "wal");
        final File walArchive = new File(wal, "archive");


        final MockWalIteratorFactory mockItFactory = new MockWalIteratorFactory(log, PAGE_SIZE, consistentId, WAL_SEGMENTS);
        final WALIterator it = mockItFactory.iterator(wal, walArchive);
        final int cntUsingMockIter = iterateAndCount(it, false);

        log.info("Total records loaded " + cntUsingMockIter);
        assert cntUsingMockIter > 0;
        assert cntUsingMockIter > cacheObjectsToWrite;

        final File walArchiveDirWithConsistentId = new File(walArchive, consistentId);
        final File walWorkDirWithConsistentId = new File(wal, consistentId);

        final File binaryMeta = U.resolveWorkDirectory(workDir, "binary_meta", false);
        final File binaryMetaWithConsId = new File(binaryMeta, consistentId);

        final IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log, PAGE_SIZE, binaryMetaWithConsId);
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

        final int cntWork = iterateAndCount(factory.iteratorWorkFiles(workFiles));

        log.info("Total records loaded from work: " + cntWork);

        assert cntWork + cntArchiveFileByFile == cntUsingMockIter
            : "Work iterator loaded [" + cntWork + "] " +
            "Archive iterator loaded [" + cntArchiveFileByFile + "]; " +
            "mock iterator [" + cntUsingMockIter + "]";
    }

    /**
     * Iterates on records and closes iterator
     *
     * @param walIter iterator to count, will be closed
     * @return count of records
     * @throws IgniteCheckedException if failed to iterate
     */
    private int iterateAndCount(WALIterator walIter) throws IgniteCheckedException {
        return iterateAndCount(walIter, true);
    }

    /**
     * Iterates on records and closes iterator
     *
     * @param walIter iterator to count, will be closed
     * @param touchEntries access data within entries
     *
     * @return count of records
     * @throws IgniteCheckedException if failed to iterate
     */
    private int iterateAndCount(WALIterator walIter, boolean touchEntries) throws IgniteCheckedException {
        int cnt = 0;

        try (WALIterator it = walIter) {
            while (it.hasNextX()) {
                final IgniteBiTuple<WALPointer, WALRecord> next = it.nextX();
                final WALRecord walRecord = next.get2();
                if (touchEntries && walRecord.type() == WALRecord.RecordType.DATA_RECORD) {
                    final DataRecord record = (DataRecord)walRecord;
                    for (DataEntry entry : record.writeEntries()) {
                        final KeyCacheObject key = entry.key();
                        final CacheObject val = entry.value();
                        if (dumpRecords)
                            log.info("Op: " + entry.op() + ", Key: " + key + ", Value: " + val);
                    }
                }
                if (dumpRecords)
                    log.info("Record: " + walRecord);
                cnt++;
            }
        }
        return cnt;
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

        putDummyRecords(ignite, 500);

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
        final int cntEntries = 1000;
        final int txCnt = 100;

        final String consistentId;
        if (fillWalBeforeTest) {
            final Ignite ignite0 = startGrid("node0");

            ignite0.active(true);

            txPutDummyRecords(ignite0, cntEntries, txCnt);

            consistentId = U.maskForFileName(ignite0.cluster().localNode().consistentId().toString());

            stopGrid("node0");
        } else
            consistentId = "127_0_0_1_47500";

        final String workDir = U.defaultWorkDirectory();
        final File db = U.resolveWorkDirectory(workDir, "db", false);
        final File wal = new File(db, "wal");
        final File walArchive = new File(wal, "archive");

        final File binaryMeta = U.resolveWorkDirectory(workDir, "binary_meta", false);
        final File binaryMetaWithConsId = new File(binaryMeta, consistentId);

        final File walArchiveDirWithConsistentId = new File(walArchive, consistentId);
        final File walWorkDirWithConsistentId = new File(wal, consistentId);

        final IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log, PAGE_SIZE, binaryMetaWithConsId);
        final File[] files = walArchiveDirWithConsistentId.listFiles(FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER);

        assert files != null : "Can't iterate over files [" + walArchiveDirWithConsistentId + "] Directory is N/A";
        final WALIterator iter = factory.iteratorArchiveFiles(files);
        final Consumer<BinaryObject> objConsumer = new Consumer<BinaryObject>() {
            @Override public void accept(BinaryObject binaryObj) {
                assertEquals(binaryObj.field("iVal").toString(),
                    binaryObj.field("jVal").toString());

                assertEquals(IndexedObject.class.getName(), binaryObj.type().typeName());

                byte data[] = binaryObj.field("data");
                for (byte datum : data) {
                    assert datum >= 'A' && datum <= 'A' + 10;
                }
            }
        };
        final Map<GridCacheVersion, Integer> cntArch = iterateAndCountDataRecord(iter, objConsumer);

        int txCntObservedArch = cntArch.size();
        if (cntArch.containsKey(null))
            txCntObservedArch -= 1; // exclude non transactional updates
        final int entriesArch = valuesSum(cntArch.values());

        log.info("Total tx found loaded using archive directory (file-by-file): " + txCntObservedArch);

        final File[] workFiles = walWorkDirWithConsistentId.listFiles(FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER);

        final WALIterator tuples = factory.iteratorWorkFiles(workFiles);
        final Map<GridCacheVersion, Integer> cntWork = iterateAndCountDataRecord(tuples, objConsumer);
        int txCntObservedWork = cntWork.size();
        if (cntWork.containsKey(null))
            txCntObservedWork -= 1; // exclude non transactional updates

        final int entriesWork = valuesSum(cntWork.values());
        log.info("Archive directory: Tx found " + txCntObservedWork + " entries " + entriesWork);

        assert entriesArch + entriesWork >= cntEntries;
        assert txCntObservedWork + txCntObservedArch >= txCnt;
    }

    /**
     * @param values collection with numbers
     * @return sum of numbers
     */
    private int valuesSum(Iterable<Integer> values) {
        int sum = 0;
        for (Integer next : values) {
            if (next != null)
                sum += next;
        }
        return sum;
    }

    /**
     * Iterates over data record, checks DataRecord and its entries, finds out all transactions in WAL
     *
     * @param walIter iterator to use
     * @return count of data records observed for each global TX ID. Contains null for non tx updates
     * @throws IgniteCheckedException if failure
     */
    private Map<GridCacheVersion, Integer> iterateAndCountDataRecord(
        final WALIterator walIter,
        @Nullable final Consumer<BinaryObject> dataEntryObjHnd) throws IgniteCheckedException {

        final Map<GridCacheVersion, Integer> entriesUnderTxFound = new HashMap<>();

        try (WALIterator stIt = walIter) {
            while (stIt.hasNextX()) {
                final IgniteBiTuple<WALPointer, WALRecord> next = stIt.nextX();
                final WALRecord walRecord = next.get2();

                if (walRecord.type() == WALRecord.RecordType.DATA_RECORD && walRecord instanceof DataRecord) {
                    final DataRecord dataRecord = (DataRecord)walRecord;
                    final List<DataEntry> entries = dataRecord.writeEntries();

                    for (DataEntry entry : entries) {
                        final GridCacheVersion globalTxId = entry.nearXidVersion();

                        final CacheObject val = entry.value();

                        final KeyCacheObject key = entry.key();

                        log.info("//Entry operation " + entry.op() + "; cache Id" + entry.cacheId() + "; " +
                            "under transaction: " + globalTxId + "; entry " + entry +
                            "; Key: " + key +
                            "; Value: " + val);

                        if (val instanceof BinaryObject) {
                            final BinaryObject binaryObj = (BinaryObject)val;

                            if (dataEntryObjHnd != null)
                                dataEntryObjHnd.accept(binaryObj);
                        }

                        final Integer entriesUnderTx = entriesUnderTxFound.get(globalTxId);

                        entriesUnderTxFound.put(globalTxId, entriesUnderTx == null ? 1 : entriesUnderTx + 1);
                    }
                }
                else if (walRecord.type() == WALRecord.RecordType.TX_RECORD && walRecord instanceof TxRecord) {
                    final TxRecord txRecord = (TxRecord)walRecord;
                    final GridCacheVersion globalTxId = txRecord.nearXidVersion();

                    log.info("//Tx Record, action: " + txRecord.action() +
                        "; nearTxVersion" + globalTxId);
                }
            }
        }
        return entriesUnderTxFound;
    }

    /**
     * Represents an operation that accepts a single input argument and returns no
     * result.
     * @param <T>
     */
    private interface Consumer<T> {
        /**
         * Performs this operation on the given argument.
         *
         * @param t the input argument
         */
        public void accept(T t);
    }

    /** Test object for placing into grid in this test */
    private static class IndexedObject {
        /** I value. */
        @QuerySqlField(index = true)
        private int iVal;

        /** J value = I value. */
        @QuerySqlField(index = true)
        private int jVal;

        /** Data filled with recognizable pattern */
        private byte[] data;

        /**
         * @param iVal Integer value.
         */
        private IndexedObject(int iVal) {
            this.iVal = iVal;
            this.jVal = iVal;
            int sz = 1024;
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
