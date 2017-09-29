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

import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
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
import org.apache.ignite.internal.pagemem.wal.record.LazyDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.UnwrapDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_WAL_SEGMENT_ARCHIVED;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 * Test suite for WAL segments reader and event generator.
 */
public class IgniteWalReaderTest extends GridCommonAbstractTest {
    /** Wal segments count */
    private static final int WAL_SEGMENTS = 10;

    /** Cache name. */
    private static final String CACHE_NAME = "cache0";

    /** additional cache for testing different combinations of types in WAL */
    private static final String CACHE_ADDL_NAME = "cache1";

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
    private int archiveIncompleteSegmentAfterInactivityMs;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(gridName);

        final CacheConfiguration<Integer, IndexedObject> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg.setIndexedTypes(Integer.class, IndexedObject.class);

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
            deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
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
        final File db = U.resolveWorkDirectory(workDir, DFLT_STORE_DIR, false);
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
        final File marshaller = U.resolveWorkDirectory(workDir, "marshaller", false);

        final IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log, PAGE_SIZE, binaryMetaWithConsId, marshaller);
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
    private IgniteCache<Object, Object> txPutDummyRecords(Ignite ignite, int recordsToWrite, int txCnt) {
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
        return cache0;
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
     * Removes entry by key and value from map (java 8 map method copy)
     *
     * @param m map to remove from.
     * @param key key to remove.
     * @param val value to remove.
     * @return true if remove was successful
     */
    private boolean remove(Map m, Object key, Object val) {
        Object curVal = m.get(key);
        if (!Objects.equals(curVal, val) ||
            (curVal == null && !m.containsKey(key)))
            return false;
        m.remove(key);
        return true;
    }

    /**
     * Places records under transaction, checks its value using WAL
     *
     * @throws Exception if failed.
     */
    public void testTxFillWalAndExtractDataRecords() throws Exception {
        final int cntEntries = 1000;
        final int txCnt = 100;

        final Map<Object, Object> ctrlMap = new HashMap<>();
        final String consistentId;
        if (fillWalBeforeTest) {
            final Ignite ignite0 = startGrid("node0");

            ignite0.active(true);

            final IgniteCache<Object, Object> entries = txPutDummyRecords(ignite0, cntEntries, txCnt);

            for (Cache.Entry<Object, Object> next : entries) {
                ctrlMap.put(next.getKey(), next.getValue());
            }

            consistentId = U.maskForFileName(ignite0.cluster().localNode().consistentId().toString());

            stopGrid("node0");
        }
        else
            consistentId = "127_0_0_1_47500";

        final String workDir = U.defaultWorkDirectory();
        final File binaryMeta = U.resolveWorkDirectory(workDir, "binary_meta", false);
        final File binaryMetaWithConsId = new File(binaryMeta, consistentId);
        final File marshallerMapping = U.resolveWorkDirectory(workDir, "marshaller", false);

        final IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log,
            PAGE_SIZE,
            binaryMetaWithConsId,
            marshallerMapping);

        final BiConsumer<Object, Object> objConsumer = new BiConsumer<Object, Object>() {
            @Override public void accept(Object key, Object val) {
                boolean rmv = remove(ctrlMap, key, val);
                if (!rmv)
                    log.error("Unable to remove Key and value from control Map K:[" + key + "] V: [" + val + "]");

                if (val instanceof IndexedObject) {
                    IndexedObject indexedObj = (IndexedObject)val;
                    assertEquals(indexedObj.iVal, indexedObj.jVal);
                    assertEquals(indexedObj.iVal, key);
                    for (byte datum : indexedObj.getData()) {
                        assert datum >= 'A' && datum <= 'A' + 10;
                    }
                }
            }
        };
        scanIterateAndCount(factory, workDir, consistentId, cntEntries, txCnt, objConsumer, null);

        assert ctrlMap.isEmpty() : " Control Map is not empty after reading entries " + ctrlMap;
    }

    /**
     * Scan WAL and WAL archive for logical records and its entries.
     *
     * @param factory WAL iterator factory.
     * @param workDir Ignite work directory.
     * @param consistentId consistent ID.
     * @param expCntEntries minimum expected entries count to find.
     * @param expTxCnt minimum expected transaction count to find.
     * @param objConsumer object handler, called for each object found in logical data records.
     * @param dataRecordHnd data handler record
     * @throws IgniteCheckedException if failed.
     */
    private void scanIterateAndCount(
        final IgniteWalIteratorFactory factory,
        final String workDir,
        final String consistentId,
        final int expCntEntries,
        final int expTxCnt,
        @Nullable final BiConsumer<Object, Object> objConsumer,
        @Nullable final Consumer<DataRecord> dataRecordHnd) throws IgniteCheckedException {

        final File db = U.resolveWorkDirectory(workDir, DFLT_STORE_DIR, false);
        final File wal = new File(db, "wal");
        final File walArchive = new File(wal, "archive");

        final File walArchiveDirWithConsistentId = new File(walArchive, consistentId);

        final File[] files = walArchiveDirWithConsistentId.listFiles(FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER);

        assert files != null : "Can't iterate over files [" + walArchiveDirWithConsistentId + "] Directory is N/A";
        final WALIterator iter = factory.iteratorArchiveFiles(files);

        final Map<GridCacheVersion, Integer> cntArch = iterateAndCountDataRecord(iter, objConsumer, dataRecordHnd);

        int txCntObservedArch = cntArch.size();
        if (cntArch.containsKey(null))
            txCntObservedArch -= 1; // exclude non transactional updates
        final int entriesArch = valuesSum(cntArch.values());

        log.info("Total tx found loaded using archive directory (file-by-file): " + txCntObservedArch);

        final File walWorkDirWithConsistentId = new File(wal, consistentId);
        final File[] workFiles = walWorkDirWithConsistentId.listFiles(FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER);

        final WALIterator tuples = factory.iteratorWorkFiles(workFiles);
        final Map<GridCacheVersion, Integer> cntWork = iterateAndCountDataRecord(tuples, objConsumer, dataRecordHnd);
        int txCntObservedWork = cntWork.size();
        if (cntWork.containsKey(null))
            txCntObservedWork -= 1; // exclude non transactional updates

        final int entriesWork = valuesSum(cntWork.values());
        log.info("Archive directory: Tx found " + txCntObservedWork + " entries " + entriesWork);

        assert entriesArch + entriesWork >= expCntEntries;
        assert txCntObservedWork + txCntObservedArch >= expTxCnt;
    }

    /**
     * @throws Exception if failed.
     */
    public void testFillWalWithDifferentTypes() throws Exception {
        int cntEntries = 0;
        final String consistentId;

        final Map<Object, Object> ctrlMap = new HashMap<>();
        final Map<Object, Object> ctrlMapForBinaryObjects = new HashMap<>();
        final Collection<String> ctrlStringsToSearch = new HashSet<>();
        final Collection<String> ctrlStringsForBinaryObjSearch = new HashSet<>();
        if (fillWalBeforeTest) {
            final Ignite ignite0 = startGrid("node0");
            ignite0.active(true);

            final IgniteCache<Object, Object> addlCache = ignite0.getOrCreateCache(CACHE_ADDL_NAME);
            addlCache.put("1", "2");
            addlCache.put(1, 2);
            addlCache.put(1L, 2L);
            addlCache.put(TestEnum.A, "Enum_As_Key");
            addlCache.put("Enum_As_Value", TestEnum.B);
            addlCache.put(TestEnum.C, TestEnum.C);

            addlCache.put("Serializable", new TestSerializable(42));
            addlCache.put(new TestSerializable(42), "Serializable_As_Key");
            addlCache.put("Externalizable", new TestExternalizable(42));
            addlCache.put(new TestExternalizable(42), "Externalizable_As_Key");
            addlCache.put(292, new IndexedObject(292));

            final String search1 = "SomeUnexpectedStringValueAsKeyToSearch";
            ctrlStringsToSearch.add(search1);
            ctrlStringsForBinaryObjSearch.add(search1);
            addlCache.put(search1, "SearchKey");

            String search2 = "SomeTestStringContainerToBePrintedLongLine";
            final TestStringContainerToBePrinted val = new TestStringContainerToBePrinted(search2);
            ctrlStringsToSearch.add(val.toString()); //will validate original toString() was called
            ctrlStringsForBinaryObjSearch.add(search2);
            addlCache.put("SearchValue", val);

            String search3 = "SomeTestStringContainerToBePrintedLongLine2";
            final TestStringContainerToBePrinted key = new TestStringContainerToBePrinted(search3);
            ctrlStringsToSearch.add(key.toString()); //will validate original toString() was called
            ctrlStringsForBinaryObjSearch.add(search3); //validate only string itself
            addlCache.put(key, "SearchKey");

            cntEntries = addlCache.size();
            for (Cache.Entry<Object, Object> next : addlCache) {
                ctrlMap.put(next.getKey(), next.getValue());
            }

            for (Cache.Entry<Object, Object> next : addlCache) {
                ctrlMapForBinaryObjects.put(next.getKey(), next.getValue());
            }

            consistentId = U.maskForFileName(ignite0.cluster().localNode().consistentId().toString());

            stopGrid("node0");
        }
        else
            consistentId = "127_0_0_1_47500";

        final String workDir = U.defaultWorkDirectory();

        final File binaryMeta = U.resolveWorkDirectory(workDir, "binary_meta", false);
        final File binaryMetaWithConsId = new File(binaryMeta, consistentId);
        final File marshallerMapping = U.resolveWorkDirectory(workDir, "marshaller", false);

        final IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log, PAGE_SIZE,
            binaryMetaWithConsId,
            marshallerMapping);
        final BiConsumer<Object, Object> objConsumer = new BiConsumer<Object, Object>() {
            @Override public void accept(Object key, Object val) {
                log.info("K: [" + key + ", " +
                    (key != null ? key.getClass().getName() : "?") + "]" +
                    " V: [" + val + ", " +
                    (val != null ? val.getClass().getName() : "?") + "]");
                boolean rmv = remove(ctrlMap, key, val);
                if (!rmv) {
                    String msg = "Unable to remove pair from control map " + "K: [" + key + "] V: [" + val + "]";
                    log.error(msg);
                }
                assert !(val instanceof BinaryObject);
            }
        };

        final Consumer<DataRecord> toStrChecker = new Consumer<DataRecord>() {
            @Override public void accept(DataRecord record) {
                String strRepresentation = record.toString();
                for (Iterator<String> iter = ctrlStringsToSearch.iterator(); iter.hasNext(); ) {
                    final String next = iter.next();
                    if (strRepresentation.contains(next)) {
                        iter.remove();
                        break;
                    }
                }
            }
        };
        scanIterateAndCount(factory, workDir, consistentId, cntEntries, 0, objConsumer, toStrChecker);

        assert ctrlMap.isEmpty() : " Control Map is not empty after reading entries: " + ctrlMap;
        assert ctrlStringsToSearch.isEmpty() : " Control Map for strings in entries is not empty after" +
            " reading records: " + ctrlStringsToSearch;

        //Validate same WAL log with flag binary objects only
        final IgniteWalIteratorFactory keepBinFactory = new IgniteWalIteratorFactory(log, PAGE_SIZE,
            binaryMetaWithConsId,
            marshallerMapping,
            true);
        final BiConsumer<Object, Object> binObjConsumer = new BiConsumer<Object, Object>() {
            @Override public void accept(Object key, Object val) {
                log.info("K(KeepBinary): [" + key + ", " +
                    (key != null ? key.getClass().getName() : "?") + "]" +
                    " V(KeepBinary): [" + val + ", " +
                    (val != null ? val.getClass().getName() : "?") + "]");
                boolean rmv = remove(ctrlMapForBinaryObjects, key, val);
                if (!rmv) {
                    if (key instanceof BinaryObject) {
                        BinaryObject keyBinObj = (BinaryObject)key;
                        String binaryObjTypeName = keyBinObj.type().typeName();
                        if (Objects.equals(TestStringContainerToBePrinted.class.getName(), binaryObjTypeName)) {
                            String data = keyBinObj.field("data");
                            rmv = ctrlMapForBinaryObjects.remove(new TestStringContainerToBePrinted(data)) != null;
                        }
                        else if (Objects.equals(TestSerializable.class.getName(), binaryObjTypeName)) {
                            Integer iVal = keyBinObj.field("iVal");
                            rmv = ctrlMapForBinaryObjects.remove(new TestSerializable(iVal)) != null;
                        }
                        else if (Objects.equals(TestEnum.class.getName(), binaryObjTypeName)) {
                            TestEnum key1 = TestEnum.values()[keyBinObj.enumOrdinal()];
                            rmv = ctrlMapForBinaryObjects.remove(key1) != null;
                        }
                    }
                    else if (val instanceof BinaryObject) {
                        //don't compare BO values, just remove by key
                        rmv = ctrlMapForBinaryObjects.remove(key) != null;
                    }
                }
                if (!rmv)
                    log.error("Unable to remove pair from control map " + "K: [" + key + "] V: [" + val + "]");

                if (val instanceof BinaryObject) {
                    BinaryObject binaryObj = (BinaryObject)val;
                    String binaryObjTypeName = binaryObj.type().typeName();
                    if (Objects.equals(IndexedObject.class.getName(), binaryObjTypeName)) {
                        assertEquals(binaryObj.field("iVal").toString(),
                            binaryObj.field("jVal").toString());

                        byte data[] = binaryObj.field("data");
                        for (byte datum : data) {
                            assert datum >= 'A' && datum <= 'A' + 10;
                        }
                    }
                }
            }
        };

        final Consumer<DataRecord> binObjToStringChecker = new Consumer<DataRecord>() {
            @Override public void accept(DataRecord record) {
                String strRepresentation = record.toString();
                for (Iterator<String> iter = ctrlStringsForBinaryObjSearch.iterator(); iter.hasNext(); ) {
                    final String next = iter.next();
                    if (strRepresentation.contains(next)) {
                        iter.remove();
                        break;
                    }
                }
            }
        };
        scanIterateAndCount(keepBinFactory, workDir, consistentId, cntEntries, 0, binObjConsumer, binObjToStringChecker);

        assert ctrlMapForBinaryObjects.isEmpty() : " Control Map is not empty after reading entries: " + ctrlMapForBinaryObjects;
        assert ctrlStringsForBinaryObjSearch.isEmpty() : " Control Map for strings in entries is not empty after" +
            " reading records: " + ctrlStringsForBinaryObjSearch;

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
     * Iterates over data records, checks each DataRecord and its entries, finds out all transactions in WAL
     *
     * @param walIter iterator to use
     * @return count of data records observed for each global TX ID. Contains null for non tx updates
     * @throws IgniteCheckedException if failure
     */
    private Map<GridCacheVersion, Integer> iterateAndCountDataRecord(
        final WALIterator walIter,
        @Nullable final BiConsumer<Object, Object> cacheObjHnd,
        @Nullable final Consumer<DataRecord> dataRecordHnd) throws IgniteCheckedException {

        final Map<GridCacheVersion, Integer> entriesUnderTxFound = new HashMap<>();

        try (WALIterator stIt = walIter) {
            while (stIt.hasNextX()) {
                final IgniteBiTuple<WALPointer, WALRecord> next = stIt.nextX();
                final WALRecord walRecord = next.get2();

                if (walRecord.type() == WALRecord.RecordType.DATA_RECORD && walRecord instanceof DataRecord) {
                    final DataRecord dataRecord = (DataRecord)walRecord;

                    if (dataRecordHnd != null)
                        dataRecordHnd.accept(dataRecord);
                    final List<DataEntry> entries = dataRecord.writeEntries();

                    for (DataEntry entry : entries) {
                        final GridCacheVersion globalTxId = entry.nearXidVersion();
                        Object unwrappedKeyObj;
                        Object unwrappedValObj;
                        if (entry instanceof UnwrapDataEntry) {
                            UnwrapDataEntry unwrapDataEntry = (UnwrapDataEntry)entry;
                            unwrappedKeyObj = unwrapDataEntry.unwrappedKey();
                            unwrappedValObj = unwrapDataEntry.unwrappedValue();
                        }
                        else if (entry instanceof LazyDataEntry) {
                            unwrappedKeyObj = null;
                            unwrappedValObj = null;
                            //can't check value
                        }
                        else {
                            final CacheObject val = entry.value();

                            unwrappedValObj = val instanceof BinaryObject ? val : val.value(null, false);

                            final CacheObject key = entry.key();

                            unwrappedKeyObj = key instanceof BinaryObject ? key : key.value(null, false);
                        }
                        log.info("//Entry operation " + entry.op() + "; cache Id" + entry.cacheId() + "; " +
                            "under transaction: " + globalTxId +
                            //; entry " + entry +
                            "; Key: " + unwrappedKeyObj +
                            "; Value: " + unwrappedValObj);

                        if (cacheObjHnd != null && unwrappedKeyObj != null || unwrappedValObj != null)
                            cacheObjHnd.accept(unwrappedKeyObj, unwrappedValObj);

                        final Integer entriesUnderTx = entriesUnderTxFound.get(globalTxId);
                        entriesUnderTxFound.put(globalTxId, entriesUnderTx == null ? 1 : entriesUnderTx + 1);
                    }
                }
                else if (walRecord.type() == WALRecord.RecordType.TX_RECORD && walRecord instanceof TxRecord) {
                    final TxRecord txRecord = (TxRecord)walRecord;
                    final GridCacheVersion globalTxId = txRecord.nearXidVersion();

                    log.info("//Tx Record, state: " + txRecord.state() +
                        "; nearTxVersion" + globalTxId);
                }
            }
        }
        return entriesUnderTxFound;
    }

    /**
     * Represents an operation that accepts a single input argument and returns no
     * result.
     *
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

    /**
     * Represents an operation that accepts two input arguments and returns no
     * result.
     *
     * @param <T>
     */
    private interface BiConsumer<T, U> {
        /**
         * Performs this operation on the given argument.
         *
         * @param t the input argument
         */
        public void accept(T t, U u);
    }

    /** Enum for cover binaryObject enum save/load */
    enum TestEnum {
        /** */A, /** */B, /** */C
    }

    /** Special class to test WAL reader resistance to Serializable interface */
    static class TestSerializable implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** I value. */
        private int iVal;

        /**
         * Creates test object
         *
         * @param iVal I value.
         */
        TestSerializable(int iVal) {
            this.iVal = iVal;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestSerializable{" +
                "iVal=" + iVal +
                '}';
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            TestSerializable that = (TestSerializable)o;

            return iVal == that.iVal;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return iVal;
        }
    }

    /** Special class to test WAL reader resistance to Serializable interface */
    static class TestExternalizable implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** I value. */
        private int iVal;

        /** Noop ctor for unmarshalling */
        public TestExternalizable() {

        }

        /**
         * Creates test object with provided value
         *
         * @param iVal I value.
         */
        public TestExternalizable(int iVal) {
            this.iVal = iVal;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestExternalizable{" +
                "iVal=" + iVal +
                '}';
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(iVal);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            iVal = in.readInt();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            TestExternalizable that = (TestExternalizable)o;

            return iVal == that.iVal;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return iVal;
        }
    }

    /** Container class to test toString of data records */
    static class TestStringContainerToBePrinted {
        /** */
        private String data;

        /**
         * Creates container
         *
         * @param data value to be searched in to String
         */
        public TestStringContainerToBePrinted(String data) {
            this.data = data;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            TestStringContainerToBePrinted printed = (TestStringContainerToBePrinted)o;

            return data != null ? data.equals(printed.data) : printed.data == null;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return data != null ? data.hashCode() : 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestStringContainerToBePrinted{" +
                "data='" + data + '\'' +
                '}';
        }
    }
}
