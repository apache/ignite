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
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
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
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

import static org.apache.ignite.events.EventType.EVT_WAL_SEGMENT_ARCHIVED;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.CREATE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor.genNewStyleSubfolderName;

/**
 * Test suite for WAL segments reader and event generator.
 */
public class IgniteWalReaderTest extends GridCommonAbstractTest {
    /** Wal segments count */
    private static final int WAL_SEGMENTS = 10;

    /** Cache name. */
    private static final String CACHE_NAME = "cache0";

    /** additional cache for testing different combinations of types in WAL. */
    private static final String CACHE_ADDL_NAME = "cache1";

    /** Dump records to logger. Should be false for non local run. */
    private static final boolean dumpRecords = false;

    /** Page size to set. */
    public static final int PAGE_SIZE = 4 * 1024;

    /**
     * Field for transferring setting from test to getConfig method.
     * Archive incomplete segment after inactivity milliseconds.
     */
    private int archiveIncompleteSegmentAfterInactivityMs;

    /** Custom wal mode. */
    private WALMode customWalMode;

    /** Clear properties in afterTest() method. */
    private boolean clearProperties;

    /** Set WAL and Archive path to same value. */
    private boolean setWalAndArchiveToSameValue;

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

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(1024L * 1024 * 1024).setPersistenceEnabled(true))
            .setPageSize(PAGE_SIZE)
            .setWalHistorySize(1)
            .setWalSegmentSize(1024 * 1024)
            .setWalSegments(WAL_SEGMENTS)
            .setWalMode(customWalMode != null ? customWalMode : WALMode.BACKGROUND);

        if (archiveIncompleteSegmentAfterInactivityMs > 0)
            dsCfg.setWalAutoArchiveAfterInactivity(archiveIncompleteSegmentAfterInactivityMs);

        final String workDir = U.defaultWorkDirectory();
        final File db = U.resolveWorkDirectory(workDir, DFLT_STORE_DIR, false);
        final File wal = new File(db, "wal");

        if(setWalAndArchiveToSameValue) {
            final String walAbsPath = wal.getAbsolutePath();

            dsCfg.setWalPath(walAbsPath);
            dsCfg.setWalArchivePath(walAbsPath);
        } else {
            dsCfg.setWalPath(wal.getAbsolutePath());
            dsCfg.setWalArchivePath(new File(wal, "archive").getAbsolutePath());
        }

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        deleteWorkFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        if (clearProperties)
            System.clearProperty(IgniteSystemProperties.IGNITE_WAL_LOG_TX_RECORDS);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void deleteWorkFiles() throws Exception {
        cleanPersistenceDir();
    }

    /**
     * @throws Exception if failed.
     */
    public void testFillWalAndReadRecords() throws Exception {
        setWalAndArchiveToSameValue = false;
        final int cacheObjectsToWrite = 10000;

        final Ignite ignite0 = startGrid("node0");

        ignite0.active(true);

        final Serializable consistentId = (Serializable)ignite0.cluster().localNode().consistentId();
        final String subfolderName = genNewStyleSubfolderName(0, (UUID)consistentId);

        putDummyRecords(ignite0, cacheObjectsToWrite);

        stopGrid("node0");

        final String workDir = U.defaultWorkDirectory();
        final File db = U.resolveWorkDirectory(workDir, DFLT_STORE_DIR, false);
        final File wal = new File(db, "wal");
        final File walArchive = setWalAndArchiveToSameValue ? wal : new File(wal, "archive");

        int[] checkKeyIterArr = new int[cacheObjectsToWrite];

        final File walArchiveDirWithConsistentId = new File(walArchive, subfolderName);
        final File walWorkDirWithConsistentId = new File(wal, subfolderName);
        final IgniteWalIteratorFactory factory = createWalIteratorFactory(workDir, subfolderName);

        //Check iteratorArchiveDirectory and iteratorArchiveFiles are same.
        final int cntArchiveDir = iterateAndCount(factory.iteratorArchiveDirectory(walArchiveDirWithConsistentId));

        log.info("Total records loaded using directory : " + cntArchiveDir);

        final int cntArchiveFileByFile = iterateAndCount(factory.iteratorArchiveFiles(
            walArchiveDirWithConsistentId.listFiles(FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER)));

        log.info("Total records loaded using archive directory (file-by-file): " + cntArchiveFileByFile);

        assertTrue(cntArchiveDir == cntArchiveFileByFile);

        //Check iteratorArchiveFiles + iteratorWorkFiles iterate over all entries.
        Arrays.fill(checkKeyIterArr, 0);

        iterateAndCountDataRecord(factory.iteratorArchiveFiles(
            walArchiveDirWithConsistentId.listFiles(FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER)), new IgniteBiInClosure<Object, Object>() {
            @Override public void apply(Object o, Object o2) {
                checkKeyIterArr[(Integer)o]++;
            }
        }, null);

        final File[] workFiles = walWorkDirWithConsistentId.listFiles(FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER);

        iterateAndCountDataRecord(factory.iteratorWorkFiles(workFiles), new IgniteBiInClosure<Object, Object>() {
            @Override public void apply(Object o, Object o2) {
                checkKeyIterArr[(Integer) o]++;
            }
        }, null).size();

        for (int i =0 ; i< cacheObjectsToWrite; i++)
            assertTrue("Iterator didn't find key="+ i, checkKeyIterArr[i] > 0);
    }

    /**
     * Iterates on records and closes iterator.
     *
     * @param walIter iterator to count, will be closed.
     * @return count of records.
     * @throws IgniteCheckedException if failed to iterate.
     */
    private int iterateAndCount(WALIterator walIter) throws IgniteCheckedException {
        return iterateAndCount(walIter, true);
    }

    /**
     * Iterates on records and closes iterator.
     *
     * @param walIter iterator to count, will be closed.
     * @param touchEntries access data within entries.
     * @return count of records.
     * @throws IgniteCheckedException if failed to iterate.
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
     * Tests archive completed event is fired.
     *
     * @throws Exception if failed.
     */
    public void testArchiveCompletedEventFired() throws Exception {
        final AtomicBoolean evtRecorded = new AtomicBoolean();

        final Ignite ignite = startGrid("node0");

        ignite.active(true);

        final IgniteEvents evts = ignite.events();

        if (!evts.isEnabled(EVT_WAL_SEGMENT_ARCHIVED))
            assertTrue("nothing to test", false);

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
        assertTrue(evtRecorded.get());
    }

    /**
     * Puts provided number of records to fill WAL.
     *
     * @param ignite ignite instance.
     * @param recordsToWrite count.
     */
    private void putDummyRecords(Ignite ignite, int recordsToWrite) {
        IgniteCache<Object, Object> cache0 = ignite.cache(CACHE_NAME);

        for (int i = 0; i < recordsToWrite; i++)
            cache0.put(i, new IndexedObject(i));
    }

    /**
     * Puts provided number of records to fill WAL.
     *
     * @param ignite ignite instance.
     * @param recordsToWrite count.
     */
    private void putAllDummyRecords(Ignite ignite, int recordsToWrite) {
        IgniteCache<Object, Object> cache0 = ignite.cache(CACHE_NAME);

        Map<Object, Object> values = new HashMap<>();

        for (int i = 0; i < recordsToWrite; i++)
            values.put(i, new IndexedObject(i));

        cache0.putAll(values);
    }

    /**
     * Puts provided number of records to fill WAL under transactions.
     *
     * @param ignite ignite instance.
     * @param recordsToWrite count.
     * @param txCnt transactions to run. If number is less then records count, txCnt records will be written.
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
     * Tests time out based WAL segment archiving.
     *
     * @throws Exception if failure occurs.
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
        assertTrue(recordedAfterSleep);
    }

    /**
     * Removes entry by key and value from map (java 8 map method copy).
     *
     * @param m map to remove from.
     * @param key key to remove.
     * @param val value to remove.
     * @return true if remove was successful.
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
     * Places records under transaction, checks its value using WAL.
     *
     * @throws Exception if failed.
     */
    public void testTxFillWalAndExtractDataRecords() throws Exception {
        final int cntEntries = 1000;
        final int txCnt = 100;

        final Ignite ignite0 = startGrid("node0");

        ignite0.active(true);

        final IgniteCache<Object, Object> entries = txPutDummyRecords(ignite0, cntEntries, txCnt);

        final Map<Object, Object> ctrlMap = new HashMap<>();    for (Cache.Entry<Object, Object> next : entries)
                ctrlMap.put(next.getKey(), next.getValue());


        final String subfolderName = genDbSubfolderName(ignite0, 0);
        stopGrid("node0");

        final String workDir = U.defaultWorkDirectory();
        final File binaryMeta = U.resolveWorkDirectory(workDir, "binary_meta", false);
        final File binaryMetaWithConsId = new File(binaryMeta, subfolderName);
        final File marshallerMapping = U.resolveWorkDirectory(workDir, "marshaller", false);

        final IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log,
            PAGE_SIZE,
            binaryMetaWithConsId,
            marshallerMapping);

        final IgniteBiInClosure<Object, Object> objConsumer = new IgniteBiInClosure<Object, Object>() {
            @Override public void apply(Object key, Object val) {
                boolean rmv = remove(ctrlMap, key, val);
                if (!rmv)
                    log.error("Unable to remove Key and value from control Map K:[" + key + "] V: [" + val + "]");

                if (val instanceof IndexedObject) {
                    IndexedObject indexedObj = (IndexedObject)val;

                    assertEquals(indexedObj.iVal, indexedObj.jVal);
                    assertEquals(indexedObj.iVal, key);

                    for (byte datum : indexedObj.getData())
                        assertTrue(datum >= 'A' && datum <= 'A' + 10);
                }
            }
        };

        scanIterateAndCount(factory, workDir, subfolderName, cntEntries, txCnt, objConsumer, null);

        assertTrue(" Control Map is not empty after reading entries " + ctrlMap, ctrlMap.isEmpty());
    }

    /**
     * Generates DB subfolder name for provided node index (local) and UUID (consistent ID).
     *
     * @param ignite ignite instance.
     * @param nodeIdx node index.
     * @return folder file name.
     */
    @NotNull private String genDbSubfolderName(Ignite ignite, int nodeIdx) {
        return genNewStyleSubfolderName(nodeIdx, (UUID)ignite.cluster().localNode().consistentId());
    }

    /**
     * Scan WAL and WAL archive for logical records and its entries.
     *
     * @param factory WAL iterator factory.
     * @param workDir Ignite work directory.
     * @param subfolderName DB subfolder name based on consistent ID.
     * @param minCntEntries minimum expected entries count to find.
     * @param minTxCnt minimum expected transaction count to find.
     * @param objConsumer object handler, called for each object found in logical data records.
     * @param dataRecordHnd data handler record.
     * @throws IgniteCheckedException if failed.
     */
    private void scanIterateAndCount(
        final IgniteWalIteratorFactory factory,
        final String workDir,
        final String subfolderName,
        final int minCntEntries,
        final int minTxCnt,
        @Nullable final IgniteBiInClosure<Object, Object> objConsumer,
        @Nullable final IgniteInClosure<DataRecord> dataRecordHnd) throws IgniteCheckedException {

        final File db = U.resolveWorkDirectory(workDir, DFLT_STORE_DIR, false);
        final File wal = new File(db, "wal");
        final File walArchive = new File(wal, "archive");

        final File walArchiveDirWithConsistentId = new File(walArchive, subfolderName);

        final File[] files = walArchiveDirWithConsistentId.listFiles(FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER);
        A.notNull(files, "Can't iterate over files [" + walArchiveDirWithConsistentId + "] Directory is N/A");
        final WALIterator iter = factory.iteratorArchiveFiles(files);

        final Map<GridCacheVersion, Integer> cntArch = iterateAndCountDataRecord(iter, objConsumer, dataRecordHnd);

        int txCntObservedArch = cntArch.size();
        if (cntArch.containsKey(null))
            txCntObservedArch -= 1; // exclude non transactional updates
        final int entriesArch = valuesSum(cntArch.values());

        log.info("Total tx found loaded using archive directory (file-by-file): " + txCntObservedArch);

        final File walWorkDirWithNodeSubDir = new File(wal, subfolderName);
        final File[] workFiles = walWorkDirWithNodeSubDir.listFiles(FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER);

        final WALIterator tuples = factory.iteratorWorkFiles(workFiles);
        final Map<GridCacheVersion, Integer> cntWork = iterateAndCountDataRecord(tuples, objConsumer, dataRecordHnd);
        int txCntObservedWork = cntWork.size();
        if (cntWork.containsKey(null))
            txCntObservedWork -= 1; // exclude non transactional updates

        final int entriesWork = valuesSum(cntWork.values());
        log.info("Archive directory: Tx found " + txCntObservedWork + " entries " + entriesWork);

        assertTrue("entriesArch=" + entriesArch + " + entriesWork=" + entriesWork
                + " >= minCntEntries=" + minCntEntries,
            entriesArch + entriesWork >= minCntEntries);
        assertTrue("txCntObservedWork=" + txCntObservedWork + " + txCntObservedArch=" + txCntObservedArch
                + " >= minTxCnt=" + minTxCnt,
            txCntObservedWork + txCntObservedArch >= minTxCnt);
    }

    /**
     * @throws Exception if failed.
     */
    public void testFillWalWithDifferentTypes() throws Exception {
        int cntEntries;

        final Map<Object, Object> ctrlMap = new HashMap<>();
        final Map<Object, Object> ctrlMapForBinaryObjects = new HashMap<>();
        final Collection<String> ctrlStringsToSearch = new HashSet<>();
        final Collection<String> ctrlStringsForBinaryObjSearch = new HashSet<>();
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
            for (Cache.Entry<Object, Object> next : addlCache)
                ctrlMap.put(next.getKey(), next.getValue());

            for (Cache.Entry<Object, Object> next : addlCache)
                ctrlMapForBinaryObjects.put(next.getKey(), next.getValue());


        final String subfolderName = genDbSubfolderName(ignite0, 0);

        stopGrid("node0");

        final String workDir = U.defaultWorkDirectory();

        final File binaryMeta = U.resolveWorkDirectory(workDir, "binary_meta", false);
        final File binaryMetaWithNodeSubfolder = new File(binaryMeta, subfolderName);
        final File marshallerMapping = U.resolveWorkDirectory(workDir, "marshaller", false);

        final IgniteWalIteratorFactory factory = createWalIteratorFactory(workDir, subfolderName);
        final IgniteBiInClosure<Object, Object> objConsumer = new IgniteBiInClosure<Object, Object>() {
            @Override public void apply(Object key, Object val) {
                log.info("K: [" + key + ", " +
                    (key != null ? key.getClass().getName() : "?") + "]" +
                    " V: [" + val + ", " +
                    (val != null ? val.getClass().getName() : "?") + "]");
                boolean rmv = remove(ctrlMap, key, val);
                if (!rmv) {
                    String msg = "Unable to remove pair from control map " + "K: [" + key + "] V: [" + val + "]";
                    log.error(msg);
                }
                assertFalse(val instanceof BinaryObject);
            }
        };

        final IgniteInClosure<DataRecord> toStrChecker = new IgniteInClosure<DataRecord>() {
            @Override public void apply(DataRecord record) {
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
        scanIterateAndCount(factory, workDir, subfolderName, cntEntries, 0, objConsumer, toStrChecker);

        assertTrue(" Control Map is not empty after reading entries: " + ctrlMap, ctrlMap.isEmpty());
        assertTrue(" Control Map for strings in entries is not empty after" +
            " reading records: " + ctrlStringsToSearch, ctrlStringsToSearch.isEmpty());

        //Validate same WAL log with flag binary objects only
        final IgniteWalIteratorFactory keepBinFactory = new IgniteWalIteratorFactory(log, PAGE_SIZE,
            binaryMetaWithNodeSubfolder,
            marshallerMapping,
            true);
        final IgniteBiInClosure<Object, Object> binObjConsumer = new IgniteBiInClosure<Object, Object>() {
            @Override public void apply(Object key, Object val) {
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
                        for (byte datum : data)
                            assertTrue(datum >= 'A' && datum <= 'A' + 10);
                    }
                }
            }
        };

        final IgniteInClosure<DataRecord> binObjToStrChecker = new IgniteInClosure<DataRecord>() {
            @Override public void apply(DataRecord record) {
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

        scanIterateAndCount(keepBinFactory, workDir, subfolderName, cntEntries, 0, binObjConsumer, binObjToStrChecker);

        assertTrue(" Control Map is not empty after reading entries: " + ctrlMapForBinaryObjects,
            ctrlMapForBinaryObjects.isEmpty());
        assertTrue(" Control Map for strings in entries is not empty after" +
                " reading records: " + ctrlStringsForBinaryObjSearch,
            ctrlStringsForBinaryObjSearch.isEmpty());

    }

    /**
     * Tests archive completed event is fired.
     *
     * @throws Exception if failed.
     */
    public void testFillWalForExactSegmentsCount() throws Exception {
        customWalMode = WALMode.FSYNC;

        final CountDownLatch reqSegments = new CountDownLatch(15);
        final Ignite ignite = startGrid("node0");

        ignite.active(true);

        final IgniteEvents evts = ignite.events();

        if (!evts.isEnabled(EVT_WAL_SEGMENT_ARCHIVED))
            assertTrue("nothing to test", false);

        evts.localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event e) {
                WalSegmentArchivedEvent archComplEvt = (WalSegmentArchivedEvent)e;
                long idx = archComplEvt.getAbsWalSegmentIdx();
                log.info("Finished archive for segment [" + idx + ", " +
                    archComplEvt.getArchiveFile() + "]: [" + e + "]");

                reqSegments.countDown();
                return true;
            }
        }, EVT_WAL_SEGMENT_ARCHIVED);


        int totalEntries = 0;
        while (reqSegments.getCount() > 0) {
            final int write = 500;
            putAllDummyRecords(ignite, write);
            totalEntries += write;
            Assert.assertTrue("Too much entries generated, but segments was not become available",
                totalEntries < 10000);
        }
        final String subfolderName = genDbSubfolderName(ignite, 0);

        stopGrid("node0");

        final String workDir = U.defaultWorkDirectory();
        final IgniteWalIteratorFactory factory = createWalIteratorFactory(workDir, subfolderName);

        scanIterateAndCount(factory, workDir, subfolderName, totalEntries, 0, null, null);
    }

    /**
     * Tests reading of empty WAL from non filled cluster.
     *
     * @throws Exception if failed.
     */
    public void testReadEmptyWal() throws Exception {
        customWalMode = WALMode.FSYNC;

        final Ignite ignite = startGrid("node0");

        ignite.active(true);
        ignite.active(false);

        final String subfolderName = genDbSubfolderName(ignite, 0);

        stopGrid("node0");

        final String workDir = U.defaultWorkDirectory();
        final IgniteWalIteratorFactory factory = createWalIteratorFactory(workDir, subfolderName);

        scanIterateAndCount(factory, workDir, subfolderName, 0, 0, null, null);
    }

    /**
     * Creates and fills cache with data.
     *
     * @param ig Ignite instance.
     * @param mode Cache Atomicity Mode.
     */
    private void createCache2(Ignite ig, CacheAtomicityMode mode) {
        if (log.isInfoEnabled())
            log.info("Populating the cache...");

        final CacheConfiguration<Integer, Organization> cfg = new CacheConfiguration<>("Org" + "11");
        cfg.setAtomicityMode(mode);
        final IgniteCache<Integer, Organization> cache = ig.getOrCreateCache(cfg).withKeepBinary()
            .withAllowAtomicOpsInTx();

        try (Transaction tx = ig.transactions().txStart()) {
            for (int i = 0; i < 10; i++) {

                cache.put(i, new Organization(i, "Organization-" + i));

                if (i % 2 == 0)
                    cache.put(i, new Organization(i, "Organization-updated-" + i));

                if (i % 5 == 0)
                    cache.remove(i);
            }
            tx.commit();
        }

    }

    /**
     * Test if DELETE operation can be found for transactional cache after mixed cache operations including remove().
     *
     * @throws Exception if failed.
     */
    public void testRemoveOperationPresentedForDataEntry() throws Exception {
        runRemoveOperationTest(CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     * Test if DELETE operation can be found for atomic cache after mixed cache operations including remove().
     *
     * @throws Exception if failed.
     */
    public void testRemoveOperationPresentedForDataEntryForAtomic() throws Exception {
        runRemoveOperationTest(CacheAtomicityMode.ATOMIC);
    }


    /**
     * Test if DELETE operation can be found after mixed cache operations including remove().
     *
     * @throws Exception if failed.
     * @param mode Cache Atomicity Mode.
     */
    private void runRemoveOperationTest(CacheAtomicityMode mode) throws Exception {
        final Ignite ignite = startGrid("node0");

        ignite.active(true);
        createCache2(ignite, mode);
        ignite.active(false);

        final String subfolderName = genDbSubfolderName(ignite, 0);

        stopGrid("node0");

        final String workDir = U.defaultWorkDirectory();
        final IgniteWalIteratorFactory factory = createWalIteratorFactory(workDir, subfolderName);

        final StringBuilder builder = new StringBuilder();
        final Map<GridCacheOperation, Integer> operationsFound = new EnumMap<>(GridCacheOperation.class);

        scanIterateAndCount(factory, workDir, subfolderName, 0, 0, null, new IgniteInClosure<DataRecord>() {
            @Override public void apply(DataRecord dataRecord) {
                final List<DataEntry> entries = dataRecord.writeEntries();

                builder.append("{");
                for (DataEntry entry : entries) {
                    final GridCacheOperation op = entry.op();
                    final Integer cnt = operationsFound.get(op);

                    operationsFound.put(op, cnt == null ? 1 : (cnt + 1));

                    if (entry instanceof UnwrapDataEntry) {
                        final UnwrapDataEntry entry1 = (UnwrapDataEntry)entry;

                        builder.append(entry1.op()).append(" for ").append(entry1.unwrappedKey());
                        final GridCacheVersion ver = entry.nearXidVersion();

                        builder.append(", ");

                        if (ver != null)
                            builder.append("tx=").append(ver).append(", ");
                    }
                }

                builder.append("}\n");
            }
        });

        final Integer deletesFound = operationsFound.get(DELETE);

        if (log.isInfoEnabled())
            log.info(builder.toString());

        assertTrue("Delete operations should be found in log: " + operationsFound,
            deletesFound != null && deletesFound > 0);
    }

    /**
     * Tests transaction generation and WAL for putAll cache operation.
     * @throws Exception if failed.
     */
    public void testPutAllTxIntoTwoNodes() throws Exception {
        final Ignite ignite = startGrid("node0");
        final Ignite ignite1 = startGrid(1);

        ignite.active(true);

        final Map<Object, IndexedObject> map = new TreeMap<>();

        final int cntEntries = 1000;
        for (int i = 0; i < cntEntries; i++)
            map.put(i, new IndexedObject(i));

        ignite.cache(CACHE_NAME).putAll(map);

        ignite.active(false);

        final String subfolderName = genDbSubfolderName(ignite, 0);
        final String subfolderName1 = genDbSubfolderName(ignite1, 1);

        stopAllGrids();

        final String workDir = U.defaultWorkDirectory();
        final IgniteWalIteratorFactory factory = createWalIteratorFactory(workDir, subfolderName);

        final StringBuilder builder = new StringBuilder();
        final Map<GridCacheOperation, Integer> operationsFound = new EnumMap<>(GridCacheOperation.class);

        final IgniteInClosure<DataRecord> drHnd = new IgniteInClosure<DataRecord>() {
            @Override public void apply(DataRecord dataRecord) {
                final List<DataEntry> entries = dataRecord.writeEntries();

                builder.append("{");
                for (DataEntry entry : entries) {
                    final GridCacheOperation op = entry.op();
                    final Integer cnt = operationsFound.get(op);

                    operationsFound.put(op, cnt == null ? 1 : (cnt + 1));

                    if (entry instanceof UnwrapDataEntry) {
                        final UnwrapDataEntry entry1 = (UnwrapDataEntry)entry;

                        builder.append(entry1.op()).append(" for ").append(entry1.unwrappedKey());
                        final GridCacheVersion ver = entry.nearXidVersion();

                        builder.append(", ");

                        if (ver != null)
                            builder.append("tx=").append(ver).append(", ");
                    }
                }

                builder.append("}\n");
            }
        };
        scanIterateAndCount(factory, workDir, subfolderName, 1, 1, null, drHnd);
        scanIterateAndCount(factory, workDir, subfolderName1, 1, 1, null, drHnd);

        final Integer createsFound = operationsFound.get(CREATE);

        if (log.isInfoEnabled())
            log.info(builder.toString());

        assertTrue("Create operations should be found in log: " + operationsFound,
            createsFound != null && createsFound > 0);

        assertTrue("Create operations count should be at least " + cntEntries + " in log: " + operationsFound,
            createsFound != null && createsFound >= cntEntries);

    }

    /**
     * Tests transaction generation and WAL for putAll cache operation.
     * @throws Exception if failed.
     */
    public void testTxRecordsReadWoBinaryMeta() throws Exception {
        clearProperties = true;
        System.setProperty(IgniteSystemProperties.IGNITE_WAL_LOG_TX_RECORDS, "true");

        final Ignite ignite = startGrid("node0");
        ignite.active(true);

        final Map<Object, IndexedObject> map = new TreeMap<>();

        for (int i = 0; i < 1000; i++)
            map.put(i, new IndexedObject(i));

        ignite.cache(CACHE_NAME).putAll(map);

        ignite.active(false);

        final String workDir = U.defaultWorkDirectory();
        final String subfolderName = genDbSubfolderName(ignite, 0);
        stopAllGrids();

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(new NullLogger(),
            PAGE_SIZE,
            null,
            null,
            false);

        scanIterateAndCount(factory, workDir, subfolderName, 1000, 1, null, null);
    }

    /**
     * @param workDir Work directory.
     * @param subfolderName Subfolder name.
     * @return WAL iterator factory.
     * @throws IgniteCheckedException If failed.
     */
    @NotNull private IgniteWalIteratorFactory createWalIteratorFactory(
        final String workDir,
        final String subfolderName
    ) throws IgniteCheckedException {
        final File binaryMeta = U.resolveWorkDirectory(workDir, "binary_meta", false);
        final File binaryMetaWithConsId = new File(binaryMeta, subfolderName);
        final File marshallerMapping = U.resolveWorkDirectory(workDir, "marshaller", false);

        return new IgniteWalIteratorFactory(log,
            PAGE_SIZE,
            binaryMetaWithConsId,
            marshallerMapping,
            false);
    }

    /**
     * @param values collection with numbers.
     * @return sum of numbers.
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
     * Iterates over data records, checks each DataRecord and its entries, finds out all transactions in WAL.
     *
     * @param walIter iterator to use.
     * @return count of data records observed for each global TX ID. Contains null for non tx updates.
     * @throws IgniteCheckedException if failure.
     */
    private Map<GridCacheVersion, Integer> iterateAndCountDataRecord(
        final WALIterator walIter,
        @Nullable final IgniteBiInClosure<Object, Object> cacheObjHnd,
        @Nullable final IgniteInClosure<DataRecord> dataRecordHnd) throws IgniteCheckedException {

        final Map<GridCacheVersion, Integer> entriesUnderTxFound = new HashMap<>();

        try (WALIterator stIt = walIter) {
            while (stIt.hasNextX()) {
                final IgniteBiTuple<WALPointer, WALRecord> next = stIt.nextX();
                final WALRecord walRecord = next.get2();

                if (walRecord.type() == WALRecord.RecordType.DATA_RECORD && walRecord instanceof DataRecord) {
                    final DataRecord dataRecord = (DataRecord)walRecord;

                    if (dataRecordHnd != null)
                        dataRecordHnd.apply(dataRecord);
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

                        if (dumpRecords)
                            log.info("//Entry operation " + entry.op() + "; cache Id" + entry.cacheId() + "; " +
                                "under transaction: " + globalTxId +
                                //; entry " + entry +
                                "; Key: " + unwrappedKeyObj +
                                "; Value: " + unwrappedValObj);

                        if (cacheObjHnd != null && (unwrappedKeyObj != null || unwrappedValObj != null))
                            cacheObjHnd.apply(unwrappedKeyObj, unwrappedValObj);

                        final Integer entriesUnderTx = entriesUnderTxFound.get(globalTxId);
                        entriesUnderTxFound.put(globalTxId, entriesUnderTx == null ? 1 : entriesUnderTx + 1);
                    }
                }
                else if (walRecord.type() == WALRecord.RecordType.TX_RECORD && walRecord instanceof TxRecord) {
                    final TxRecord txRecord = (TxRecord)walRecord;
                    final GridCacheVersion globalTxId = txRecord.nearXidVersion();

                    if (dumpRecords)
                        log.info("//Tx Record, state: " + txRecord.state() +
                            "; nearTxVersion" + globalTxId);
                }
            }
        }
        return entriesUnderTxFound;
    }

    /** Enum for cover binaryObject enum save/load. */
    enum TestEnum {
        /** */A, /** */B, /** */C
    }

    /** Special class to test WAL reader resistance to Serializable interface. */
    static class TestSerializable implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** I value. */
        private int iVal;

        /**
         * Creates test object.
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

    /** Special class to test WAL reader resistance to Serializable interface. */
    static class TestExternalizable implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** I value. */
        private int iVal;

        /** Noop ctor for unmarshalling */
        public TestExternalizable() {

        }

        /**
         * Creates test object with provided value.
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

    /** Container class to test toString of data records. */
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

    /** Test class for storing in ignite. */
    private static class Organization {
        /** Key. */
        private final int key;
        /** Name. */
        private final String name;

        /**
         * @param key Key.
         * @param name Name.
         */
        public Organization(int key, String name) {
            this.key = key;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Organization{" +
                "key=" + key +
                ", name='" + name + '\'' +
                '}';
        }
    }
}
