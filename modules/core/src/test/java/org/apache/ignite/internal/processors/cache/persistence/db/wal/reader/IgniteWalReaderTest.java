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
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
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
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

import static java.util.Arrays.fill;
import static org.apache.ignite.events.EventType.EVT_WAL_SEGMENT_ARCHIVED;
import static org.apache.ignite.events.EventType.EVT_WAL_SEGMENT_COMPACTED;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.TX_RECORD;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.CREATE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor.genNewStyleSubfolderName;

/**
 * Test suite for WAL segments reader and event generator.
 */
public class IgniteWalReaderTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Wal segments count */
    private static final int WAL_SEGMENTS = 10;

    /** Cache name. */
    private static final String CACHE_NAME = "cache0";

    /** additional cache for testing different combinations of types in WAL. */
    private static final String CACHE_ADDL_NAME = "cache1";

    /** Dump records to logger. Should be false for non local run. */
    private static final boolean DUMP_RECORDS = true;

    /**
     * Field for transferring setting from test to getConfig method.
     * Archive incomplete segment after inactivity milliseconds.
     */
    private int archiveIncompleteSegmentAfterInactivityMs;

    /** Custom wal mode. */
    private WALMode customWalMode;

    /** Clear properties in afterTest() method. */
    private boolean clearProps;

    /** Set WAL and Archive path to same value. */
    private boolean setWalAndArchiveToSameVal;

    /** Whether to enable WAL archive compaction. */
    private boolean enableWalCompaction;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        CacheConfiguration<Integer, IndexedObject> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg.setIndexedTypes(Integer.class, IndexedObject.class);

        cfg.setCacheConfiguration(ccfg);

        cfg.setIncludeEventTypes(EVT_WAL_SEGMENT_ARCHIVED, EVT_WAL_SEGMENT_COMPACTED);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(1024L * 1024 * 1024)
                    .setPersistenceEnabled(true))
            .setWalSegmentSize(1024 * 1024)
            .setWalSegments(WAL_SEGMENTS)
            .setWalMode(customWalMode != null ? customWalMode : WALMode.BACKGROUND)
            .setWalCompactionEnabled(enableWalCompaction);

        if (archiveIncompleteSegmentAfterInactivityMs > 0)
            dsCfg.setWalAutoArchiveAfterInactivity(archiveIncompleteSegmentAfterInactivityMs);

        String workDir = U.defaultWorkDirectory();
        File db = U.resolveWorkDirectory(workDir, DFLT_STORE_DIR, false);
        File wal = new File(db, "wal");

        if(setWalAndArchiveToSameVal) {
            String walAbsPath = wal.getAbsolutePath();

            dsCfg.setWalPath(walAbsPath);
            dsCfg.setWalArchivePath(walAbsPath);
        }
        else {
            dsCfg.setWalPath(wal.getAbsolutePath());
            dsCfg.setWalArchivePath(new File(wal, "archive").getAbsolutePath());
        }

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        if (clearProps)
            System.clearProperty(IgniteSystemProperties.IGNITE_WAL_LOG_TX_RECORDS);
    }

    /**
     * @throws Exception if failed.
     */
    public void testFillWalAndReadRecords() throws Exception {
        setWalAndArchiveToSameVal = false;

        Ignite ignite0 = startGrid();

        ignite0.cluster().active(true);

        Serializable consistentId = (Serializable)ignite0.cluster().localNode().consistentId();

        String subfolderName = genNewStyleSubfolderName(0, (UUID)consistentId);

        int cacheObjectsToWrite = 10_000;

        putDummyRecords(ignite0, cacheObjectsToWrite);

        stopGrid();

        String workDir = U.defaultWorkDirectory();

        File db = U.resolveWorkDirectory(workDir, DFLT_STORE_DIR, false);

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

        IteratorParametersBuilder params =
            createIteratorParametersBuilder(workDir, subfolderName)
                .filesOrDirs(db);

        // Check iteratorArchiveDirectory and iteratorArchiveFiles are same.
        int cntArchiveDir = iterateAndCount(factory.iterator(params));

        log.info("Total records loaded using directory : " + cntArchiveDir);

        assertTrue(cntArchiveDir > 0);

        // Check iteratorArchiveFiles + iteratorWorkFiles iterate over all entries.
        int[] checkKeyIterArr = new int[cacheObjectsToWrite];

        fill(checkKeyIterArr, 0);

        iterateAndCountDataRecord(
            factory.iterator(params),
            (o1, o2) -> checkKeyIterArr[(Integer)o1]++,
            null
        );

        for (int i = 0; i < cacheObjectsToWrite; i++)
            assertTrue("Iterator didn't find key=" + i, checkKeyIterArr[i] > 0);
    }

    /**
     * Iterates on records and closes iterator.
     *
     * @param walIter iterator to count, will be closed.
     * @return count of records.
     * @throws IgniteCheckedException if failed to iterate.
     */
    private int iterateAndCount(WALIterator walIter) throws IgniteCheckedException {
        int cnt = 0;

        try (WALIterator it = walIter) {
            while (it.hasNextX()) {
                IgniteBiTuple<WALPointer, WALRecord> tup = it.nextX();

                WALRecord walRecord = tup.get2();

                if (walRecord.type() == DATA_RECORD) {
                    DataRecord record = (DataRecord)walRecord;

                    for (DataEntry entry : record.writeEntries()) {
                        KeyCacheObject key = entry.key();
                        CacheObject val = entry.value();

                        if (DUMP_RECORDS)
                            log.info("Op: " + entry.op() + ", Key: " + key + ", Value: " + val);
                    }
                }

                if (DUMP_RECORDS)
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
        assertTrue(checkWhetherWALRelatedEventFired(EVT_WAL_SEGMENT_ARCHIVED));
    }

    /**
     * Tests archive completed event is fired.
     *
     * @throws Exception if failed.
     */
    public void testArchiveCompactedEventFired() throws Exception {
        boolean oldEnableWalCompaction = enableWalCompaction;

        try {
            enableWalCompaction = true;

            assertTrue(checkWhetherWALRelatedEventFired(EVT_WAL_SEGMENT_COMPACTED));
        }
        finally {
            enableWalCompaction = oldEnableWalCompaction;
        }
    }

    /** */
    private boolean checkWhetherWALRelatedEventFired(int evtType) throws Exception {
        AtomicBoolean evtRecorded = new AtomicBoolean();

        Ignite ignite = startGrid();

        ignite.cluster().active(true);

        final IgniteEvents evts = ignite.events();

        if (!evts.isEnabled(evtType))
            fail("nothing to test");

        evts.localListen(e -> {
            WalSegmentArchivedEvent archComplEvt = (WalSegmentArchivedEvent)e;

            long idx = archComplEvt.getAbsWalSegmentIdx();

            log.info("Finished for segment [" +
                idx + ", " + archComplEvt.getArchiveFile() + "]: [" + e + "]");

            evtRecorded.set(true);

            return true;
        }, evtType);

        putDummyRecords(ignite, 5_000);

        stopGrid();

        return evtRecorded.get();
    }

    /**
     * Tests time out based WAL segment archiving.
     *
     * @throws Exception if failure occurs.
     */
    public void testArchiveIncompleteSegmentAfterInactivity() throws Exception {
        AtomicBoolean waitingForEvt = new AtomicBoolean();

        CountDownLatch archiveSegmentForInactivity = new CountDownLatch(1);

        archiveIncompleteSegmentAfterInactivityMs = 1000;

        Ignite ignite = startGrid();

        ignite.cluster().active(true);

        IgniteEvents evts = ignite.events();

        evts.localListen(e -> {
            WalSegmentArchivedEvent archComplEvt = (WalSegmentArchivedEvent)e;

            long idx = archComplEvt.getAbsWalSegmentIdx();

            log.info("Finished archive for segment [" + idx + ", " +
                archComplEvt.getArchiveFile() + "]: [" + e + ']');

            if (waitingForEvt.get())
                archiveSegmentForInactivity.countDown();

            return true;
        }, EVT_WAL_SEGMENT_ARCHIVED);

        putDummyRecords(ignite, 100);

        waitingForEvt.set(true); // Flag for skipping regular log() and rollOver().

        log.info("Wait for archiving segment for inactive grid started");

        boolean recordedAfterSleep = archiveSegmentForInactivity.await(
            archiveIncompleteSegmentAfterInactivityMs + 1001, TimeUnit.MILLISECONDS);

        stopGrid();

        assertTrue(recordedAfterSleep);
    }

    /**
     * Tests archive completed event is fired.
     *
     * @throws Exception if failed.
     */
    public void testFillWalForExactSegmentsCount() throws Exception {
        customWalMode = WALMode.FSYNC;

        CountDownLatch reqSegments = new CountDownLatch(15);

        Ignite ignite = startGrid();

        ignite.cluster().active(true);

        final IgniteEvents evts = ignite.events();

        if (!evts.isEnabled(EVT_WAL_SEGMENT_ARCHIVED))
            fail("nothing to test");

        evts.localListen(e -> {
            WalSegmentArchivedEvent archComplEvt = (WalSegmentArchivedEvent)e;

            long idx = archComplEvt.getAbsWalSegmentIdx();

            log.info("Finished archive for segment [" + idx + ", " +
                archComplEvt.getArchiveFile() + "]: [" + e + "]");

            reqSegments.countDown();

            return true;
        }, EVT_WAL_SEGMENT_ARCHIVED);

        int totalEntries = 0;

        while (reqSegments.getCount() > 0) {
            int write = 500;

            putAllDummyRecords(ignite, write);

            totalEntries += write;

            Assert.assertTrue("Too much entries generated, but segments was not become available",
                totalEntries < 10000);
        }

        String subfolderName = genDbSubfolderName(ignite, 0);

        stopGrid();

        String workDir = U.defaultWorkDirectory();

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

        IteratorParametersBuilder iterParametersBuilder = createIteratorParametersBuilder(workDir, subfolderName);

        iterParametersBuilder.filesOrDirs(workDir);

        scanIterateAndCount(
            factory,
            iterParametersBuilder,
            totalEntries,
            0,
            null,
            null
        );
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
        Ignite ignite0 = startGrid();

        ignite0.cluster().active(true);

        int cntEntries = 1000;
        int txCnt = 100;

        IgniteCache<Object, Object> entries = txPutDummyRecords(ignite0, cntEntries, txCnt);

        Map<Object, Object> ctrlMap = new HashMap<>();

        for (Cache.Entry<Object, Object> next : entries)
            ctrlMap.put(next.getKey(), next.getValue());

        String subfolderName = genDbSubfolderName(ignite0, 0);

        stopGrid();

        String workDir = U.defaultWorkDirectory();

        IteratorParametersBuilder params = createIteratorParametersBuilder(workDir, subfolderName);

        params.filesOrDirs(workDir);

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

        IgniteBiInClosure<Object, Object> objConsumer = (key, val) -> {
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
        };

        scanIterateAndCount(factory, params, cntEntries, txCnt, objConsumer, null);

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
     * @param minCntEntries minimum expected entries count to find.
     * @param minTxCnt minimum expected transaction count to find.
     * @param objConsumer object handler, called for each object found in logical data records.
     * @param dataRecordHnd data handler record.
     * @throws IgniteCheckedException if failed.
     */
    private void scanIterateAndCount(
        IgniteWalIteratorFactory factory,
        IteratorParametersBuilder itParamBuilder,
        int minCntEntries,
        int minTxCnt,
        @Nullable IgniteBiInClosure<Object, Object> objConsumer,
        @Nullable IgniteInClosure<DataRecord> dataRecordHnd
    ) throws IgniteCheckedException {
        WALIterator iter = factory.iterator(itParamBuilder);

        Map<GridCacheVersion, Integer> cntArch = iterateAndCountDataRecord(iter, objConsumer, dataRecordHnd);

        int txCntObservedArch = cntArch.size();

        if (cntArch.containsKey(null))
            txCntObservedArch -= 1; // Exclude non transactional updates.

        int entries = valuesSum(cntArch.values());

        log.info("Total tx found loaded using archive directory (file-by-file): " + txCntObservedArch);

        assertTrue("txCntObservedArch=" + txCntObservedArch + " >= minTxCnt=" + minTxCnt,
            txCntObservedArch >= minTxCnt);

        assertTrue("entries=" + entries + " >= minCntEntries=" + minCntEntries,
            entries >= minCntEntries);
    }

    /**
     * @throws Exception if failed.
     */
    public void testFillWalWithDifferentTypes() throws Exception {
        Ignite ig = startGrid();

        ig.cluster().active(true);

        IgniteCache<Object, Object> addlCache = ig.getOrCreateCache(CACHE_ADDL_NAME);

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

        String search1 = "SomeUnexpectedStringValueAsKeyToSearch";

        Collection<String> ctrlStringsToSearch = new HashSet<>();

        ctrlStringsToSearch.add(search1);

        Collection<String> ctrlStringsForBinaryObjSearch = new HashSet<>();

        ctrlStringsForBinaryObjSearch.add(search1);

        addlCache.put(search1, "SearchKey");

        String search2 = "SomeTestStringContainerToBePrintedLongLine";

        TestStringContainerToBePrinted val = new TestStringContainerToBePrinted(search2);

        ctrlStringsToSearch.add(val.toString()); //will validate original toString() was called
        ctrlStringsForBinaryObjSearch.add(search2);

        addlCache.put("SearchValue", val);

        String search3 = "SomeTestStringContainerToBePrintedLongLine2";

        TestStringContainerToBePrinted key = new TestStringContainerToBePrinted(search3);
        ctrlStringsToSearch.add(key.toString()); //will validate original toString() was called
        ctrlStringsForBinaryObjSearch.add(search3); //validate only string itself

        addlCache.put(key, "SearchKey");

        int cntEntries = addlCache.size();

        Map<Object, Object> ctrlMap = new HashMap<>();

        for (Cache.Entry<Object, Object> next : addlCache)
            ctrlMap.put(next.getKey(), next.getValue());

        Map<Object, Object> ctrlMapForBinaryObjects = new HashMap<>();

        for (Cache.Entry<Object, Object> next : addlCache)
            ctrlMapForBinaryObjects.put(next.getKey(), next.getValue());

        String subfolderName = genDbSubfolderName(ig, 0);

        // Wait async allocation wal segment file by archiver.
        Thread.sleep(1000);

        stopGrid("node0", false);

        String workDir = U.defaultWorkDirectory();

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

        IteratorParametersBuilder params0 = createIteratorParametersBuilder(workDir, subfolderName);

        params0.filesOrDirs(workDir);

        IgniteBiInClosure<Object, Object> objConsumer = (key12, val1) -> {
            log.info("K: [" + key12 + ", " +
                (key12 != null ? key12.getClass().getName() : "?") + "]" +
                " V: [" + val1 + ", " +
                (val1 != null ? val1.getClass().getName() : "?") + "]");
            boolean rmv = remove(ctrlMap, key12, val1);
            if (!rmv) {
                String msg = "Unable to remove pair from control map " + "K: [" + key12 + "] V: [" + val1 + "]";
                log.error(msg);
            }
            assertFalse(val1 instanceof BinaryObject);
        };

        IgniteInClosure<DataRecord> toStrChecker = record -> {
            String strRepresentation = record.toString();

            for (Iterator<String> iter = ctrlStringsToSearch.iterator(); iter.hasNext(); ) {
                final String next = iter.next();
                if (strRepresentation.contains(next)) {
                    iter.remove();
                    break;
                }
            }
        };

        scanIterateAndCount(factory, params0, cntEntries, 0, objConsumer, toStrChecker);

        assertTrue(" Control Map is not empty after reading entries: " + ctrlMap, ctrlMap.isEmpty());
        assertTrue(" Control Map for strings in entries is not empty after" +
            " reading records: " + ctrlStringsToSearch, ctrlStringsToSearch.isEmpty());

        IgniteBiInClosure<Object, Object> binObjConsumer = (key13, val12) -> {
            log.info("K(KeepBinary): [" + key13 + ", " +
                (key13 != null ? key13.getClass().getName() : "?") + "]" +
                " V(KeepBinary): [" + val12 + ", " +
                (val12 != null ? val12.getClass().getName() : "?") + "]");

            boolean rmv = remove(ctrlMapForBinaryObjects, key13, val12);

            if (!rmv) {
                if (key13 instanceof BinaryObject) {
                    BinaryObject keyBinObj = (BinaryObject)key13;
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
                else if (val12 instanceof BinaryObject) {
                    //don't compare BO values, just remove by key
                    rmv = ctrlMapForBinaryObjects.remove(key13) != null;
                }
            }
            if (!rmv)
                log.error("Unable to remove pair from control map " + "K: [" + key13 + "] V: [" + val12 + "]");

            if (val12 instanceof BinaryObject) {
                BinaryObject binaryObj = (BinaryObject)val12;
                String binaryObjTypeName = binaryObj.type().typeName();

                if (Objects.equals(IndexedObject.class.getName(), binaryObjTypeName)) {
                    assertEquals(
                        binaryObj.field("iVal").toString(),
                        binaryObj.field("jVal").toString()
                    );

                    byte data[] = binaryObj.field("data");

                    for (byte datum : data)
                        assertTrue(datum >= 'A' && datum <= 'A' + 10);
                }
            }
        };

        IgniteInClosure<DataRecord> binObjToStrChecker = record -> {
            String strRepresentation = record.toString();

            for (Iterator<String> iter = ctrlStringsForBinaryObjSearch.iterator(); iter.hasNext(); ) {
                final String next = iter.next();

                if (strRepresentation.contains(next)) {
                    iter.remove();

                    break;
                }
            }
        };

        IteratorParametersBuilder params1 = createIteratorParametersBuilder(workDir, subfolderName);

        params1.filesOrDirs(workDir).keepBinary(true);

        //Validate same WAL log with flag binary objects only
        IgniteWalIteratorFactory keepBinFactory = new IgniteWalIteratorFactory(log);

        scanIterateAndCount(keepBinFactory, params1, cntEntries, 0, binObjConsumer, binObjToStrChecker);

        assertTrue(" Control Map is not empty after reading entries: " +
            ctrlMapForBinaryObjects, ctrlMapForBinaryObjects.isEmpty());

        assertTrue(" Control Map for strings in entries is not empty after" +
            " reading records: " + ctrlStringsForBinaryObjSearch, ctrlStringsForBinaryObjSearch.isEmpty());
    }

    /**
     * Tests reading of empty WAL from non filled cluster.
     *
     * @throws Exception if failed.
     */
    public void testReadEmptyWal() throws Exception {
        customWalMode = WALMode.FSYNC;

        Ignite ignite = startGrid();

        ignite.cluster().active(true);

        ignite.cluster().active(false);

        final String subfolderName = genDbSubfolderName(ignite, 0);

        stopGrid();

        String workDir = U.defaultWorkDirectory();

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

        IteratorParametersBuilder iterParametersBuilder =
            createIteratorParametersBuilder(workDir, subfolderName)
                .filesOrDirs(workDir);

        scanIterateAndCount(
            factory,
            iterParametersBuilder,
            0,
            0,
            null,
            null
        );
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
     * @param mode Cache Atomicity Mode.
     * @throws Exception if failed.
     */
    private void runRemoveOperationTest(CacheAtomicityMode mode) throws Exception {
        Ignite ignite = startGrid();

        ignite.cluster().active(true);

        createCache2(ignite, mode);

        ignite.cluster().active(false);

        String subfolderName = genDbSubfolderName(ignite, 0);

        stopGrid();

        String workDir = U.defaultWorkDirectory();

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

        IteratorParametersBuilder params = createIteratorParametersBuilder(workDir, subfolderName);

        params.filesOrDirs(workDir);

        StringBuilder sb = new StringBuilder();

        Map<GridCacheOperation, Integer> operationsFound = new EnumMap<>(GridCacheOperation.class);

        scanIterateAndCount(
            factory,
            params,
            0,
            0,
            null,
            dataRecord -> {
                final List<DataEntry> entries = dataRecord.writeEntries();

                sb.append("{");

                for (DataEntry entry : entries) {
                    GridCacheOperation op = entry.op();
                    Integer cnt = operationsFound.get(op);

                    operationsFound.put(op, cnt == null ? 1 : (cnt + 1));

                    if (entry instanceof UnwrapDataEntry) {
                        UnwrapDataEntry entry1 = (UnwrapDataEntry)entry;

                        sb.append(entry1.op())
                            .append(" for ")
                            .append(entry1.unwrappedKey());

                        GridCacheVersion ver = entry.nearXidVersion();

                        sb.append(", ");

                        if (ver != null)
                            sb.append("tx=")
                                .append(ver)
                                .append(", ");
                    }
                }

                sb.append("}\n");
            });

        final Integer deletesFound = operationsFound.get(DELETE);

        if (log.isInfoEnabled())
            log.info(sb.toString());

        assertTrue("Delete operations should be found in log: " + operationsFound,
            deletesFound != null && deletesFound > 0);
    }

    /**
     * Tests transaction generation and WAL for putAll cache operation.
     *
     * @throws Exception if failed.
     */
    public void testPutAllTxIntoTwoNodes() throws Exception {
        Ignite ignite = startGrid("node0");
        Ignite ignite1 = startGrid(1);

        ignite.cluster().active(true);

        Map<Object, IndexedObject> map = new TreeMap<>();

        int cntEntries = 1000;

        for (int i = 0; i < cntEntries; i++)
            map.put(i, new IndexedObject(i));

        ignite.cache(CACHE_NAME).putAll(map);

        ignite.cluster().active(false);

        String subfolderName1 = genDbSubfolderName(ignite, 0);
        String subfolderName2 = genDbSubfolderName(ignite1, 1);

        stopAllGrids();

        String workDir = U.defaultWorkDirectory();

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

        StringBuilder sb = new StringBuilder();

        Map<GridCacheOperation, Integer> operationsFound = new EnumMap<>(GridCacheOperation.class);

        IgniteInClosure<DataRecord> drHnd = dataRecord -> {
            List<DataEntry> entries = dataRecord.writeEntries();

            sb.append("{");

            for (DataEntry entry : entries) {
                GridCacheOperation op = entry.op();
                Integer cnt = operationsFound.get(op);

                operationsFound.put(op, cnt == null ? 1 : (cnt + 1));

                if (entry instanceof UnwrapDataEntry) {
                    final UnwrapDataEntry entry1 = (UnwrapDataEntry)entry;

                    sb.append(entry1.op()).append(" for ").append(entry1.unwrappedKey());
                    final GridCacheVersion ver = entry.nearXidVersion();

                    sb.append(", ");

                    if (ver != null)
                        sb.append("tx=").append(ver).append(", ");
                }
            }

            sb.append("}\n");
        };

        scanIterateAndCount(
            factory,
            createIteratorParametersBuilder(workDir, subfolderName1)
                .filesOrDirs(
                    workDir + "/db/wal/" + subfolderName1,
                    workDir + "/db/wal/archive/" + subfolderName1
                ),
            1,
            1,
            null, drHnd
        );

        scanIterateAndCount(
            factory,
            createIteratorParametersBuilder(workDir, subfolderName2)
                .filesOrDirs(
                    workDir + "/db/wal/" + subfolderName2,
                    workDir + "/db/wal/archive/" + subfolderName2
                ),
            1,
            1,
            null,
            drHnd
        );

        Integer createsFound = operationsFound.get(CREATE);

        if (log.isInfoEnabled())
            log.info(sb.toString());

        assertTrue("Create operations should be found in log: " + operationsFound,
            createsFound != null && createsFound > 0);

        assertTrue("Create operations count should be at least " + cntEntries + " in log: " + operationsFound,
            createsFound >= cntEntries);
    }

    /**
     * Tests transaction generation and WAL for putAll cache operation.
     *
     * @throws Exception if failed.
     */
    public void testTxRecordsReadWoBinaryMeta() throws Exception {
        clearProps = true;

        System.setProperty(IgniteSystemProperties.IGNITE_WAL_LOG_TX_RECORDS, "true");

        Ignite ignite = startGrid("node0");

        ignite.cluster().active(true);

        Map<Object, IndexedObject> map = new TreeMap<>();

        for (int i = 0; i < 1000; i++)
            map.put(i, new IndexedObject(i));

        ignite.cache(CACHE_NAME).putAll(map);

        ignite.cluster().active(false);

        String workDir = U.defaultWorkDirectory();

        String subfolderName = genDbSubfolderName(ignite, 0);

        stopAllGrids();

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(new NullLogger());

        IteratorParametersBuilder params = createIteratorParametersBuilder(workDir, subfolderName);

        scanIterateAndCount(
            factory,
            params.filesOrDirs(workDir),
            1000,
            1,
            null,
            null
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testCheckBoundsIterator() throws Exception {
        Ignite ignite = startGrid("node0");

        ignite.cluster().active(true);

        try (IgniteDataStreamer<Integer, IndexedObject> st = ignite.dataStreamer(CACHE_NAME)) {
            st.allowOverwrite(true);

            for (int i = 0; i < 10_000; i++)
                st.addData(i, new IndexedObject(i));
        }

        stopAllGrids();

        List<FileWALPointer> wal = new ArrayList<>();

        String workDir = U.defaultWorkDirectory();

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory();

        try (WALIterator it = factory.iterator(workDir)) {
            while (it.hasNext()) {
                IgniteBiTuple<WALPointer, WALRecord> tup = it.next();

                wal.add((FileWALPointer)tup.get1());
            }
        }

        Random rnd = new Random();

        int from0 = rnd.nextInt(wal.size() - 2) + 1;
        int to0 = wal.size() - 1;

        // +1 for skip first record.
        FileWALPointer exp0First = wal.get(from0);
        FileWALPointer exp0Last = wal.get(to0);

        T2<FileWALPointer, WALRecord> actl0First = null;
        T2<FileWALPointer, WALRecord> actl0Last = null;

        int records0 = 0;

        try (WALIterator it = factory.iterator(exp0First, workDir)) {
            while (it.hasNext()) {
                IgniteBiTuple<WALPointer, WALRecord> tup = it.next();

                if (actl0First == null)
                    actl0First = new T2<>((FileWALPointer)tup.get1(), tup.get2());

                actl0Last = new T2<>((FileWALPointer)tup.get1(), tup.get2());

                records0++;
            }
        }

        log.info("Check REPLAY FROM:" + exp0First + "\n" +
            "expFirst=" + exp0First + " actlFirst=" + actl0First + ", " +
            "expLast=" + exp0Last + " actlLast=" + actl0Last);

        // +1 because bound include.
        Assert.assertEquals(to0 - from0 + 1, records0);

        Assert.assertNotNull(actl0First);
        Assert.assertNotNull(actl0Last);

        Assert.assertEquals(exp0First, actl0First.get1());
        Assert.assertEquals(exp0Last, actl0Last.get1());

        int from1 = 0;
        int to1 = rnd.nextInt(wal.size() - 3) + 1;

        // -3 for skip last record.
        FileWALPointer exp1First = wal.get(from1);
        FileWALPointer exp1Last = wal.get(to1);

        T2<FileWALPointer, WALRecord> actl1First = null;
        T2<FileWALPointer, WALRecord> actl1Last = null;

        int records1 = 0;

        try (WALIterator it = factory.iterator(
            new IteratorParametersBuilder()
                .filesOrDirs(workDir)
                .to(exp1Last)
        )) {
            while (it.hasNext()) {
                IgniteBiTuple<WALPointer, WALRecord> tup = it.next();

                if (actl1First == null)
                    actl1First = new T2<>((FileWALPointer)tup.get1(), tup.get2());

                actl1Last = new T2<>((FileWALPointer)tup.get1(), tup.get2());

                records1++;
            }
        }

        log.info("Check REPLAY TO:" + exp1Last + "\n" +
            "expFirst=" + exp1First + " actlFirst=" + actl1First + ", " +
            "expLast=" + exp1Last + " actlLast=" + actl1Last);

        // +1 because bound include.
        Assert.assertEquals(to1 - from1 + 1, records1);

        Assert.assertNotNull(actl1First);
        Assert.assertNotNull(actl1Last);

        Assert.assertEquals(exp1First, actl1First.get1());
        Assert.assertEquals(exp1Last, actl1Last.get1());

        int from2 = rnd.nextInt(wal.size() - 2);
        int to2 = rnd.nextInt((wal.size() - 1) - from2) + from2;

        FileWALPointer exp2First = wal.get(from2);
        FileWALPointer exp2Last = wal.get(to2);

        T2<FileWALPointer, WALRecord> actl2First = null;
        T2<FileWALPointer, WALRecord> actl2Last = null;

        int records2 = 0;

        try (WALIterator it = factory.iterator(
            new IteratorParametersBuilder()
                .filesOrDirs(workDir)
                .from(exp2First)
                .to(exp2Last)
        )) {
            while (it.hasNext()) {
                IgniteBiTuple<WALPointer, WALRecord> tup = it.next();

                if (actl2First == null)
                    actl2First = new T2<>((FileWALPointer)tup.get1(), tup.get2());

                actl2Last = new T2<>((FileWALPointer)tup.get1(), tup.get2());

                records2++;
            }
        }

        log.info("Check REPLAY BETWEEN:" + exp2First + " " + exp2Last+ "\n" +
            "expFirst=" + exp2First + " actlFirst=" + actl2First + ", " +
            "expLast=" + exp2Last + " actlLast=" + actl2Last);

        // +1 because bound include.
        Assert.assertEquals(to2 - from2 + 1, records2);

        Assert.assertNotNull(actl2First);
        Assert.assertNotNull(actl2Last);

        Assert.assertEquals(exp2First, actl2First.get1());
        Assert.assertEquals(exp2Last, actl2Last.get1());
    }

    /**
     * @param workDir Work directory.
     * @param subfolderName Subfolder name.
     * @return WAL iterator factory.
     * @throws IgniteCheckedException If failed.
     */
    @NotNull private IteratorParametersBuilder createIteratorParametersBuilder(
        String workDir,
        String subfolderName
    ) throws IgniteCheckedException {
        File binaryMeta = U.resolveWorkDirectory(workDir, "binary_meta", false);
        File binaryMetaWithConsId = new File(binaryMeta, subfolderName);
        File marshallerMapping = U.resolveWorkDirectory(workDir, "marshaller", false);

        return new IteratorParametersBuilder()
            .binaryMetadataFileStoreDir(binaryMetaWithConsId)
            .marshallerMappingFileStoreDir(marshallerMapping);
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
        WALIterator walIter,
        @Nullable IgniteBiInClosure<Object, Object> cacheObjHnd,
        @Nullable IgniteInClosure<DataRecord> dataRecordHnd
    ) throws IgniteCheckedException {

        Map<GridCacheVersion, Integer> entriesUnderTxFound = new HashMap<>();

        try (WALIterator stIt = walIter) {
            while (stIt.hasNextX()) {
                IgniteBiTuple<WALPointer, WALRecord> tup = stIt.nextX();

                WALRecord walRecord = tup.get2();

                if (walRecord.type() == DATA_RECORD && walRecord instanceof DataRecord) {
                    DataRecord dataRecord = (DataRecord)walRecord;

                    if (dataRecordHnd != null)
                        dataRecordHnd.apply(dataRecord);

                    List<DataEntry> entries = dataRecord.writeEntries();

                    for (DataEntry entry : entries) {
                        GridCacheVersion globalTxId = entry.nearXidVersion();

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

                        if (DUMP_RECORDS)
                            log.info("//Entry operation " + entry.op() + "; cache Id" + entry.cacheId() + "; " +
                                "under transaction: " + globalTxId +
                                //; entry " + entry +
                                "; Key: " + unwrappedKeyObj +
                                "; Value: " + unwrappedValObj);

                        if (cacheObjHnd != null && (unwrappedKeyObj != null || unwrappedValObj != null))
                            cacheObjHnd.apply(unwrappedKeyObj, unwrappedValObj);

                        Integer entriesUnderTx = entriesUnderTxFound.get(globalTxId);

                        entriesUnderTxFound.put(globalTxId, entriesUnderTx == null ? 1 : entriesUnderTx + 1);
                    }
                }
                else if (walRecord.type() == TX_RECORD && walRecord instanceof TxRecord) {
                    TxRecord txRecord = (TxRecord)walRecord;
                    GridCacheVersion globalTxId = txRecord.nearXidVersion();

                    if (DUMP_RECORDS)
                        log.info("//Tx Record, state: " + txRecord.state() +
                            "; nearTxVersion" + globalTxId);
                }
            }
        }

        return entriesUnderTxFound;
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
        TestExternalizable(int iVal) {
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
        TestStringContainerToBePrinted(String data) {
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
        Organization(int key, String name) {
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
