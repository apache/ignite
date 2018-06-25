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
import java.io.Serializable;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.WalSegmentArchivedEvent;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.UnwrapDataEntry;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.logger.NullLogger;
import org.junit.Assert;

import static org.apache.ignite.events.EventType.EVT_WAL_SEGMENT_ARCHIVED;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.CREATE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor.genNewStyleSubfolderName;

/**
 * Test suite for WAL segments reader and event generator.
 */
public class IgniteWalReaderTest extends AbstractIgniteWalReaderTest {
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

        File[] workFiles = walWorkDirWithConsistentId.listFiles(FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER);

        iterateAndCountDataRecord(factory.iteratorWorkFiles(workFiles), new IgniteBiInClosure<Object, Object>() {
            @Override public void apply(Object o, Object o2) {
                checkKeyIterArr[(Integer) o]++;
            }
        }, null).size();

        for (int i =0 ; i< cacheObjectsToWrite; i++)
            assertTrue("Iterator didn't find key="+ i, checkKeyIterArr[i] > 0);
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
}
