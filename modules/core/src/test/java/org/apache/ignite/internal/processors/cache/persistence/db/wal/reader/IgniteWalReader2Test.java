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

package org.apache.ignite.internal.processors.cache.persistence.db.wal.reader;

import java.io.File;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.UnwrappedDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Arrays.fill;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_BINARY_METADATA_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_MARSHALLER_PATH;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.events.EventType.EVT_WAL_SEGMENT_ARCHIVED;
import static org.apache.ignite.events.EventType.EVT_WAL_SEGMENT_COMPACTED;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

public class IgniteWalReader2Test extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setIndexedTypes(Integer.class, IndexedObject.class));

        cfg.setIncludeEventTypes(EVT_WAL_SEGMENT_ARCHIVED, EVT_WAL_SEGMENT_COMPACTED);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(1024L * 1024 * 1024)
                    .setPersistenceEnabled(true))
            .setWalSegmentSize(1024 * 1024)
            .setWalSegments(10)
            .setWalMode(LOG_ONLY);

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    @Test
    public void testFillWalAndReadRecords() throws Exception {
        Ignite ign = startGrid();

        ign.cluster().active(true);

        String consIdDir = U.maskForFileName(ign.cluster().localNode().consistentId().toString());

        IgniteCache<Object, Object> cache0 = ign.cache(DEFAULT_CACHE_NAME);

        int entryCnt = 10_000;

        for (int i = 0; i < entryCnt; i++)
            cache0.put(i, new IndexedObject(i));

        stopGrid();

        String workDir = U.defaultWorkDirectory();

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

        IteratorParametersBuilder params = new IteratorParametersBuilder()
            .binaryMetadataFileStoreDir(
                new File(U.resolveWorkDirectory(workDir, DFLT_BINARY_METADATA_PATH, false), consIdDir))
            .marshallerMappingFileStoreDir(U.resolveWorkDirectory(workDir, DFLT_MARSHALLER_PATH, false))
            .filesOrDirs(U.resolveWorkDirectory(workDir, DFLT_STORE_DIR, false));

        int[] keyArr = new int[entryCnt];

        fill(keyArr, 0);

        iterateAndCountDataRecord(
            factory.iterator(params),
            (key, val) -> keyArr[(Integer)key]++,
            null
        );

        for (int i = 0; i < entryCnt; i++)
            assertTrue("Iterator didn't find key=" + i, keyArr[i] > 0);
    }

    private void iterateAndCountDataRecord(
        WALIterator iter,
        @Nullable IgniteBiInClosure<Object, Object> cacheObjHnd,
        @Nullable IgniteInClosure<DataRecord> dataRecordHnd
    ) throws IgniteCheckedException {
        try (WALIterator stIt = iter) {
            while (stIt.hasNextX()) {
                IgniteBiTuple<WALPointer, WALRecord> tup = stIt.nextX();

                WALRecord walRecord = tup.get2();

                WALRecord.RecordType type = walRecord.type();

                //noinspection EnumSwitchStatementWhichMissesCases
                switch (type) {
                    case DATA_RECORD:
                        // Fallthrough.
                    case MVCC_DATA_RECORD: {
                        assert walRecord instanceof DataRecord;

                        DataRecord dataRecord = (DataRecord)walRecord;

                        if (dataRecordHnd != null)
                            dataRecordHnd.apply(dataRecord);

                        List<DataEntry> entries = dataRecord.writeEntries();

                        for (DataEntry entry : entries) {
                            GridCacheVersion globalTxId = entry.nearXidVersion();

                            Object unwrappedKeyObj;
                            Object unwrappedValObj;

                            if (entry instanceof UnwrappedDataEntry) {
                                UnwrappedDataEntry unwrapDataEntry = (UnwrappedDataEntry)entry;
                                unwrappedKeyObj = unwrapDataEntry.unwrappedKey();
                                unwrappedValObj = unwrapDataEntry.unwrappedValue();
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

                            if (cacheObjHnd != null && (unwrappedKeyObj != null || unwrappedValObj != null))
                                cacheObjHnd.apply(unwrappedKeyObj, unwrappedValObj);
                        }
                    }

                    break;

                    case TX_RECORD:
                        // Fallthrough
                    case MVCC_TX_RECORD: {
                        assert walRecord instanceof TxRecord;

                        TxRecord txRecord = (TxRecord)walRecord;
                        GridCacheVersion globalTxId = txRecord.nearXidVersion();

                        log.info("//Tx Record, state: " + txRecord.state() + "; nearTxVersion" + globalTxId);
                    }
                }
            }
        }
    }
}
