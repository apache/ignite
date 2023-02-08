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

package org.apache.ignite;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cdc.CdcIndexRebuildTest.TestVal;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_PAGE_INSERT;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_PAGE_REPLACE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.util.IgniteUtils.MB;

/** */
@SuppressWarnings({"resource"})
public class WalDuringIndexRebuildTest extends GridCommonAbstractTest {
    /** Batches count. */
    public static final int ENTRIES_CNT = 10_000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setClusterStateOnStart(INACTIVE)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true))
                .setMaxWalArchiveSize(UNLIMITED_WAL_ARCHIVE));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testIndexBinRemoveRebuild() throws Exception {
        doTestIndexRebuild(srv -> {
            try {
                srv.cluster().state(INACTIVE);

                awaitPartitionMapExchange();

                Path path = checkIdxFileExists();

                stopGrid(0);

                Files.delete(path);

                srv = startGrid(0);

                srv.cluster().state(ACTIVE);
            }
            catch (Throwable e) {
                throw new IgniteException(e);
            }
        });
    }

    /** */
    @Test
    public void testIndexForceRebuild() throws Exception {
        doTestIndexRebuild(srv -> forceRebuildIndexes(srv, srv.cachex(DEFAULT_CACHE_NAME).context()));
    }

    /** */
    private void doTestIndexRebuild(Consumer<IgniteEx> doRebuildIdx) throws Exception {
        IgniteEx srv = startGrid(0);

        srv.cluster().state(ACTIVE);

        createAndPopulateCache(srv);

        forceCheckpoint(srv);

        WALPointer walPrtBefore = srv.context().cache().context().wal().lastWritePointer();

        long startTs = System.currentTimeMillis();

        doRebuildIdx.accept(srv);

        srv = grid(0);

        indexRebuildFuture(srv, cacheId(DEFAULT_CACHE_NAME)).get(getTestTimeout());

        log.warning(">>>>>> Index rebuild finished and took " + (System.currentTimeMillis() - startTs) + " ms");

        forceCheckpoint(srv);

        Map<RecordType, Long> recTypesBefore = countWalRecordsByTypes((rt, wp) -> wp.compareTo(walPrtBefore) <= 0);
        Map<RecordType, Long> recTypesAfter = countWalRecordsByTypes((rt, wp) -> wp.compareTo(walPrtBefore) > 0);

        log.warning(">>>>>> WalRecords comparison:");

        log.warning(String.format("%-62.60s%-30.28s%-30.28s\n", "Record type", "Data load (before rebuild)",
                "After index rebuild"));

        for (RecordType recType : RecordType.values()) {
            log.warning(String.format("%-62.60s%-30.28s%-30.28s",
                recType,
                recTypesBefore.get(recType),
                recTypesAfter.get(recType)));
        }

        checkIdxFileExists();

        assertEquals((Long)3L, recTypesAfter.getOrDefault(BTREE_PAGE_INSERT, 0L));
        assertEquals((Long)0L, recTypesAfter.getOrDefault(BTREE_PAGE_REPLACE, 0L));
    }

    /** */
    private void createAndPopulateCache(IgniteEx ignite) {
        IgniteCache<Integer, TestVal> cache = ignite.getOrCreateCache(
            new CacheConfiguration<Integer, TestVal>(DEFAULT_CACHE_NAME)
                .setIndexedTypes(Integer.class, TestVal.class));

        for (int i = 0; i < ENTRIES_CNT; i++)
            cache.put(i, new TestVal());

        assertEquals(3, ignite.context().indexProcessor().indexes(DEFAULT_CACHE_NAME).size());
    }

    /** */
    private Path checkIdxFileExists() throws IgniteCheckedException, IOException {
        Path pdsDir = U.resolveWorkDirectory(
            U.defaultWorkDirectory(),
            DFLT_STORE_DIR + File.separatorChar + grid(0).name().replace(".", "_") +
                File.separatorChar + "cache-" + DEFAULT_CACHE_NAME,
            false
        ).toPath();

        Path idxFile = Files.list(pdsDir)
            .filter(p -> p.endsWith(INDEX_FILE_NAME))
            .findFirst()
            .orElseThrow(() -> new IgniteException("index.bin not found"));

        assertTrue("Index file not found", idxFile.toFile().exists());

        log.warning(">>>>>> Index file size: " + Files.size(idxFile) / MB + " MB");

        return idxFile;
    }

    /** @param filter Predicate. */
    private Map<RecordType, Long> countWalRecordsByTypes(
        IgniteBiPredicate<RecordType, WALPointer> filter
    ) throws IgniteCheckedException {
        String dn2DirName = grid(0).name().replace(".", "_");

        IteratorParametersBuilder beforeIdxRemoveBldr = new IteratorParametersBuilder()
            .filesOrDirs(
                U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_WAL_PATH + "/" + dn2DirName, false),
                U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_WAL_ARCHIVE_PATH + "/" + dn2DirName, false)
            )
            .filter(filter);

        Map<RecordType, Long> cntByRecTypes = new EnumMap<>(RecordType.class);

        for (RecordType recType : RecordType.values())
            cntByRecTypes.put(recType, 0L);

        try (WALIterator walIter = new IgniteWalIteratorFactory(log).iterator(beforeIdxRemoveBldr)) {
            while (walIter.hasNext())
                cntByRecTypes.merge(walIter.next().getValue().type(), 1L, Long::sum);
        }

        return cntByRecTypes;
    }

    /** @param ignite Ignite. */
    private long curWalIdx(IgniteEx ignite) {
        return ignite.context()
            .cache()
            .context()
            .wal()
            .currentSegment();
    }
}
