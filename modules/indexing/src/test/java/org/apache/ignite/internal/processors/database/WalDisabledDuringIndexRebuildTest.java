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

package org.apache.ignite.internal.processors.database;

import java.io.File;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cdc.CdcIndexRebuildTest.TestVal;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISABLE_WAL_DURING_FULL_INDEX_REBUILD;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_PAGE_INSERT;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;

/** */
@SuppressWarnings({"resource"})
public class WalDisabledDuringIndexRebuildTest extends GridCommonAbstractTest {
    /** Batches count. */
    public static final int ENTRIES_CNT = 1_000;

    // 1а, 1б - проверить тоже самое на cacheGroup'е с несколькими кешами.
    // 3. index.bin удаляется и перезапускается когда нода упала посередине.
    // 4. Можно ли отключать при запуске вручную (?).

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
    @WithSystemProperty(key = IGNITE_DISABLE_WAL_DURING_FULL_INDEX_REBUILD, value = "true")
    public void testDisabled() throws Exception {
        doTestIndexRebuild(true);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_DISABLE_WAL_DURING_FULL_INDEX_REBUILD, value = "false")
    public void testNotDisabled() throws Exception {
        doTestIndexRebuild(false);
    }

    /** */
    private void doTestIndexRebuild(boolean expWalDisabled) throws Exception {
        IgniteEx srv = startGrid(0);

        srv.cluster().state(ACTIVE);

        IgniteCache<Integer, TestVal> cache = srv.getOrCreateCache(
            new CacheConfiguration<Integer, TestVal>(DEFAULT_CACHE_NAME)
                .setIndexedTypes(Integer.class, TestVal.class));

        for (int i = 0; i < ENTRIES_CNT; i++)
            cache.put(i, new TestVal());

        assertEquals(3, srv.context().indexProcessor().indexes(DEFAULT_CACHE_NAME).size());

        WALPointer walPrtBefore = srv.context().cache().context().wal().lastWritePointer();

        File idx = checkIdxFile();

        stopGrid(0);

        U.delete(idx);

        srv = startGrid(0);

        srv.cluster().state(ACTIVE);

        indexRebuildFuture(srv, cacheId(DEFAULT_CACHE_NAME)).get(getTestTimeout());

        forceCheckpoint(srv);

        Map<RecordType, Long> recTypesAfter = countWalRecordsByTypes(wp -> wp.compareTo(walPrtBefore) > 0);

        checkIdxFile();

        if (expWalDisabled)
            assertEquals((Long)3L, recTypesAfter.getOrDefault(BTREE_PAGE_INSERT, 0L));
        else
            assertTrue(recTypesAfter.getOrDefault(BTREE_PAGE_INSERT, 0L) > 0);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_DISABLE_WAL_DURING_FULL_INDEX_REBUILD, value = "true")
    public void testRestartInCaseOfFailure() throws Exception {
        AtomicBoolean err = new AtomicBoolean(false);

        BPlusTree.testHndWrapper = (tree, hnd) -> {
            if (tree.name().equals("-33802375_TESTVAL_F1_IDX##H2Tree")) {
                PageHandler<Object, BPlusTree.Result> delegate = (PageHandler<Object, BPlusTree.Result>)hnd;

                return new PageHandler() {
                    @Override public Object run(
                        int cacheId,
                        long pageId,
                        long page,
                        long pageAddr,
                        PageIO io,
                        Boolean walPlc,
                        Object arg,
                        int intArg,
                        IoStatisticsHolder statHolder
                    ) throws IgniteCheckedException {
                        if (err.get())
                            throw new IgniteCheckedException("Expected error");

                        return delegate.run(cacheId, pageId, page, pageAddr, io, walPlc, arg, intArg, statHolder);
                    }
                };
            }

            return hnd;
        };

        try {
            doTestIndexRebuild(true);
        }
        finally {
            BPlusTree.testHndWrapper = null;
        }
    }

    /** */
    private File checkIdxFile() throws IgniteCheckedException {
        File idxFile = new File(U.resolveWorkDirectory(
            U.defaultWorkDirectory(),
            DFLT_STORE_DIR + File.separatorChar + grid(0).name().replace(".", "_") +
                File.separatorChar + "cache-" + DEFAULT_CACHE_NAME,
            false
        ), INDEX_FILE_NAME);

        assertTrue("Index file not found", idxFile.exists());

        return idxFile;
    }

    /** @param filter Predicate. */
    private Map<RecordType, Long> countWalRecordsByTypes(
        Predicate<WALPointer> filter
    ) throws IgniteCheckedException {
        String dn2DirName = grid(0).name().replace(".", "_");

        IteratorParametersBuilder beforeIdxRemoveBldr = new IteratorParametersBuilder()
            .filesOrDirs(
                U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_WAL_PATH + "/" + dn2DirName, false),
                U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_WAL_ARCHIVE_PATH + "/" + dn2DirName, false)
            )
            .filter((rt, ptr) -> filter.test(ptr));

        Map<RecordType, Long> cntByRecTypes = new EnumMap<>(RecordType.class);

        for (RecordType recType : RecordType.values())
            cntByRecTypes.put(recType, 0L);

        try (WALIterator walIter = new IgniteWalIteratorFactory(log).iterator(beforeIdxRemoveBldr)) {
            while (walIter.hasNext())
                cntByRecTypes.merge(walIter.next().getValue().type(), 1L, Long::sum);
        }

        return cntByRecTypes;
    }
}
