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
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryBasicIdMapper;
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
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.BTREE_PAGE_INSERT;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheGroupId;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_GRP_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.query.schema.management.SortedIndexDescriptorFactory.H2_TREE;

/** */
@RunWith(Parameterized.class)
@SuppressWarnings({"resource"})
public class WalDisabledDuringIndexRecreateTest extends GridCommonAbstractTest {
    /** Batches count. */
    public static final int ENTRIES_CNT = 1_000;

    /** */
    public static final String GRP_NAME = "my-group";

    /** */
    private ListeningTestLogger testLog;

    /** */
    @Parameterized.Parameter()
    public boolean cacheGrps;

    /** */
    @Parameterized.Parameters(name = "cacheGroups={0}")
    public static Iterable<Object> data() {
        return Arrays.asList(true, false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(testLog)
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
    public void testDisabled() throws Exception {
        WALPointer walStartPtr = createDataAndDeleteIndexBin();

        awaitRebuild();

        assertEquals(
            (Long)3L,
            countWalRecordsByTypes(wp -> wp.compareTo(walStartPtr) > 0).getOrDefault(BTREE_PAGE_INSERT, 0L)
        );
    }

    /** */
    @Test
    public void testRestartInCaseOfFailure() throws Exception {
        WALPointer walStartPtr = createDataAndDeleteIndexBin();

        String treeName = BPlusTree.treeName(
            new BinaryBasicIdMapper().typeId(TestVal.class.getName()) + "_"
                + TestVal.class.getSimpleName().toUpperCase() + "_F0_IDX",
            H2_TREE
        );

        try {
            AtomicInteger errCntr = new AtomicInteger(10);

            // Setting up wrapper that will fail on 10 put operation.
            BPlusTree.testHndWrapper = (tree, hnd) -> {
                if (!tree.name().equals(treeName))
                    return hnd;

                PageHandler<Object, BPlusTree.Result> delegate = (PageHandler<Object, BPlusTree.Result>)hnd;

                return new PageHandler<BPlusTree.Get, BPlusTree.Result>() {
                    @Override public BPlusTree.Result run(
                        int cacheId,
                        long pageId,
                        long page,
                        long pageAddr,
                        PageIO io,
                        Boolean walPlc,
                        BPlusTree.Get arg,
                        int lvl,
                        IoStatisticsHolder statHolder
                    ) throws IgniteCheckedException {
                        if (arg instanceof BPlusTree.Put && errCntr.decrementAndGet() == 0)
                            throw new IgniteCheckedException("Test error!");

                        return delegate.run(cacheId, pageId, page, pageAddr, io, walPlc, arg, lvl, statHolder);
                    }

                    @Override public boolean releaseAfterWrite(
                        int cacheId, long pageId, long page, long pageAddr, BPlusTree.Get g, int lvl
                    ) {
                        return g.canRelease(pageId, lvl);
                    }
                };
            };

            try {
                awaitRebuild();

                throw new RuntimeException("Rebuild must not succeed!");
            }
            catch (Exception ignore) {
                BPlusTree.testHndWrapper = null;

                stopAllGrids();
            }

            LogListener lsnr = LogListener.matches(
                "Recreate of index.bin don't finish before node stop, index.bin can be inconsistent. " +
                    "Removing it to recreate one more time " +
                    "[grpId=" + cacheGroupId(cacheName(), cacheGroupName())
            ).build();

            testLog = new ListeningTestLogger(log);

            testLog.registerListener(lsnr);

            awaitRebuild();

            assertTrue(lsnr.check());

            assertEquals(
                (Long)6L,
                countWalRecordsByTypes(wp -> wp.compareTo(walStartPtr) > 0).getOrDefault(BTREE_PAGE_INSERT, 0L)
            );
        }
        finally {
            BPlusTree.testHndWrapper = null;

            testLog = null;
        }
    }

    /** */
    private void awaitRebuild() throws Exception {
        IgniteEx srv = startGrid(0);

        srv.cluster().state(ACTIVE);

        indexRebuildFuture(srv, cacheId(cacheName())).get(getTestTimeout());

        checkIdxFile();
    }

    /** */
    private WALPointer createDataAndDeleteIndexBin() throws Exception {
        IgniteEx srv = startGrid(0);

        srv.cluster().state(ACTIVE);

        for (int i = 0; i < (cacheGrps ? 1 : 3); i++)
            produceData(srv, cacheName() + (i > 0 ? i + 1 : ""));

        WALPointer walPrtBefore = srv.context().cache().context().wal().lastWritePointer();

        File idx = checkIdxFile();

        stopGrid(0);

        U.delete(idx);

        return walPrtBefore;
    }

    /** */
    private void produceData(IgniteEx srv, String cacheName) {
        IgniteCache<Integer, TestVal> cache = srv.getOrCreateCache(
            new CacheConfiguration<Integer, TestVal>(cacheName)
                .setGroupName(cacheGroupName())
                .setIndexedTypes(Integer.class, TestVal.class));

        for (int i = 0; i < ENTRIES_CNT; i++)
            cache.put(i, new TestVal());

        assertEquals(3, srv.context().indexProcessor().indexes(cacheName).size());
    }

    /** */
    private File checkIdxFile() throws IgniteCheckedException {
        String dirName = cacheGrps ? (CACHE_GRP_DIR_PREFIX + cacheGroupName()) : (CACHE_DIR_PREFIX + cacheName());

        File idxFile = new File(U.resolveWorkDirectory(
            U.defaultWorkDirectory(),
            DFLT_STORE_DIR + File.separatorChar + grid(0).name().replace(".", "_") + File.separatorChar + dirName,
            false
        ), INDEX_FILE_NAME);

        assertTrue("Index file not found", idxFile.exists());

        return idxFile;
    }

    /** @param filter Predicate. */
    private Map<RecordType, Long> countWalRecordsByTypes(
        Predicate<WALPointer> filter
    ) throws IgniteCheckedException {
        String dir = grid(0).name().replace(".", "_");

        IteratorParametersBuilder beforeIdxRemoveBldr = new IteratorParametersBuilder()
            .filesOrDirs(
                U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_WAL_PATH + "/" + dir, false),
                U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_WAL_ARCHIVE_PATH + "/" + dir, false)
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

    /** */
    private String cacheName() {
        return cacheGrps ? (DEFAULT_CACHE_NAME + "2") : DEFAULT_CACHE_NAME;
    }

    /** */
    private String cacheGroupName() {
        return cacheGrps ? GRP_NAME : null;
    }
}
