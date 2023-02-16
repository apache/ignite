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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryBasicIdMapper;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cdc.CdcIndexRebuildTest.TestVal;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.WalRecordCacheGroupAware;
import org.apache.ignite.internal.pagemem.wal.record.delta.InsertRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PageDeltaRecord;
import org.apache.ignite.internal.processors.cache.persistence.IndexStorageImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
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
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
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
    public static final int GRP_CACHES_CNT = 3;

    /** */
    public static final long UNKNOWN_PAGE_ID = -1;

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
            .setFailureHandler(new StopNodeFailureHandler())
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

        testLog = new ListeningTestLogger(log);

        awaitRebuild();

        assertEquals(
            0,
            countWalGrpRecords(wp -> wp.compareTo(walStartPtr) > 0, CU.cacheGroupId(cacheName(), cacheGroupName()))
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
                        throw new IgniteCheckedException("Test error on 10 put"); // Node failure during index rebuild.

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
            testLog = new ListeningTestLogger(log);

            awaitRebuild();

            assertTrue("Rebuild must not succeed", false);
        }
        catch (Exception ignore) {
            // No-op
        }
        finally {
            BPlusTree.testHndWrapper = null;

            stopAllGrids();
        }

        testLog = new ListeningTestLogger(log);

        LogListener lsnr = LogListener.matches(
            "Recreate of index.bin don't finish before node stop, index.bin can be inconsistent. " +
                "Removing it to recreate one more time " +
                "[grpId=" + cacheGroupId(cacheName(), cacheGroupName())
        ).build();

        testLog.registerListener(lsnr);

        awaitRebuild();

        assertTrue(lsnr.check());

        assertEquals(
            0,
            countWalGrpRecords(wp -> wp.compareTo(walStartPtr) > 0, cacheGroupId(cacheName(), cacheGroupName()))
        );
    }

    /** */
    private void awaitRebuild() throws Exception {
        LogListener walDisabledLsnr = LogListener.matches(
            "WAL disabled for index partition " +
                "[name=" + cacheGroupName() + ", id=" + cacheGroupId(cacheName(), cacheGroupName()) + ']'
        ).build();

        LogListener walEnabledLsnr = LogListener.matches(
            "WAL enabled for index partition " +
                "[name=" + cacheGroupName() + ", id=" + cacheGroupId(cacheName(), cacheGroupName()) + ']'
        ).build();

        testLog.registerListener(walDisabledLsnr);
        testLog.registerListener(walEnabledLsnr);

        IgniteEx srv = startGrid(0);

        srv.cluster().state(ACTIVE);

        for (int i = 0; i < cachesCnt(); i++) {
            IgniteInternalFuture<?> rbldFut = indexRebuildFuture(srv, cacheId(DEFAULT_CACHE_NAME + i));

            if (rbldFut != null)
                rbldFut.get(10_000);
        }

        assertTrue(srv.<Integer, TestVal>cache(cacheName()).containsKey(0));

        assertTrue(walDisabledLsnr.check());
        assertTrue(walEnabledLsnr.check());

        checkIdxFile();
    }

    /** */
    private WALPointer createDataAndDeleteIndexBin() throws Exception {
        IgniteEx srv = startGrid(0);

        srv.cluster().state(ACTIVE);

        for (int i = 0; i < cachesCnt(); i++)
            produceData(srv, DEFAULT_CACHE_NAME + i);

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

        assertEquals(
            TestVal.class.getDeclaredFields().length + 1,
            srv.context().indexProcessor().indexes(cacheName).size()
        );
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
    private long countWalGrpRecords(
        Predicate<WALPointer> filter,
        long grpId
    ) throws IgniteCheckedException {
        String dir = grid(0).name().replace(".", "_");

        IteratorParametersBuilder walIterBldr = new IteratorParametersBuilder()
            .filesOrDirs(
                U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_WAL_PATH + "/" + dir, false),
                U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_WAL_ARCHIVE_PATH + "/" + dir, false)
            )
            .filter((rt, ptr) -> filter.test(ptr));

        long cntGrpRecs = 0;

        try (WALIterator walIter = new IgniteWalIteratorFactory(log).iterator(walIterBldr)) {
            while (walIter.hasNext()) {
                IgniteBiTuple<WALPointer, WALRecord> rec = walIter.next();

                if (!filter.test(rec.get1()))
                    continue;

                if (rec.get2() instanceof InsertRecord
                    && ((WalRecordCacheGroupAware)rec.get2()).groupId() == grpId) {

                    BPlusIO<?> io = ((InsertRecord)rec.get2()).io();

                    if (io instanceof IndexStorageImpl.MetaStoreLeafIO
                        || io instanceof IndexStorageImpl.MetaStoreInnerIO)
                        continue;

                    long pageId = pageId(rec.get2());

                    if (pageId != UNKNOWN_PAGE_ID && PageIdUtils.partId(pageId) != INDEX_PARTITION)
                        continue;

                    cntGrpRecs++;
                }
            }
        }

        return cntGrpRecs;
    }

    /** */
    public long pageId(WALRecord rec) {
        if (rec instanceof PageDeltaRecord)
            return ((PageDeltaRecord)rec).pageId();
        else if (rec instanceof PageSnapshot)
            return ((PageSnapshot)rec).fullPageId().pageId();

        return UNKNOWN_PAGE_ID;
    }

    /** */
    private String cacheName() {
        return DEFAULT_CACHE_NAME + (cacheGrps ? "2" : "0");
    }

    /** */
    private String cacheGroupName() {
        return cacheGrps ? GRP_NAME : null;
    }

    /** */
    private int cachesCnt() {
        return cacheGrps ? GRP_CACHES_CNT : 1;
    }
}
