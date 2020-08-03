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

package org.apache.ignite.internal.processors.query.h2;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.CountDownLatch;
import java.util.stream.LongStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexAbstractSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.query.GridQueryIndexing;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.DFLT_SCHEMA;
import static org.apache.ignite.internal.util.IgniteUtils.delete;

/**
 * Index rebuild after node restart test.
 */
public class GridIndexRebuildSelfTest extends DynamicIndexAbstractSelfTest {
    /** Data size. */
    protected static final int AMOUNT = 50;

    /** Data size. */
    protected static final String CACHE_NAME = "T";

    /** Test instance to allow interaction with static context. */
    private static GridIndexRebuildSelfTest INSTANCE;

    /** Latch to signal that rebuild may start. */
    private final CountDownLatch rebuildLatch = new CountDownLatch(1);

    /** Thread pool size for build index. */
    private Integer buildIdxThreadPoolSize;

    /** GridQueryIndexing class. */
    private Class<? extends GridQueryIndexing> qryIndexingCls = BlockingIndexing.class;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration commonConfiguration(int idx) throws Exception {
        IgniteConfiguration cfg = super.commonConfiguration(idx);

        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration()
            .setMaxSize(300 * 1024L * 1024L)
            .setPersistenceEnabled(true);

        if (nonNull(buildIdxThreadPoolSize))
            cfg.setBuildIndexThreadPoolSize(buildIdxThreadPoolSize);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration serverConfiguration(int idx) throws Exception {
        IgniteConfiguration cfg = super.serverConfiguration(idx);

        if (nonNull(qryIndexingCls))
            GridQueryProcessor.idxCls = qryIndexingCls;

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        // Just in case.
        cleanPersistenceDir();

        INSTANCE = this;

        BlockingIndexing.slowRebuildIdxFut = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
        GridQueryProcessor.idxCls = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        cleanPersistenceDir();

        super.afterTestsStopped();
    }

    /**
     * Do test.
     * <p>
     * Steps are as follows:
     * <ul>
     *     <li>Put some data;</li>
     *     <li>Stop the node;</li>
     *     <li>Remove index file;</li>
     *     <li>Restart the node and block index rebuild;</li>
     *     <li>For half of the keys do cache puts <b>before</b> corresponding key
     *     has been processed during index rebuild;</li>
     *     <li>Check that:
     *         <ul>
     *             <li>For MVCC case: some keys have all versions that existed before restart, while those
     *             updated concurrently have only put version (one with mark value -1)
     *             and latest version present before node restart;</li>
     *             <li>For non MVCC case: keys updated concurrently must have mark values of -1 despite that
     *             index rebuild for them has happened after put.</li>
     *         </ul>
     *     </li>
     * </ul></p>
     * @throws Exception if failed.
     */
    @Test
    public void testIndexRebuild() throws Exception {
        IgniteEx srv = startServer();

        IgniteInternalCache cc = createAndFillTableWithIndex(srv);

        checkDataState(srv, false);

        File idxPath = indexFile(cc);

        stopAllGrids();

        assertTrue(delete(idxPath));

        srv = startServer();

        putData(srv, true);

        checkDataState(srv, true);
    }

    /**
     * Test checks that index rebuild will be with default pool size.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testDefaultCntThreadForRebuildIdx() throws Exception {
        checkCntThreadForRebuildIdx(IgniteConfiguration.DFLT_BUILD_IDX_THREAD_POOL_SIZE);
    }

    /**
     * Test checks that index rebuild uses the number of threads that specified
     * in configuration.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testCustomCntThreadForRebuildIdx() throws Exception {
        checkCntThreadForRebuildIdx(6);
    }

    /**
     * Test checks that there will be no data race between notifications about index rebuilding
     * and an indication that index has been rebuilt.
     *
     * Steps:
     * 1)Create a node with data filling;
     * 2)Stopping a node with deletion index.bin;
     * 3)Set a delay between notification and a note about index rebuilding;
     * 4)Restarting node with waiting index rebuild;
     * 5)Checking that index is not being rebuilt.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testDataRaceWhenMarkIdxRebuild() throws Exception {
        IgniteEx srv = startServer();

        IgniteInternalCache internalCache = createAndFillTableWithIndex(srv);

        File idxFile = indexFile(internalCache);

        stopAllGrids();

        assertTrue(delete(idxFile));

        BlockingIndexing.slowRebuildIdxFut = true;

        srv = startServer();

        srv.cache(CACHE_NAME).indexReadyFuture().get();

        IgniteH2Indexing idx = (IgniteH2Indexing)srv.context().query().getIndexing();

        GridH2Table tbl = idx.schemaManager().dataTable(DFLT_SCHEMA, CACHE_NAME);

        assertNotNull(tbl);

        assertFalse(tbl.rebuildFromHashInProgress());
    }

    /**
     * Check that index rebuild uses the number of threads
     * that specified in configuration.
     *
     * @param buildIdxThreadCnt Thread pool size for build index,
     *      after restart node.
     * @throws Exception if failed.
     */
    private void checkCntThreadForRebuildIdx(int buildIdxThreadCnt) throws Exception {
        qryIndexingCls = null;

        IgniteEx srv = startServer();

        IgniteInternalCache internalCache = createAndFillTableWithIndex(srv);

        int partCnt = internalCache.configuration().getAffinity().partitions();

        assertTrue(partCnt > buildIdxThreadCnt);

        File idxPath = indexFile(internalCache);

        stopAllGrids();

        assertTrue(delete(idxPath));

        buildIdxThreadPoolSize = buildIdxThreadCnt;

        srv = startServer();
        srv.cache(CACHE_NAME).indexReadyFuture().get();

        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

        long buildIdxRunnerCnt = LongStream.of(threadMXBean.getAllThreadIds()).mapToObj(threadMXBean::getThreadInfo)
            .filter(threadInfo -> threadInfo.getThreadName().startsWith("build-idx-runner")).count();

        assertEquals(buildIdxThreadCnt, buildIdxRunnerCnt);
    }

    /**
     * Creating a cache, table, index and populating data.
     *
     * @param node Node.
     * @return Cache.
     * @throws Exception if failed.
     */
    private IgniteInternalCache createAndFillTableWithIndex(IgniteEx node) throws Exception {
        requireNonNull(node);

        String cacheName = CACHE_NAME;

        execute(node, "CREATE TABLE T(k int primary key, v int) WITH \"cache_name=" + cacheName +
            ",wrap_value=false,atomicity=transactional\"");

        execute(node, "CREATE INDEX IDX ON T(v)");

        IgniteInternalCache cc = node.cachex(cacheName);

        assertNotNull(cc);

        putData(node, false);

        return cc;
    }

    /**
     * Get index file.
     *
     * @param internalCache Cache.
     * @return Index file.
     */
    protected File indexFile(IgniteInternalCache internalCache) {
        requireNonNull(internalCache);

        File cacheWorkDir = ((FilePageStoreManager)internalCache.context().shared().pageStore())
            .cacheWorkDir(internalCache.configuration());

        return cacheWorkDir.toPath().resolve(INDEX_FILE_NAME).toFile();
    }

    /**
     * Check versions presence in index tree.
     *
     * @param srv Node.
     * @param afterRebuild Whether index rebuild has occurred.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"ConstantConditions", "unchecked"})
    protected void checkDataState(IgniteEx srv, boolean afterRebuild) throws IgniteCheckedException {
        IgniteInternalCache icache = srv.cachex(CACHE_NAME);

        IgniteCache cache = srv.cache(CACHE_NAME);

        assertNotNull(icache);

        for (IgniteCacheOffheapManager.CacheDataStore store : icache.context().offheap().cacheDataStores()) {
            GridCursor<? extends CacheDataRow> cur = store.cursor();

            while (cur.next()) {
                CacheDataRow row = cur.get();

                int key = row.key().value(icache.context().cacheObjectContext(), false);

                if (!afterRebuild || key <= AMOUNT / 2)
                    assertEquals(key, cache.get(key));
                else
                    assertEquals(-1, cache.get(key));
            }
        }
    }

    /**
     * Put data to cache.
     *
     * @param node Node.
     * @throws Exception if failed.
     */
    protected void putData(Ignite node, final boolean forConcurrentPut) throws Exception {
        final IgniteCache<Integer, Integer> cache = node.cache(CACHE_NAME);

        assertNotNull(cache);

        for (int i = 1; i <= AMOUNT; i++) {
            if (forConcurrentPut) {
                // Concurrent put affects only second half of the keys.
                if (i <= AMOUNT / 2)
                    continue;

                cache.put(i, -1);

                rebuildLatch.countDown();
            }
            else {
                // Data streamer is not used intentionally in order to preserve all versions.
                for (int j = 1; j <= i; j++)
                    cache.put(i, j);
            }
        }
    }

    /**
     * Start server node.
     *
     * @return Started node.
     * @throws Exception if failed.
     */
    protected IgniteEx startServer() throws Exception {
        IgniteEx srvNode = startGrid(serverConfiguration(0));
        srvNode.active(true);
        return srvNode;
    }

    /**
     * Blocking indexing processor.
     */
    private static class BlockingIndexing extends IgniteH2Indexing {
        /** Flag to ignore first rebuild performed on initial node start. */
        private boolean firstRbld = true;

        /** Flag for slowing down {@code rebuildIdxFut} to reproduce data race. */
        static boolean slowRebuildIdxFut;

        /** {@inheritDoc} */
        @Override protected void rebuildIndexesFromHash0(
            GridCacheContext cctx,
            SchemaIndexCacheVisitorClosure clo,
            GridFutureAdapter<Void> rebuildIdxFut
        ) {
            if (!firstRbld) {
                try {
                    U.await(INSTANCE.rebuildLatch);
                }
                catch (IgniteInterruptedCheckedException e) {
                    throw new IgniteException(e);
                }
            }
            else
                firstRbld = false;

            if (slowRebuildIdxFut) {
                rebuildIdxFut.listen(fut -> {
                    try {
                        U.sleep(1_000);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        log.error("Error while slow down " + fut, e);
                    }
                });
            }

            super.rebuildIndexesFromHash0(cctx, clo, rebuildIdxFut);
        }
    }
}
