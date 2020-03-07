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
import java.util.concurrent.CountDownLatch;
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
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

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

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration commonConfiguration(int idx) throws Exception {
        IgniteConfiguration cfg =  super.commonConfiguration(idx);

        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration()
            .setMaxSize(300*1024L*1024L)
            .setPersistenceEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        // Just in case.
        cleanPersistenceDir();

        INSTANCE = this;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
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

        execute(srv, "CREATE TABLE T(k int primary key, v int) WITH \"cache_name=T,wrap_value=false," +
            "atomicity=transactional\"");

        execute(srv, "CREATE INDEX IDX ON T(v)");

        IgniteInternalCache cc = srv.cachex(CACHE_NAME);

        assertNotNull(cc);

        putData(srv, false);

        checkDataState(srv, false);

        File cacheWorkDir = ((FilePageStoreManager)cc.context().shared().pageStore()).cacheWorkDir(cc.configuration());

        File idxPath = cacheWorkDir.toPath().resolve("index.bin").toFile();

        stopAllGrids();

        assertTrue(U.delete(idxPath));

        srv = startServer();

        putData(srv, true);

        checkDataState(srv, true);
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
        // Have to do this for each starting node - see GridQueryProcessor ctor, it nulls
        // idxCls static field on each call.
        GridQueryProcessor.idxCls = BlockingIndexing.class;

        IgniteConfiguration cfg = serverConfiguration(0);

        IgniteEx res = startGrid(cfg);

        res.active(true);

        return res;
    }

    /**
     * Blocking indexing processor.
     */
    private static class BlockingIndexing extends IgniteH2Indexing {
        /** Flag to ignore first rebuild performed on initial node start. */
        private boolean firstRbld = true;

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

            super.rebuildIndexesFromHash0(cctx, clo, rebuildIdxFut);
        }
    }
}
