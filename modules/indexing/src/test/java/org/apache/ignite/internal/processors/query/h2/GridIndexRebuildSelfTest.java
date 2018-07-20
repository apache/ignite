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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexAbstractSelfTest;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorImpl;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Index rebuild after node restart test.
 */
public class GridIndexRebuildSelfTest extends DynamicIndexAbstractSelfTest {
    /** Data size. */
    private final static int AMOUNT = 10;

    /** Data size. */
    private final static String CACHE_NAME = "T";

    /** Test instance to allow interaction with static context. */
    private static GridIndexRebuildSelfTest INSTANCE;

    /** Latch to signal that rebuild may start. */
    private final CountDownLatch rebuildLatch = new CountDownLatch(1);

    /** Latch to signal that concurrent put may start. */
    private final Semaphore rebuildSemaphore = new Semaphore(1, true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration commonConfiguration(int idx) throws Exception {
        IgniteConfiguration cfg =  super.commonConfiguration(idx);

        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        // Just in case.
        cleanPersistenceDir();

        INSTANCE = this;
    }

    /**
     * Do test with MVCC enabled.
     */
    public void testMvccEnabled() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-7259");
        doTest(true);
    }

    /**
     * Do test with MVCC disabled.
     */
    public void testMvccDisabled() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-7259");
        doTest(false);
    }

    /**
     * Do test.<p>
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
     * </ul>
     * @param mvccEnabled MVCC flag.
     * @throws Exception if failed.
     */
    private void doTest(boolean mvccEnabled) throws Exception {
        IgniteEx srv = startServer(mvccEnabled);

        execute(srv, "CREATE TABLE T(k int primary key, v int) WITH \"cache_name=T,wrap_value=false," +
            "atomicity=transactional\"");

        execute(srv, "CREATE INDEX IDX ON T(v)");

        IgniteInternalCache cc = srv.cachex(CACHE_NAME);

        assertNotNull(cc);

        if (mvccEnabled)
            lockVersion(srv);

        putData(srv, false);

        checkDataState(srv, mvccEnabled, false);

        File cacheWorkDir = ((FilePageStoreManager)cc.context().shared().pageStore()).cacheWorkDir(cc.configuration());

        File idxPath = cacheWorkDir.toPath().resolve("index.bin").toFile();

        stopAllGrids();

        assertTrue(U.delete(idxPath));

        srv = startServer(mvccEnabled);

        putData(srv, true);

        checkDataState(srv, mvccEnabled, true);
    }

    /**
     * Check versions presence in index tree.
     * @param srv Node.
     * @param mvccEnabled MVCC flag.
     * @param afterRebuild Whether index rebuild has occurred.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"ConstantConditions", "unchecked"})
    private void checkDataState(IgniteEx srv, boolean mvccEnabled, boolean afterRebuild) throws IgniteCheckedException {
        IgniteInternalCache icache = srv.cachex(CACHE_NAME);

        IgniteCache cache = srv.cache(CACHE_NAME);

        assertNotNull(icache);

        for (IgniteCacheOffheapManager.CacheDataStore store : icache.context().offheap().cacheDataStores()) {
            GridCursor<? extends CacheDataRow> cur = store.cursor();

            while (cur.next()) {
                CacheDataRow row = cur.get();

                int key  = row.key().value(icache.context().cacheObjectContext(), false);

                if (mvccEnabled) {
                    List<IgniteBiTuple<Object, MvccVersion>> vers = store.mvccFindAllVersions(icache.context(), row.key());

                    if (!afterRebuild || key <= AMOUNT / 2)
                        assertEquals(key, vers.size());
                    else {
                        // For keys affected by concurrent put there are two versions -
                        // -1 (concurrent put mark) and newest restored value as long as put cleans obsolete versions.
                        assertEquals(2, vers.size());

                        assertEquals(-1, vers.get(0).getKey());
                        assertEquals(key, vers.get(1).getKey());
                    }
                }
                else {
                    if (!afterRebuild || key <= AMOUNT / 2)
                        assertEquals(key, cache.get(key));
                    else
                        assertEquals(-1, cache.get(key));
                }
            }
        }
    }

    /**
     * Lock coordinator version in order to keep MVCC versions in place.
     * @param node Node.
     * @throws IgniteCheckedException if failed.
     */
    private static void lockVersion(IgniteEx node) throws IgniteCheckedException {
        node.context().coordinators().requestSnapshotAsync().get();
    }

    /**
     * Put data to cache.
     * @param node Node.
     * @throws Exception if failed.
     */
    private void putData(Ignite node, final boolean forConcurrentPut) throws Exception {
        final IgniteCache<Integer, Integer> cache = node.cache(CACHE_NAME);

        assertNotNull(cache);

        for (int i = 1; i <= AMOUNT; i++) {
            if (forConcurrentPut) {
                // Concurrent put affects only second half of the keys.
                if (i <= AMOUNT / 2)
                    continue;

                rebuildSemaphore.acquire();

                cache.put(i, -1);

                rebuildLatch.countDown();

                rebuildSemaphore.release();
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
     * @param mvccEnabled MVCC flag.
     * @return Started node.
     * @throws Exception if failed.
     */
    private IgniteEx startServer(boolean mvccEnabled) throws Exception {
        // Have to do this for each starting node - see GridQueryProcessor ctor, it nulls
        // idxCls static field on each call.
        GridQueryProcessor.idxCls = BlockingIndexing.class;

        IgniteConfiguration cfg = serverConfiguration(0).setMvccEnabled(mvccEnabled);

        IgniteEx res = startGrid(cfg);

        res.active(true);

        return res;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        cleanPersistenceDir();
    }

    /**
     * Blocking indexing processor.
     */
    private static class BlockingIndexing extends IgniteH2Indexing {
        /** Flag to ignore first rebuild performed on initial node start. */
        private boolean firstRbld = true;

        /** {@inheritDoc} */
        @Override public void rebuildIndexesFromHash(String cacheName) throws IgniteCheckedException {
            if (!firstRbld)
                U.await(INSTANCE.rebuildLatch);
            else
                firstRbld = false;

            int cacheId = CU.cacheId(cacheName);

            GridCacheContext cctx = ctx.cache().context().cacheContext(cacheId);

            final GridCacheQueryManager qryMgr = cctx.queries();

            SchemaIndexCacheVisitor visitor = new SchemaIndexCacheVisitorImpl(cctx);

            visitor.visit(new TestRebuildClosure(qryMgr, cctx.mvccEnabled()));

            for (H2TableDescriptor tblDesc : tables(cacheName))
                tblDesc.table().markRebuildFromHashInProgress(false);
        }
    }

    /**
     * Test closure.
     */
    private final static class TestRebuildClosure extends RebuildIndexFromHashClosure {
        /** Seen keys set to track moment when concurrent put may start. */
        private final Set<KeyCacheObject> keys =
            Collections.newSetFromMap(new ConcurrentHashMap<KeyCacheObject, Boolean>());

        /**
         * @param qryMgr      Query manager.
         * @param mvccEnabled MVCC status flag.
         */
        TestRebuildClosure(GridCacheQueryManager qryMgr, boolean mvccEnabled) {
            super(qryMgr, mvccEnabled);
        }

        /** {@inheritDoc} */
        @Override public synchronized void apply(CacheDataRow row) throws IgniteCheckedException {
            // For half of the keys, we want to do rebuild
            // after corresponding key had been put from a concurrent thread.
            boolean keyFirstMet = keys.add(row.key()) && keys.size() > AMOUNT / 2;

            if (keyFirstMet) {
                try {
                    INSTANCE.rebuildSemaphore.acquire();
                }
                catch (InterruptedException e) {
                    throw new IgniteCheckedException(e);
                }
            }

            super.apply(row);

            if (keyFirstMet)
                INSTANCE.rebuildSemaphore.release();
        }
    }
}
