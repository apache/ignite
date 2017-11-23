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
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

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
import org.apache.ignite.internal.processors.cache.mvcc.MvccCounter;
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
import org.apache.ignite.lang.IgniteCallable;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 * T.
 */
public class GridIndexRebuildSelfTest extends DynamicIndexAbstractSelfTest {
    /** Data size. */
    private final static int AMOUNT = 40;

    /** Test instance to allow interaction with static context. */
    private static GridIndexRebuildSelfTest INSTANCE;

    /** Next key to put. */
    private int nextKey = 1;

    /** Next value to put. */
    private int nextVal = 1;

    /** Latch to signal that rebuild may continue. */
    private CountDownLatch rebuildLatch;

    /** Latch to signal that concurrent put may start. */
    private CountDownLatch putLatch;

    /** */
    public GridIndexRebuildSelfTest() {
        INSTANCE = this;
    }

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
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));

        nextKey = 1;

        nextVal = 1;

        rebuildLatch = new CountDownLatch(1);

        putLatch = new CountDownLatch(1);
    }

    /**
     * Do test with MVCC enabled.
     */
    public void testMvccEnabled() throws Exception {
        doTest(true);
    }

    /**
     * Do test with MVCC disabled.
     */
    public void testMvccDisabled() throws Exception {
        doTest(false);
    }

    /**
     * Do test.
     * @param mvccEnabled MVCC flag.
     * @throws Exception if failed.
     */
    private void doTest(boolean mvccEnabled) throws Exception {
        IgniteEx srv = startServer(mvccEnabled);

        execute(srv, "CREATE TABLE T(k int primary key, v int) WITH \"cache_name=T,wrap_value=false," +
            "atomicity=transactional\"");

        execute(srv, "CREATE INDEX IDX ON T(v)");

        IgniteInternalCache cc = srv.cachex("T");

        assertNotNull(cc);

        if (mvccEnabled)
            lockVersion(srv);

        putData(srv, false);

        checkTreeState(srv, mvccEnabled, false);

        File cacheWorkDir = ((FilePageStoreManager)cc.context().shared().pageStore()).cacheWorkDir(cc.configuration());

        File idxPath = cacheWorkDir.toPath().resolve("index.bin").toFile();

        stopAllGrids();

        assertTrue(U.delete(idxPath));

        srv = startServer(mvccEnabled);

        U.await(putLatch);

        nextKey = AMOUNT / 2;

        putData(srv, true);

        rebuildLatch.countDown();

        checkTreeState(srv, mvccEnabled, true);
    }

    /**
     * Check versions presence in index tree.
     * @param srv Node.
     * @param mvccEnabled MVCC flag.
     * @param afterRebuild Whether index rebuild has occurred.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"ConstantConditions", "unchecked"})
    private void checkTreeState(IgniteEx srv, boolean mvccEnabled, boolean afterRebuild) throws IgniteCheckedException {
        IgniteInternalCache icache = srv.cachex("T");

        IgniteCache cache = srv.cache("T");

        assertNotNull(icache);

        for (IgniteCacheOffheapManager.CacheDataStore store : icache.context().offheap().cacheDataStores()) {
            GridCursor<? extends CacheDataRow> cur = store.cursor();

            while (cur.next()) {
                CacheDataRow row = cur.get();

                int key  = row.key().value(icache.context().cacheObjectContext(), false);

                if (mvccEnabled) {
                    List<T2<Object, MvccCounter>> vers = store.mvccFindAllVersions(icache.context(), row.key());

                    if (!afterRebuild || key <= AMOUNT / 2)
                        assertEquals(key, vers.size());
                    else {
                        // For keys affected by concurrent put there are two versions -
                        // -1 (concurrent put mark) and newest restored value.
                        assertEquals(2, vers.size());

                        assertEquals(-1, vers.get(0).getKey());
                        assertEquals(key, vers.get(1).getKey());
                    }
                }
                else {
                    if (!afterRebuild || key <= AMOUNT / 2)
                        assertEquals(key, cache.get(key));
                    else {
                        assertEquals(-1, cache.get(key));
                    }
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
        node.context().coordinators().requestQueryCounter(node.context().coordinators().currentCoordinator()).get();
    }

    /**
     * Put data to cache.
     * @param node Node.
     * @throws Exception if failed.
     */
    private void putData(Ignite node, final boolean forConcurrentPut) throws Exception {
        final IgniteCache<Integer, Integer> cache = node.cache("T");

        assertNotNull(cache);

        // Data streamer is not used intentionally in order to preserve all versions.
        multithreadedAsync(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                T2<Integer, Integer> t;

                while ((t = forConcurrentPut ? nextKeyAndValueForConcurrentPut() : nextKeyAndValue()) != null)
                    cache.put(t.getKey(), t.getValue());

                return null;
            }
        }, 4).get();
    }

    /**
     * @return Next key-value pair to put to cache.
     */
    private synchronized T2<Integer, Integer> nextKeyAndValue() {
        if (nextKey <= AMOUNT && nextVal <= AMOUNT) {
            T2<Integer, Integer> res = new T2<>(nextKey, nextVal);

            if (nextVal == nextKey) {
                nextVal = 1;

                nextKey++;
            }
            else
                nextVal++;

            return res;
        }

        return null;
    }

    /**
     * @return Next key-value pair for concurrent put.
     */
    private synchronized T2<Integer, Integer> nextKeyAndValueForConcurrentPut() {
        if (++nextKey <= AMOUNT)
            return new T2<>(nextKey, -1);

        return null;
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

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }

    /**
     * Blocking indexing processor.
     */
    private static class BlockingIndexing extends IgniteH2Indexing {
        /** {@inheritDoc} */
        @Override public void rebuildIndexesFromHash(String cacheName) throws IgniteCheckedException {
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
            Collections.newSetFromMap(new IdentityHashMap<KeyCacheObject, Boolean>());

        /**
         * @param qryMgr      Query manager.
         * @param mvccEnabled MVCC status flag.
         */
        TestRebuildClosure(GridCacheQueryManager qryMgr, boolean mvccEnabled) {
            super(qryMgr, mvccEnabled);
        }

        /** {@inheritDoc} */
        @Override public void apply(CacheDataRow row) throws IgniteCheckedException {
            // When we have processed half of the keys, let's signal
            // to concurrent put that it may start.
            if (keys.add(row.key()) && keys.size() == AMOUNT / 2 + 1) {
                INSTANCE.putLatch.countDown();

                U.await(INSTANCE.rebuildLatch);
            }

            super.apply(row);
        }
    }
}
