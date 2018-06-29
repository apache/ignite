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

package org.apache.ignite.internal.processors.cache.index;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 *
 */
public class DynamicIndexPdsPartialBuiltSelfTest extends DynamicIndexAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        GridQueryProcessor.idxCls = BlockingIndexing.class;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        GridQueryProcessor.idxCls = null;

        BlockingIndexing.clear();

        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60 * 1000L;
    }

    /**
     * @param idx Index.
     */
    private IgniteConfiguration pdsConfiguration(int idx) throws Exception {
        IgniteConfiguration cfg = serverConfiguration(idx);

        DataStorageConfiguration dsCfg = cfg.getDataStorageConfiguration()
            .setPageSize(2048)
            .setWalSegmentSize(10 * 1024 * 1024);

        dsCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setMaxSize(2048L * 1024 * 1024);

        return cfg;
    }

    /**
     * @param atomicityMode Atomicity mode.
     */
    private CacheConfiguration<KeyClass, ValueClass> cacheConfiguration(CacheAtomicityMode atomicityMode) {
        return super.cacheConfiguration().setAtomicityMode(atomicityMode);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIndexPartialBuilt_Atomic_1() throws Exception {
        checkPartialBuiltIndex(CacheAtomicityMode.ATOMIC, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIndexPartialBuilt_Atomic_2() throws Exception {
        checkPartialBuiltIndex(CacheAtomicityMode.ATOMIC, 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIndexPartialBuilt_Atomic_3() throws Exception {
        checkPartialBuiltIndex(CacheAtomicityMode.ATOMIC, 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIndexPartialBuilt_Tx_1() throws Exception {
        checkPartialBuiltIndex(CacheAtomicityMode.TRANSACTIONAL, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIndexPartialBuilt_Tx_2() throws Exception {
        checkPartialBuiltIndex(CacheAtomicityMode.TRANSACTIONAL, 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIndexPartialBuilt_Tx_3() throws Exception {
        checkPartialBuiltIndex(CacheAtomicityMode.TRANSACTIONAL, 3);
    }

    /**
     * @param atomicityMode Atomicity mode.
     * @param nodeCnt Node count.
     */
    private void checkPartialBuiltIndex(CacheAtomicityMode atomicityMode, final int nodeCnt) throws Exception {
        final IgniteEx node = startPdsGrids(nodeCnt);

        node.cluster().active(true);

        node.cluster().setBaselineTopology(node.cluster().currentBaselineTopology());

        createSqlCache(node, cacheConfiguration(atomicityMode));

        final int cacheSize = LARGE_CACHE_SIZE;

        put(node, 0, cacheSize);

        forceCheckpointParallel();

        // Start index operation in blocked state.
        CountDownLatch idxLatch1 = BlockingIndexing.block(node);

        @SuppressWarnings("unchecked")
        QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1));

        final IgniteInternalFuture<?> idxFut =
            queryProcessor(node).dynamicIndexCreate(CACHE_NAME, CACHE_NAME, TBL_NAME, idx, false, 0);

        idxLatch1.await();

        AtomicBoolean idxDone = new AtomicBoolean();

        final CountDownLatch idxPagesChanged = new CountDownLatch(1);

        new Thread(() -> {
            int pages = totalIndexPages();

            System.out.println(">>> index pages: " + pages);

            while (!idxDone.get()) {
                if (totalIndexPages() >= pages + cacheSize / 1000) {
                    System.out.println(">>> index pages changed: " + totalIndexPages());

                    idxPagesChanged.countDown();

                    break;
                }

                Thread.yield();
            }
        }).start();

        BlockingIndexing.unblock(node);

        idxPagesChanged.await(2000, TimeUnit.MILLISECONDS);

        System.out.println(">>> index pages before checkpoint: " + totalIndexPages());

        forceCheckpointParallel();

        System.out.println(">>> index pages after checkpoint: " + totalIndexPages());

        stopAllGridsParallel();

        boolean idxFutComplete = false;

        try {
            idxFut.get(30_000);

            idxFutComplete = true;

            log.warning("Exception has not been thrown on idxFut.get()!");
        }
        catch (SchemaOperationException | IgniteFutureTimeoutCheckedException e) {
            log.warning("Exception during index build", e);
        }

        idxDone.set(true);

        System.out.println(">>> index pages after stop: " + totalIndexPages());

        final IgniteEx nodeRestarted = startPdsGrids(nodeCnt);

        awaitPartitionMapExchange();

        assertTrue(nodeRestarted.cluster().active());

        System.out.println(">>> index pages after restart: " + totalIndexPages());

        if (idxFutComplete)
            assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_ARG_1);
        else
            assertIndexNotUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_ARG_1);

        assertSqlSimpleData(SQL_SIMPLE_FIELD_1, cacheSize - SQL_ARG_1);
    }

    /**
     * @param cnt Count.
     */
    private IgniteEx startPdsGrids(int cnt) throws Exception {
        assertTrue(cnt > 0);

        final IgniteEx srv1 = startGrid(pdsConfiguration(1));

        for (int i = 2; i <= cnt; i++)
            startGrid(pdsConfiguration(i));

        return srv1;
    }

    /**
     *
     */
    private void stopAllGridsParallel() throws InterruptedException {
        forAllGridsDoParallel("stop", g -> stopGrid(g.name(), true));
    }

    /**
     *
     */
    private void forceCheckpointParallel() throws InterruptedException {
        forAllGridsDoParallel("cp", g -> {
            try {
                forceCheckpoint(g);
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * @param threadSuffix Thread suffix.
     * @param consumer Consumer.
     */
    private void forAllGridsDoParallel(final String threadSuffix, final Consumer<Ignite> consumer) throws InterruptedException {
        List<Ignite> grids = G.allGrids();

        if (grids.isEmpty())
            return;

        if (grids.size() == 1) {
            consumer.accept(grids.iterator().next());

            return;
        }

        CountDownLatch latch = new CountDownLatch(grids.size());

        for (final Ignite g : grids) {
            new Thread(g.name() + "-" + threadSuffix) {
                public void run() {
                    try {
                        consumer.accept(g);
                    }
                    finally {
                        latch.countDown();
                    }
                }
            }.start();
        }

        latch.await();
    }

    /**
     * @param node Node.
     * @param partId Partition id.
     * @return Page store of {@code node} with specified partition id.
     */
    private FilePageStore getPageStore(Ignite node, int partId) {
        FilePageStoreManager pageStoreMgr = (FilePageStoreManager)((IgniteEx)node).context().cache().context().pageStore();

        assert pageStoreMgr != null : "Persistence is not enabled";

        try {
            return (FilePageStore)pageStoreMgr.getStore(CU.cacheId(CACHE_NAME), partId);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param node Node.
     * @return Index page store of {@code node}.
     */
    private FilePageStore getIndexPageStore(Ignite node) {
        return getPageStore(node, PageIdAllocator.INDEX_PARTITION);
    }

    /**
     * @return Total index pages count.
     */
    private int totalIndexPages() {
        return Ignition.allGrids().stream()
            .map(this::getIndexPageStore)
            .map(FilePageStore::pages)
            .reduce(Integer::sum)
            .orElse(0);
    }
}
