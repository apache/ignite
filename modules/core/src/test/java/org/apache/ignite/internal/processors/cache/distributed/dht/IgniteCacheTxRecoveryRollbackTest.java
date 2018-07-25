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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.CorruptedTreeException;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Assert;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class IgniteCacheTxRecoveryRollbackTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static ConcurrentHashMap<Object, Object> storeMap = new ConcurrentHashMap<>();

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try {
            for (Ignite node : G.allGrids()) {
                Collection<IgniteInternalTx> txs = ((IgniteKernal)node).context().cache().context().tm().activeTransactions();

                assertTrue("Unfinished txs [node=" + node.name() + ", txs=" + txs + ']', txs.isEmpty());
            }
        }
        finally {
            stopAllGrids();

            storeMap.clear();

            super.afterTest();
        }
    }

/*    *//**
     * @throws Exception If failed.
     *//*
    public void testNearTx1Implicit() throws Exception {
        nearTx1(null);
    }

    *//**
     * @throws Exception If failed.
     *//*
    public void testNearTx1Optimistic() throws Exception {
        nearTx1(OPTIMISTIC);
    }

    *//**
     * @throws Exception If failed.
     *//*
    public void testNearTx1Pessimistic() throws Exception {
        nearTx1(PESSIMISTIC);
    }

    *//**
     * Stop tx near node (client2), near cache tx on client1 is either committed
     * by primary or invalidated.
     *
     * @param concurrency Tx concurrency or {@code null} for implicit transaction.
     * @throws Exception If failed.
     *//*
    private void nearTx1(final TransactionConcurrency concurrency) throws Exception {
        startGrids(4);

        Ignite srv0 = grid(0);

        final IgniteCache<Integer, Integer> srvCache = srv0.createCache(cacheConfiguration(2, false, false));

        awaitPartitionMapExchange();

        client = true;

        Ignite client1 = startGrid(4);
        final Ignite client2 = startGrid(5);

        final Integer key = primaryKey(srv0.cache(DEFAULT_CACHE_NAME));

        final IgniteCache<Integer, Integer> cache1 =
            client1.createNearCache(DEFAULT_CACHE_NAME, new NearCacheConfiguration<Integer, Integer>());

        final IgniteCache<Integer, Integer> cache2 =
            client2.createNearCache(DEFAULT_CACHE_NAME, new NearCacheConfiguration<Integer, Integer>());

        cache1.put(key, 1);

        final Integer newVal = 2;

        testSpi(client2).blockMessages(GridNearTxFinishRequest.class, srv0.name());

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                log.info("Start put, concurrency: " + concurrency);

                if (concurrency != null) {
                    try (Transaction tx = client2.transactions().txStart(concurrency, REPEATABLE_READ)) {
                        cache2.put(key, newVal);

                        tx.commit();
                    }
                }
                else
                    cache2.put(key, newVal);

                return null;
            }
        });

        U.sleep(500);

        assertFalse(fut.isDone());

        testSpi(client2).waitForBlocked(GridNearTxFinishRequest.class, srv0.name());

        stopGrid(client2.name());

        try {
            fut.get();
        }
        catch (IgniteCheckedException ignore) {
            // No-op.
        }

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return newVal.equals(srvCache.get(key)) && newVal.equals(cache1.get(key));
            }
        }, 5000);

        checkData(F.asMap(key, newVal));
    }

    *//**
     * @throws Exception If failed.
     *//*
    public void testNearTx2Implicit() throws Exception {
        nearTx2(null);
    }

    *//**
     * @throws Exception If failed.
     *//*
    public void testNearTx2Optimistic() throws Exception {
        nearTx2(OPTIMISTIC);
    }

    *//**
     * @throws Exception If failed.
     *//*
    public void testNearTx2Pessimistic() throws Exception {
        nearTx2(PESSIMISTIC);
    }

    *//**
     * Stop both tx near node (client2) and primary node, near cache tx on client1 is invalidated.
     *
     * @param concurrency Tx concurrency or {@code null} for implicit transaction.
     * @throws Exception If failed.
     *//*
    private void nearTx2(final TransactionConcurrency concurrency) throws Exception {
        startGrids(4);

        Ignite srv0 = grid(0);

        srv0.createCache(cacheConfiguration(2, false, false));

        awaitPartitionMapExchange();

        client = true;

        Ignite client1 = startGrid(4);
        final Ignite client2 = startGrid(5);

        final Integer key = primaryKey(srv0.cache(DEFAULT_CACHE_NAME));

        final IgniteCache<Integer, Integer> cache1 =
            client1.createNearCache(DEFAULT_CACHE_NAME, new NearCacheConfiguration<Integer, Integer>());

        final IgniteCache<Integer, Integer> cache2 =
            client2.createNearCache(DEFAULT_CACHE_NAME, new NearCacheConfiguration<Integer, Integer>());

        cache1.put(key, 1);

        final Integer newVal = 2;

        testSpi(client2).blockMessages(GridNearTxFinishRequest.class, srv0.name());

        testSpi(srv0).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                return msg instanceof GridDhtTxFinishRequest;
            }
        });

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                log.info("Start put, concurrency: " + concurrency);

                if (concurrency != null) {
                    try (Transaction tx = client2.transactions().txStart(concurrency, REPEATABLE_READ)) {
                        cache2.put(key, newVal);

                        tx.commit();
                    }
                }
                else
                    cache2.put(key, newVal);

                return null;
            }
        });

        U.sleep(500);

        assertFalse(fut.isDone());

        testSpi(client2).waitForBlocked(GridNearTxFinishRequest.class, srv0.name());

        stopGrid(client2.name());
        stopGrid(srv0.name());

        try {
            fut.get();
        }
        catch (IgniteCheckedException ignore) {
            // No-op.
        }

        final IgniteCache<Integer, Integer> srvCache = grid(1).cache(DEFAULT_CACHE_NAME);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return newVal.equals(srvCache.get(key)) && newVal.equals(cache1.get(key));
            }
        }, 5000);

        checkData(F.asMap(key, newVal));
    }

    *//**
     * @throws Exception If failed.
     *//*
    public void testTxWithStoreImplicit() throws Exception {
        txWithStore(null, true);
    }

    *//**
     * @throws Exception If failed.
     *//*
    public void testTxWithStoreOptimistic() throws Exception {
        txWithStore(OPTIMISTIC, true);
    }

    *//**
     * @throws Exception If failed.
     *//*
    public void testTxWithStorePessimistic() throws Exception {
        txWithStore(PESSIMISTIC, true);
    }

    *//**
     * @throws Exception If failed.
     *//*
    public void testTxWithStoreNoWriteThroughImplicit() throws Exception {
        txWithStore(null, false);
    }

    *//**
     * @throws Exception If failed.
     *//*
    public void testTxWithStoreNoWriteThroughOptimistic() throws Exception {
        txWithStore(OPTIMISTIC, false);
    }

    *//**
     * @throws Exception If failed.
     *//*
    public void testTxWithStoreNoWriteThroughPessimistic() throws Exception {
        txWithStore(PESSIMISTIC, false);
    }

    *//**
     * @param concurrency Tx concurrency or {@code null} for implicit transaction.
     * @param writeThrough Store write through flag.
     * @throws Exception If failed.
     *//*
    private void txWithStore(final TransactionConcurrency concurrency, boolean writeThrough) throws Exception {
        startGrids(4);

        Ignite srv0 = grid(0);

        IgniteCache<Integer, Integer> srv0Cache = srv0.createCache(cacheConfiguration(1, true, writeThrough));

        awaitPartitionMapExchange();

        final Integer key = primaryKey(srv0Cache);

        srv0Cache.put(key, 1);

        client = true;

        Ignite client = startGrid(4);

        testSpi(srv0).blockMessages(GridNearTxPrepareResponse.class, client.name());

        final IgniteCache<Integer, Integer> clientCache = client.cache(DEFAULT_CACHE_NAME);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                log.info("Start put");

                clientCache.put(key, 2);

                return null;
            }
        });

        U.sleep(500);

        assertFalse(fut.isDone());

        testSpi(srv0).waitForBlocked(GridNearTxPrepareResponse.class, client.name());

        stopGrid(client.name());

        try {
            fut.get();
        }
        catch (IgniteCheckedException ignore) {
            // No-op.
        }

        U.sleep(1000);

        if (writeThrough)
            checkData(F.asMap(key, 1));
        else
            checkData(F.asMap(key, 2));
    }*/

    @Override
    protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeFailureHandler();
    }

    public void testUncommitFailureRecovery() throws Exception {
        BPlusTree.pageHndWrapper = (tree, hnd) -> {
            IgniteEx localIgnite = (IgniteEx) Ignition.localIgnite();

            if (!localIgnite.name().endsWith("1"))
                return hnd;

            if (hnd instanceof BPlusTree.Insert && tree.getName().contains("p-0")) {
                log.warning("Created corrupted handler -> " + tree);

                PageHandler<Object, BPlusTree.Result> delegate = (PageHandler<Object, BPlusTree.Result>) hnd;

                return new PageHandler<BPlusTree.Put, BPlusTree.Result>() {
                    @Override public BPlusTree.Result run(int cacheId, long pageId, long page, long pageAddr, PageIO io, Boolean walPlc, BPlusTree.Put arg, int lvl) throws IgniteCheckedException {
                        if (arg.row() instanceof CacheDataRow) {
                            log.warning("Invoked insert into BTree -> " + arg.row() + " " + lvl);

                            if (((CacheDataRow) arg.row()).key().hashCode() > 300)
                                throw new AssertionError("Test");
                        }

                        return delegate.run(cacheId, pageId, page, pageAddr, io, walPlc, arg, lvl);
                    }

                    @Override public boolean releaseAfterWrite(int cacheId, long pageId, long page, long pageAddr, BPlusTree.Put g, int lvl) {
                        return g.canRelease(pageId, lvl);
                    }
                };
            }

            return hnd;
        };

        IgniteEx crd = (IgniteEx) startGrids(3);

        client = true;

        IgniteEx cl = startGrid(3);

        client = false;

        IgniteCache cache = cl.getOrCreateCache(cacheConfiguration("INDEXED", 2, false, false, true));

        awaitPartitionMapExchange();

        try (Transaction tx = cl.transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
            for (int i = 0; i < 500; i++)
                cache.put(i, new IndexedObject(i));

            tx.commit();
        }

        // Node with corrupted index should be stopped automatically.
        GridTestUtils.waitForCondition(() -> crd.cluster().nodes().size() == 3, 5000);

        // Recover corrupted tree.
        BPlusTree.pageHndWrapper = (tree, hnd) -> hnd;

        // Restart grids failed during tx commit.
        for (int idx = 0; idx < 4; idx++) {
            try {
                grid(idx);
            }
            catch (IgniteIllegalStateException e) {
                startGrid(idx);
            }
        }

        startGrid(4);

        awaitPartitionMapExchange();

        for (int idx = 0; idx < 5; idx++) {
            log.warning("Checking " + idx);

            IgniteEx ignite = grid(idx);

            IgniteCache<Integer, IndexedObject> cache1 = ignite.cache("INDEXED");

            for (int i = 0; i < 500; i++) {
                IndexedObject value = cache1.get(i);

                Assert.assertNotNull(value);
                Assert.assertEquals(i, value.val);
            }
        }

        int k = 2;
    }

    static class IndexedObject {

        @QuerySqlField(index = true)
        private final int val;

        private final byte[] payload = new byte [1024];

        public IndexedObject(int val) {
            this.val = val;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IndexedObject that = (IndexedObject) o;
            return val == that.val;
        }

        @Override
        public int hashCode() {
            return Objects.hash(val);
        }
    }

    /**
     * @param node Node.
     * @return Node communication SPI.
     */
    private TestRecordingCommunicationSpi testSpi(Ignite node) {
        return (TestRecordingCommunicationSpi)node.configuration().getCommunicationSpi();
    }

    private CacheConfiguration cacheConfiguration(int backups, boolean store, boolean writeThrough) {
        return cacheConfiguration(DEFAULT_CACHE_NAME, backups, store, writeThrough, false);
    }

    /**
     * @param backups Number of backups.
     * @param store Cache store flag.
     * @param writeThrough Store write through flag.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Object> cacheConfiguration(String name, int backups, boolean store, boolean writeThrough, boolean indexed) {
        CacheConfiguration<Integer, Object> ccfg = new CacheConfiguration<>(name);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setBackups(backups);
        ccfg.setRebalanceMode(ASYNC);
        ccfg.setReadFromBackup(true);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 64));

        if (store) {
            ccfg.setWriteThrough(writeThrough);

            ccfg.setCacheStoreFactory(new TestStoreFactory());
        }

        if (indexed)
            ccfg.setIndexedTypes(Integer.class, IndexedObject.class);

        return ccfg;
    }

    /**
     * @param expData Expected cache data.
     */
    private void checkData(Map<Integer, Integer> expData) {
        assert !expData.isEmpty();

        List<Ignite> nodes = G.allGrids();

        assertFalse(nodes.isEmpty());

        for (Ignite node : nodes) {
            IgniteCache<Integer, Integer> cache = node.cache(DEFAULT_CACHE_NAME);

            for (Map.Entry<Integer, Integer> e : expData.entrySet()) {
                assertEquals("Invalid value [key=" + e.getKey() + ", node=" + node.name() + ']',
                    e.getValue(),
                    cache.get(e.getKey()));
            }
        }
    }

    /**
     *
     */
    private static class TestStoreFactory implements Factory<CacheStore<Object, Object>> {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public CacheStore<Object, Object> create() {
            return new CacheStoreAdapter() {
                @Override public Object load(Object key) throws CacheLoaderException {
                    return storeMap.get(key);
                }

                @Override public void write(Cache.Entry entry) throws CacheWriterException {
                    log.info("Store write [key=" + entry.getKey() + ", val=" + entry.getValue() + ']');

                    storeMap.put(entry.getKey(), entry.getValue());
                }

                @Override public void delete(Object key) throws CacheWriterException {
                    log.info("Store delete [key=" + key + ']');

                    storeMap.remove(key);
                }
            };
        }
    }
}
