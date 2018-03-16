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

package org.apache.ignite.internal.processors.cache;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.store.CacheLocalStore;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStore;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.CacheNameResource;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public abstract class GridCacheAbstractCacheStorePrepareTest extends GridCommonAbstractTest {
    /** Local jdbc store on cache0 on node 0. */
    private static final TestLocalCommitStore LOCAL_COMMIT_STORE_0_0 = new TestLocalCommitStore();

    /** Local jdbc store on cache0 on node 1. */
    private static final TestLocalCommitStore LOCAL_COMMIT_STORE_0_1 = new TestLocalCommitStore();

    /** Local non-jdbc store on cache1 on node 0. */
    private static final TestLocalStore LOCAL_STORE_1_0 = new TestLocalStore();

    /** Local non-jdbc store on cache1 on node 1. */
    private static final TestLocalStore LOCAL_STORE_1_1 = new TestLocalStore();

    /** Global jdbc store on cache0 on node 0. */
    private static final TestGlobalCommitStore GLOBAL_COMMIT_STORE_0_0 = new TestGlobalCommitStore();

    /** Global jdbc store on cache0 on node 1. */
    private static final TestGlobalCommitStore GLOBAL_COMMIT_STORE_0_1 = new TestGlobalCommitStore();

    /** Global non-jdbc store on cache1 on node 0. */
    private static final TestGlobalStore GLOBAL_STORE_1_0 = new TestGlobalStore();

    /** Global non-jdbc store on cache1 on node 1. */
    private static final TestGlobalStore GLOBAL_STORE_1_1 = new TestGlobalStore();

    /** Cache, configured with local store, which supports prepare-commit. */
    private static final String CACHE_WITH_LOCAL_COMMIT_STORE = "cache0";

    /** Cache, configured with local store, which doesn't support prepare-commit */
    private static final String CACHE_WITH_LOCAL_STORE = "cache1";

    /** Cache, configured with global store, which supports prepare-commit. */
    private static final String CACHE_WITH_GLOBAL_COMMIT_STORE = "cache2";

    /** Cache, configured with global store, which doesn't support prepare-commit. */
    private static final String CACHE_WITH_GLOBAL_STORE = "cache3";

    /**
     * @param cacheName Cache name.
     * @return Configuration.
     */
    @SuppressWarnings("unchecked")
    private CacheConfiguration cache(String cacheName) {
        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setName(cacheName);
        cacheCfg.setCacheMode(getCacheMode());
        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cacheCfg.setNearConfiguration(nearConfiguration());
        cacheCfg.setCacheStoreFactory(new StoreFactory());
        cacheCfg.setWriteBehindEnabled(writeBehindEnabled());
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setWriteThrough(true);
        cacheCfg.setReadThrough(true);
        cacheCfg.setBackups(1);
        cacheCfg.setWriteBehindFlushFrequency(50);
        cacheCfg.setWriteBehindFlushThreadCount(Runtime.getRuntime().availableProcessors());

        return cacheCfg;
    }

    /**
     *
     */
    protected boolean writeBehindEnabled() {
        return false;
    }

    /**
     * @return NearCacheConfiguration.
     */
    protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /**
     * @return Cache mode.
     */
    protected abstract CacheMode getCacheMode();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cacheCfg = cache(CACHE_WITH_LOCAL_COMMIT_STORE);
        cacheCfg.setAffinity(new RendezvousAffinityFunction());

        CacheConfiguration cacheCfg2 = cache(CACHE_WITH_LOCAL_STORE);
        cacheCfg2.setAffinity(new RendezvousAffinityFunction());

        CacheConfiguration cacheCfg3 = cache(CACHE_WITH_GLOBAL_COMMIT_STORE);
        cacheCfg2.setAffinity(new RendezvousAffinityFunction());

        CacheConfiguration cacheCfg4 = cache(CACHE_WITH_GLOBAL_STORE);
        cacheCfg2.setAffinity(new RendezvousAffinityFunction());

        cfg.setCommunicationSpi(new BlockTcpCommunicationSpi());
        cfg.setCacheConfiguration(cacheCfg, cacheCfg2, cacheCfg3, cacheCfg4);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(2);

        awaitPartitionMapExchange(true, true, null);

        Thread.sleep(500);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Test populate global cache stores correctly.
     *
     * Note, that caches with local and non-local stores can't be enlisted in one transaction).
     *
     * @throws IgniteCheckedException If failed.
     */
    public void testCorrectGlobalStoresPersistence() throws IgniteCheckedException {
        IgniteEx ignite0 = grid(0);

        IgniteCache<Object, Object> cache1 = ignite0.cache(CACHE_WITH_GLOBAL_COMMIT_STORE);
        IgniteCache<Object, Object> cache2 = ignite0.cache(CACHE_WITH_GLOBAL_STORE);

        Integer key1 = primaryKey(cache1);
        Integer key2 = primaryKey(cache2);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                TransactionProxyImpl tx = (TransactionProxyImpl)ignite0.transactions().txStart(concurrency, isolation);

                cache1.put(key1, key1);
                cache2.put(key2, key2);

                tx.tx().prepare(true);

                U.sleep(100);

                assertEquals(null, GLOBAL_COMMIT_STORE_0_0.load(key1));
                assertEquals(null, GLOBAL_COMMIT_STORE_0_1.load(key1));
                assertEquals(writeBehindEnabled() ? 0 : 1, GLOBAL_COMMIT_STORE_0_0.writeCount.getAndSet(0));
                assertEquals(0, GLOBAL_COMMIT_STORE_0_1.writeCount.get());

                assertEquals(null, GLOBAL_STORE_1_0.load(key2));
                assertEquals(null, GLOBAL_STORE_1_1.load(key2));
                assertEquals(0, GLOBAL_STORE_1_0.writeCount.get());
                assertEquals(0, GLOBAL_STORE_1_1.writeCount.get());

                tx.commit();

                flushStoreIfWriteBehindEnabled();

                GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        boolean storesFlushed;

                        storesFlushed = GLOBAL_COMMIT_STORE_0_0.load(key1) == key1;

                        storesFlushed &= GLOBAL_COMMIT_STORE_0_1.load(key1) == key1;

                        storesFlushed &= GLOBAL_STORE_1_0.load(key2) == key2;

                        storesFlushed &= GLOBAL_STORE_1_1.load(key2) == key2;

                        return storesFlushed;
                    }
                }, getTestTimeout());

                assertEquals(writeBehindEnabled() ? 1 : 0, GLOBAL_COMMIT_STORE_0_0.writeCount.getAndSet(0));
                assertEquals(0, GLOBAL_COMMIT_STORE_0_1.writeCount.get());
                assertEquals(key1, cache1.get(key1));

                assertEquals(1, GLOBAL_STORE_1_0.writeCount.getAndSet(0));
                assertEquals(0, GLOBAL_STORE_1_1.writeCount.get());
                assertEquals(key2, cache2.get(key2));

                cache1.remove(key1);
                cache2.remove(key2);

                TestGlobalCommitStore.storage.clear();
                TestGlobalStore.storage.clear();
            }
        }
    }

    /**
     * Test populate local cache stores correctly.
     *
     * Note, that caches with local and non-local stores can't be enlisted in one transaction).
     *
     * @throws IgniteCheckedException If failed.
     */
    public void testCorrectLocalStoresPersistence() throws IgniteCheckedException, InterruptedException {
        IgniteEx ignite0 = grid(0);

        IgniteCache<Object, Object> cache1 = ignite0.cache(CACHE_WITH_LOCAL_STORE);
        IgniteCache<Object, Object> cache2 = ignite0.cache(CACHE_WITH_LOCAL_COMMIT_STORE);

        Integer key1 = primaryKey(cache1);
        Integer key2 = primaryKey(cache2);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                TransactionProxyImpl tx = (TransactionProxyImpl)ignite0.transactions().txStart(concurrency, isolation);

                cache1.put(key1, key1);
                cache2.put(key2, key2);

                tx.tx().prepare(true);

                U.sleep(100);

                assertEquals(null, LOCAL_COMMIT_STORE_0_0.load(key1));
                assertEquals(null, LOCAL_COMMIT_STORE_0_1.load(key1));
                assertEquals(writeBehindEnabled() ? 0 : 1, LOCAL_COMMIT_STORE_0_0.writeCount.getAndSet(0));
                assertEquals(writeBehindEnabled() ? 0 : 1, LOCAL_COMMIT_STORE_0_1.writeCount.getAndSet(0));

                assertEquals(null, LOCAL_STORE_1_0.load(key2));
                assertEquals(null, LOCAL_STORE_1_1.load(key2));
                assertEquals(0, LOCAL_STORE_1_0.writeCount.get());
                assertEquals(0, LOCAL_STORE_1_1.writeCount.get());

                tx.commit();

                flushStoreIfWriteBehindEnabled();

                GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        boolean storesFlushed;

                        storesFlushed = LOCAL_COMMIT_STORE_0_0.load(key1) == key1;

                        storesFlushed &= LOCAL_COMMIT_STORE_0_1.load(key1) == key1;

                        storesFlushed &= LOCAL_STORE_1_0.load(key2) == key2;

                        storesFlushed &= LOCAL_STORE_1_0.load(key2) == key2;

                        return storesFlushed;
                    }
                }, getTestTimeout());

                assertEquals(writeBehindEnabled() ? 1 : 0, LOCAL_COMMIT_STORE_0_0.writeCount.getAndSet(0));
                assertEquals(writeBehindEnabled() ? 1 : 0, LOCAL_COMMIT_STORE_0_1.writeCount.getAndSet(0));
                assertEquals(key1, cache1.get(key1));

                assertEquals(1, LOCAL_STORE_1_0.writeCount.getAndSet(0));
                assertEquals(1, LOCAL_STORE_1_1.writeCount.getAndSet(0));
                assertEquals(key2, cache2.get(key2));

                cache1.remove(key1);
                cache2.remove(key2);

                LOCAL_COMMIT_STORE_0_0.storage.clear();
                LOCAL_COMMIT_STORE_0_1.storage.clear();
                LOCAL_STORE_1_0.storage.clear();
                LOCAL_STORE_1_1.storage.clear();
            }
        }
    }

    /**
     * Tests that store is not affected if tx prepare fails.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void testTxFailOnLocalPrimaryNodeWithLocalStore() throws IgniteCheckedException {
        IgniteEx ignite0 = grid(0);

        IgniteCache<Object, Object> cache0 = ignite0.cache(CACHE_WITH_LOCAL_COMMIT_STORE);
        IgniteCache<Object, Object> cache1 = ignite0.cache(CACHE_WITH_LOCAL_STORE);

        commSpi(ignite(1)).blockMessage(GridDhtTxPrepareResponse.class);

        Integer key0 = primaryKey(cache0);
        Integer key1 = primaryKey(cache1);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                Transaction tx = ignite0.transactions().txStart(concurrency, isolation, 700, 2);

                cache0.put(key0, key0);
                cache1.put(key1, key1);

                tx.commitAsync();

                U.sleep(800);

                waitTxFinish(tx);

                tx.close();

                flushStoreIfWriteBehindEnabled();

                if (writeBehindEnabled()) {
                    GridTestUtils.waitForCondition(new GridAbsPredicate() {
                        @Override public boolean apply() {
                            boolean storesFlushed;

                            storesFlushed = LOCAL_COMMIT_STORE_0_0.load(key0) == key0;

                            storesFlushed &= LOCAL_COMMIT_STORE_0_1.load(key0) == key0;

                            storesFlushed &= LOCAL_STORE_1_0.load(key1) == key1;

                            storesFlushed &= LOCAL_STORE_1_0.load(key1) == key1;

                            return storesFlushed;
                        }
                    }, getTestTimeout());

                    assertEquals(key1, cache1.get(key1));
                    assertEquals(key0, cache0.get(key0));
                }
                else {
                    assertEquals(null, LOCAL_COMMIT_STORE_0_0.load(key0));
                    assertEquals(null, LOCAL_COMMIT_STORE_0_1.load(key0));
                    assertNull(cache0.get(key0));

                    assertEquals(null, LOCAL_STORE_1_0.load(key1));
                    assertEquals(null, LOCAL_STORE_1_1.load(key1));
                    assertNull(cache1.get(key1));
                }

                cache0.remove(key0);
                cache1.remove(key1);

                LOCAL_COMMIT_STORE_0_0.storage.clear();
                LOCAL_COMMIT_STORE_0_1.storage.clear();
                LOCAL_STORE_1_0.storage.clear();
                LOCAL_STORE_1_1.storage.clear();
            }
        }

        LOCAL_COMMIT_STORE_0_0.writeCount.set(0);
        LOCAL_COMMIT_STORE_0_1.writeCount.set(0);
        LOCAL_STORE_1_0.writeCount.set(0);
        LOCAL_STORE_1_1.writeCount.set(0);

        commSpi(ignite(1)).unblockMessage();
    }

    /**
     * Tests that store is not affected if tx prepare fails.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void testTxFailOnLocalPrimaryNodeWithGlobalStore() throws IgniteCheckedException {
        IgniteEx ignite0 = grid(0);

        IgniteCache<Object, Object> cache1 = ignite0.cache(CACHE_WITH_GLOBAL_COMMIT_STORE);
        IgniteCache<Object, Object> cache2 = ignite0.cache(CACHE_WITH_GLOBAL_STORE);

        commSpi(ignite0).blockMessage(GridDhtTxPrepareRequest.class);

        Integer key1 = primaryKey(cache1);
        Integer key2 = primaryKey(cache2);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {

                Transaction tx = ignite0.transactions().txStart(concurrency, isolation, 700, 2);

                cache1.put(key1, key1);
                cache2.put(key2, key2);

                tx.commitAsync();

                U.sleep(800);

                GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override
                    public boolean apply() {
                        return TransactionState.ROLLED_BACK.equals(tx.state());
                    }
                }, getTestTimeout());

                tx.close();

                flushStoreIfWriteBehindEnabled();

                if (writeBehindEnabled()) {
                    GridTestUtils.waitForCondition(new GridAbsPredicate() {
                        @Override public boolean apply() {
                            boolean storesFlushed;

                            storesFlushed = GLOBAL_COMMIT_STORE_0_0.load(key1) == key1;

                            storesFlushed &= GLOBAL_COMMIT_STORE_0_1.load(key1) == key1;

                            storesFlushed &= GLOBAL_STORE_1_0.load(key2) == key2;

                            storesFlushed &= GLOBAL_STORE_1_0.load(key2) == key2;

                            return storesFlushed;
                        }
                    }, getTestTimeout());

                    assertEquals(key1, cache1.get(key1));
                    assertEquals(key2, cache2.get(key2));
                }
                else {
                    assertEquals(null, GLOBAL_COMMIT_STORE_0_0.load(key1));
                    assertEquals(null, GLOBAL_COMMIT_STORE_0_1.load(key1));
                    assertNull(cache1.get(key1));

                    assertEquals(null, GLOBAL_STORE_1_0.load(key2));
                    assertEquals(null, GLOBAL_STORE_1_1.load(key2));
                    assertEquals(null, cache2.get(key2));
                }

                cache1.remove(key1);
                cache2.remove(key2);

                TestGlobalCommitStore.storage.clear();
                TestGlobalStore.storage.clear();
            }
        }

        GLOBAL_COMMIT_STORE_0_0.writeCount.set(0);
        GLOBAL_COMMIT_STORE_0_1.writeCount.set(0);
        GLOBAL_STORE_1_0.writeCount.set(0);
        GLOBAL_STORE_1_1.writeCount.set(0);

        commSpi(ignite0).unblockMessage();
    }

    /**
     * Tests that store is not affected if store prepare is failed.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void testPDSPrepareFailOnLocalPrimaryNodeWithLocalStore() throws IgniteCheckedException {
        IgniteEx ignite0 = grid(0);

        IgniteCache<Object, Object> cache0 = ignite0.cache(CACHE_WITH_LOCAL_COMMIT_STORE);
        IgniteCache<Object, Object> cache1 = ignite0.cache(CACHE_WITH_LOCAL_STORE);

        Integer key0 = primaryKey(cache0);
        Integer key1 = primaryKey(cache1);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                LOCAL_COMMIT_STORE_0_0.writeFails = true;

                Transaction tx = ignite0.transactions().txStart(concurrency, isolation);

                cache0.put(key0, key0);
                cache1.put(key1, key1);

                try {
                    tx.commit();
                }
                catch (Exception ignore) {
                    //No-op.
                }

                waitTxFinish(tx);

                tx.close();

                flushStoreIfWriteBehindEnabled();

                if (writeBehindEnabled()) {
                    GridTestUtils.waitForCondition(new GridAbsPredicate() {
                        @Override public boolean apply() {
                            boolean storesFlushed;

                            storesFlushed = LOCAL_COMMIT_STORE_0_0.load(key0) == key0;

                            storesFlushed &= LOCAL_COMMIT_STORE_0_1.load(key0) == key0;

                            storesFlushed &= LOCAL_STORE_1_0.load(key1) == key1;

                            storesFlushed &= LOCAL_STORE_1_0.load(key1) == key1;

                            return storesFlushed;
                        }
                    }, getTestTimeout());

                    assertEquals(key0, cache0.get(key0));
                    assertEquals(key1, cache1.get(key1));
                }
                else {
                    assertEquals(null, LOCAL_COMMIT_STORE_0_0.load(key0));
                    assertEquals(null, LOCAL_COMMIT_STORE_0_1.load(key0));
                    assertNull(cache0.get(key0));

                    assertEquals(null, LOCAL_STORE_1_0.load(key1));
                    assertEquals(null, LOCAL_STORE_1_1.load(key1));
                    assertNull(cache1.get(key1));
                }

                cache0.remove(key0);
                cache1.remove(key1);

                LOCAL_COMMIT_STORE_0_0.storage.clear();
                LOCAL_COMMIT_STORE_0_1.storage.clear();
                LOCAL_STORE_1_0.storage.clear();
                LOCAL_STORE_1_1.storage.clear();
            }
        }

        LOCAL_COMMIT_STORE_0_0.writeCount.set(0);
        LOCAL_COMMIT_STORE_0_1.writeCount.set(0);
        LOCAL_STORE_1_0.writeCount.set(0);
        LOCAL_STORE_1_1.writeCount.set(0);
    }

    /**
     * Tests that store is not affected if store prepare is failed.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void testPDSPrepareFailOnLocalPrimaryNodeWithGlobalStore() throws IgniteCheckedException {
        IgniteEx ignite0 = grid(0);

        IgniteCache<Object, Object> cache2 = ignite0.cache(CACHE_WITH_GLOBAL_COMMIT_STORE);
        IgniteCache<Object, Object> cache3 = ignite0.cache(CACHE_WITH_GLOBAL_STORE);

        Integer key2 = primaryKey(cache2);
        Integer key3 = primaryKey(cache3);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            GLOBAL_COMMIT_STORE_0_0.writeFails = true;

            Transaction tx = ignite0.transactions().txStart(concurrency, TransactionIsolation.REPEATABLE_READ);

            cache2.put(key2, key2);
            cache3.put(key3, key3);

            try {
                tx.commit();
            }
            catch (Exception ignore) {
                //No-op.
            }

            waitTxFinish(tx);

            tx.close();

            flushStoreIfWriteBehindEnabled();

            if (writeBehindEnabled()) {
                GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        boolean storesFlushed;

                        storesFlushed = GLOBAL_COMMIT_STORE_0_0.load(key2) == key2;

                        storesFlushed &= GLOBAL_STORE_1_0.load(key3) == key3;

                        return storesFlushed;
                    }
                }, getTestTimeout());

                assertEquals(key2, cache2.get(key2));
                assertEquals(key3, cache3.get(key3));
            }
            else {
                assertEquals(null, GLOBAL_COMMIT_STORE_0_0.load(key2));
                assertEquals(null, GLOBAL_COMMIT_STORE_0_1.load(key2));
                assertEquals(null, cache2.get(key2));

                assertEquals(null, GLOBAL_STORE_1_0.load(key3));
                assertEquals(null, GLOBAL_STORE_1_1.load(key3));
                assertEquals(null, cache3.get(key3));
            }

            cache2.remove(key2);
            cache3.remove(key3);

            TestGlobalCommitStore.storage.clear();
            TestGlobalStore.storage.clear();
        }

        GLOBAL_COMMIT_STORE_0_0.writeCount.set(0);
        GLOBAL_COMMIT_STORE_0_1.writeCount.set(0);
        GLOBAL_STORE_1_0.writeCount.set(0);
        GLOBAL_STORE_1_1.writeCount.set(0);
    }

    /**
     * @param ignite Node.
     * @return Communication SPI.
     */
    protected BlockTcpCommunicationSpi commSpi(Ignite ignite) {
        return ((BlockTcpCommunicationSpi)ignite.configuration().getCommunicationSpi());
    }

    /**
     *
     */
    @CacheLocalStore
    private static class TestLocalCommitStore extends MapBasedCacheStore {
        /** {@inheritDoc} */
//        @Override public boolean supports2PhaseCommit() {
//            return true;
//        }
    }

    /**
     *
     */
    @CacheLocalStore
    private static class TestLocalStore extends MapBasedCacheStore {
    }

    /**
     *
     */
    private static class TestGlobalCommitStore extends MapBasedCacheStore {
        /** Storage. */
        public static HashMap<Integer, Integer> storage = new HashMap<>();

        /** {@inheritDoc} */
        @Override protected HashMap<Integer, Integer> storage() {
            return storage;
        }

        /** {@inheritDoc} */
//        @Override public boolean supports2PhaseCommit() {
//            return true;
//        }
    }

    /**
     *
     */
    private static class TestGlobalStore extends MapBasedCacheStore {
        /** Storage. */
        public static HashMap<Integer, Integer> storage = new HashMap<>();

        /** {@inheritDoc} */
        @Override protected HashMap<Integer, Integer> storage() {
            return storage;
        }
    }

    /**
     * Cache store, which stores data in HashMap.
     * If 2-phase-commit is supported, than data is put into state on prepare step.
     * On commit step, data is persisted into storage.
     */
    private static class MapBasedCacheStore extends CacheStoreAdapter<Integer, Integer> {
        /** Storage. */
        public HashMap<Integer, Integer> storage = new HashMap<>();

        /** State. */
        public HashMap<Integer, Integer> state = new HashMap<>();

        /** Whether commit step must fail with exception. */
        public boolean commitFails;

        /** Whether write must fail with exception. */
        public boolean writeFails;

        /** Write count. Write is performed on prepare step if 2-pahe-commit is enabled. */
        public AtomicInteger writeCount = new AtomicInteger(0);

        private boolean writeBehindEnabled;

        /**
         *
         */
        protected HashMap<Integer, Integer> storage() {
            return storage;
        }

        /** {@inheritDoc} */
        @Override public Integer load(Integer key) throws CacheLoaderException {
            Object val = storage().get(key);

            if (val instanceof IgniteBiTuple)
                return ((IgniteBiTuple<Integer, Object>)val).get1();
            else
                return (Integer)val;
        }

        StackTraceElement[] lastThread;

        /** {@inheritDoc} */
        @Override public void write(
            Cache.Entry<? extends Integer, ? extends Integer> entry) throws CacheWriterException {
            if (writeFails) {
                writeFails = false;

                throw new CacheWriterException();
            }

//            if (supports2PhaseCommit() && !writeBehindEnabled)
//                state.put(entry.getKey(), entry.getValue());
//            else
//                storage().put(entry.getKey(), entry.getValue());

            writeCount.incrementAndGet();

            lastThread = Thread.currentThread().getStackTrace();
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            state.clear();

            storage().clear();
        }

        /** {@inheritDoc} */
        @Override public void sessionEnd(boolean commit) {
            super.sessionEnd(commit);

//            if (!supports2PhaseCommit())
//                return;

            if (commit) {
                if (commitFails)
                    throw new CacheWriterException();

                storage().putAll(state);
            }
            else
                state.clear();
        }

        /**
         * @param writeBehindEnabled Write behind enabled.
         */
        public MapBasedCacheStore withWriteBehindEnabled(boolean writeBehindEnabled) {
            this.writeBehindEnabled = writeBehindEnabled;

            return this;
        }
    }

    /**
     *
     */
    static class StoreFactory implements Factory<CacheStore> {
        /** */
        @IgniteInstanceResource
        private Ignite node;

        /** */
        @CacheNameResource
        private String cacheName;

        /** {@inheritDoc} */
        @Override public CacheStore create() {
            String igniteInstanceName = node.configuration().getIgniteInstanceName();

            boolean writeBehindEnabled = false;

            for (CacheConfiguration configuration : node.configuration().getCacheConfiguration()) {
                if (configuration.getName().equals(cacheName)) {
                    writeBehindEnabled = configuration.isWriteBehindEnabled();

                    break;
                }
            }

            if (CACHE_WITH_LOCAL_COMMIT_STORE.equals(cacheName) && igniteInstanceName.endsWith("0"))
                return LOCAL_COMMIT_STORE_0_0.withWriteBehindEnabled(writeBehindEnabled);
            else if (CACHE_WITH_LOCAL_COMMIT_STORE.equals(cacheName) && igniteInstanceName.endsWith("1"))
                return LOCAL_COMMIT_STORE_0_1.withWriteBehindEnabled(writeBehindEnabled);
            else if (CACHE_WITH_LOCAL_STORE.equals(cacheName) && igniteInstanceName.endsWith("0"))
                return LOCAL_STORE_1_0.withWriteBehindEnabled(writeBehindEnabled);
            else if (CACHE_WITH_LOCAL_STORE.equals(cacheName) && igniteInstanceName.endsWith("1"))
                return LOCAL_STORE_1_1.withWriteBehindEnabled(writeBehindEnabled);
            else if (CACHE_WITH_GLOBAL_COMMIT_STORE.equals(cacheName) && igniteInstanceName.endsWith("0"))
                return GLOBAL_COMMIT_STORE_0_0.withWriteBehindEnabled(writeBehindEnabled);
            else if (CACHE_WITH_GLOBAL_COMMIT_STORE.equals(cacheName) && igniteInstanceName.endsWith("1"))
                return GLOBAL_COMMIT_STORE_0_1.withWriteBehindEnabled(writeBehindEnabled);
            else if (CACHE_WITH_GLOBAL_STORE.equals(cacheName) && igniteInstanceName.endsWith("0"))
                return GLOBAL_STORE_1_0.withWriteBehindEnabled(writeBehindEnabled);
            else if (CACHE_WITH_GLOBAL_STORE.equals(cacheName) && igniteInstanceName.endsWith("1"))
                return GLOBAL_STORE_1_1.withWriteBehindEnabled(writeBehindEnabled);

            return null;
        }
    }

    /**
     *
     */
    protected static class BlockTcpCommunicationSpi extends TcpCommunicationSpi {
        /** Message class to block. */
        volatile Class msgCls;

        /** How much time message was blocked. */
        public AtomicInteger blockedTimes = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            Class msgCls0 = msgCls;

            if (msgCls0 != null && msg instanceof GridIoMessage
                && ((GridIoMessage)msg).message().getClass().equals(msgCls)) {
                blockedTimes.incrementAndGet();

                return;
            }

            super.sendMessage(node, msg, ackC);
        }

        /**
         * @param clazz Class of messages which will be block.
         */
        public void blockMessage(Class clazz) {
            msgCls = clazz;
        }

        /**
         * Unlock all message.
         */
        public void unblockMessage() {
            msgCls = null;
        }
    }

    /**
     *
     */
    private void flushStoreIfWriteBehindEnabled() throws IgniteCheckedException {
        for (Ignite ignite : G.allGrids()) {
            for (String cacheName : ignite.cacheNames())
                internalCache(ignite, cacheName).context().store().forceFlush();
        }
    }

    /**
     * @param tx Tx.
     */
    private void waitTxFinish(Transaction tx) throws IgniteInterruptedCheckedException {
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                if (writeBehindEnabled())
                    return TransactionState.COMMITTED.equals(tx.state());
                else
                    return TransactionState.ROLLED_BACK.equals(tx.state());
            }
        }, getTestTimeout());
    }


}
