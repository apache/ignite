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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.processors.cache.CacheStoppedException;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class TxOnCachesStopTest extends GridCommonAbstractTest {
    /** Cache1 name. */
    private static final String CACHE_1_NAME = "cache1";

    /** Cache2 name. */
    private static final String CACHE_2_NAME = "cache2";

    /** rnd instance. */
    private static final GridRandom rnd = new GridRandom();

    /** */
    private CacheConfiguration<Integer, byte[]> destroyCacheCfg;

    /** */
    private CacheConfiguration<Integer, byte[]> surviveCacheCfg;

    /** */
    private static final int CACHE_CNT = 30;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();
        cfg.setCommunicationSpi(commSpi);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(100 * 1024 * 1024).setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration<Integer, byte[]> ccfg1 = new CacheConfiguration<>();

        ccfg1.setName(CACHE_1_NAME);
        ccfg1.setBackups(1);
        ccfg1.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg1.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg1.setAffinity(new RendezvousAffinityFunction(false, 32));

        destroyCacheCfg = ccfg1;

        CacheConfiguration<Integer, byte[]> ccfg2 = new CacheConfiguration<>();

        ccfg2.setName(CACHE_2_NAME);
        ccfg2.setBackups(1);
        ccfg2.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg2.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg2.setAffinity(new RendezvousAffinityFunction(false, 32));

        surviveCacheCfg = ccfg2;

        cfg.setCacheConfiguration(destroyCacheCfg, surviveCacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        grid(0).destroyCache(destroyCacheCfg.getName());
        grid(0).destroyCache(surviveCacheCfg.getName());

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxOnCacheStopNoMessageBlock() throws Exception {
        runTxOnCacheStop(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxOnCacheStopWithMessageBlock() throws Exception {
        runTxOnCacheStop(true);
    }

    /**
     * @param block {@code True} To block GridNearTxPrepareRequest message.
     */
    private void runTxOnCacheStop(boolean block) throws Exception {
        startGridsMultiThreaded(2);

        IgniteEx ig = startClientGrid("client");

        ig.cluster().active(true);

        for (TransactionConcurrency conc : TransactionConcurrency.values()) {
            for (TransactionIsolation iso : TransactionIsolation.values())
                runTxOnCacheStop(conc, iso, ig, block);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxOnCacheStopInMid() throws Exception {
        startGridsMultiThreaded(2);

        IgniteEx ig = startClientGrid("client");

        ig.cluster().active(true);

        for (TransactionConcurrency conc : TransactionConcurrency.values()) {
            for (TransactionIsolation iso : TransactionIsolation.values())
                runCacheStopInMidTx(conc, iso, ig);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticTxMappedOnPMETopology() throws Exception {
        Assume.assumeFalse(MvccFeatureChecker.forcedMvcc());

        startGridsMultiThreaded(1);

        Ignite client = startClientGrid("client");

        client.cluster().active(true);

        awaitPartitionMapExchange(true, true, null);

        final IgniteCache<Integer, byte[]> cache = client.getOrCreateCache(destroyCacheCfg);
        final IgniteCache<Integer, byte[]> cache2 = client.getOrCreateCache(surviveCacheCfg);

        final TestRecordingCommunicationSpi srvSpi = TestRecordingCommunicationSpi.spi(grid(0));

        CountDownLatch destroyLatch = new CountDownLatch(1);

        srvSpi.blockMessages((node, msg) -> (msg instanceof GridDhtPartitionsFullMessage));

        try (Transaction tx = client.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
            cache2.put(100, new byte[1024]);
            cache.put(100, new byte[1024]);

            GridTestUtils.runAsync(() -> {
                grid(0).destroyCache(destroyCacheCfg.getName());

                destroyLatch.countDown();
            });

            destroyLatch.await();

            IgniteFuture commitFut = tx.commitAsync();

            srvSpi.stopBlock();

            commitFut.get(10_000);

            fail("Transaction should be rolled back.");
        }
        catch (IgniteFutureTimeoutException fte) {
            srvSpi.stopBlock();

            fail("Partition map exchange hangs [err=" + fte + ']');
        }
        catch (IgniteException e) {
            srvSpi.stopBlock();

            assertTrue(X.hasCause(e, CacheInvalidStateException.class) || X.hasCause(e, IgniteException.class));
        }
    }

    /**
     * @param conc Concurrency mode.
     * @param iso Isolation level.
     * @param ig Client node.
     * @param runConc {@code true} if a cache should be destroyed concurrently.
     * @throws Exception If Failed.
     */
    private void runTxOnCacheStop(
        TransactionConcurrency conc,
        TransactionIsolation iso,
        Ignite ig,
        boolean runConc
    ) throws Exception {
        if ((conc == TransactionConcurrency.OPTIMISTIC) && (MvccFeatureChecker.forcedMvcc()))
            return;

        if (log.isInfoEnabled()) {
            log.info("Starting runTxOnCacheStop " +
                "[concurrency=" + conc + ", isolation=" + iso + ", blockPrepareRequests=" + !runConc + ']');
        }

        CountDownLatch destroyLatch = new CountDownLatch(1);

        final IgniteCache<Integer, byte[]> cache = ig.getOrCreateCache(destroyCacheCfg);

        final IgniteCache<Integer, byte[]> cache2 = ig.getOrCreateCache(surviveCacheCfg);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(ig);

        IgniteInternalFuture f0 = GridTestUtils.runAsync(() -> {
            try {
                destroyLatch.await();

                IgniteInternalFuture f = GridTestUtils.runAsync(() -> {
                    doSleep(rnd.nextInt(500));

                    spi.stopBlock();
                });

                cache.destroy();

                f.get();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });

        spi.blockMessages((node, msg) -> {
            if (msg instanceof GridNearTxPrepareRequest) {
                destroyLatch.countDown();

                return runConc;
            }

            return false;
        });

        IgniteInternalFuture f1 = GridTestUtils.runAsync(() -> {
            byte[] val = new byte[1024];

            try (Transaction tx = ig.transactions().txStart(conc, iso, 1_000, 2)) {
                cache.put(100, val);

                cache2.put(100, val);

                tx.commit();
            }
            catch (IgniteException e) {
                assertTrue(X.hasCause(e, IgniteTxTimeoutCheckedException.class)
                    || X.hasCause(e, CacheInvalidStateException.class) || X.hasCause(e, IgniteException.class));
            }
        });

        f1.get();
        f0.get();

        try {
            assertEquals(cache2.get(100), cache.get(100));
        }
        catch (IllegalStateException e) {
            assertTrue(X.hasCause(e, CacheStoppedException.class));
        }

        spi.stopBlock();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticTransactionsOnCacheDestroy() throws Exception {
        Assume.assumeFalse(MvccFeatureChecker.forcedMvcc());

        startGridsMultiThreaded(3);

        ArrayList<Ignite> clients = new ArrayList<>();
        for (int ci = 0; ci < 2; ++ci)
            clients.add(startClientGrid("client-" + ci));

        clients.get(0).cluster().active(true);

        for (TransactionIsolation iso : TransactionIsolation.values()) {
            grid(0).getOrCreateCaches(createCacheConfigurations());

            // Make sure that all caches are started.
            awaitPartitionMapExchange();

            testConcurrentTransactionsOnCacheDestroy(clients, OPTIMISTIC, iso);

            // Make sure that all caches are stopped.
            awaitPartitionMapExchange();
        }
    }

    /**
     * Creates a list of cache configurations.
     *
     * @return List of cache configurations.
     */
    private List<CacheConfiguration> createCacheConfigurations() {
        String GRP_NAME = "test-destroy-group";

        List<CacheConfiguration> cacheCfgs = new ArrayList<>(CACHE_CNT);

        for (int i = 0; i < CACHE_CNT; ++i) {
            CacheConfiguration<Integer, byte[]> c = new CacheConfiguration<>("test-cache-" + i);
            c.setBackups(2);
            c.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            c.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            c.setAffinity(new RendezvousAffinityFunction(false, 32));
            c.setGroupName(GRP_NAME);

            cacheCfgs.add(c);
        }

        return cacheCfgs;
    }

    /**
     * @param clients Client nodes that are used for initiating transactions.
     * @param conc Transaction concurrency mode.
     * @param iso Transaction isolation.
     * @throws Exception If failed.
     */
    private void testConcurrentTransactionsOnCacheDestroy(
        final ArrayList<Ignite> clients,
        TransactionConcurrency conc,
        TransactionIsolation iso
    ) throws Exception {
        if (log.isInfoEnabled()) {
            log.info("Starting testConcurrentTransactionsOnCacheDestroy " +
                "[concurrency=" + conc + ", isolation=" + iso + ']');
        }

        final AtomicBoolean stopTxLoad = new AtomicBoolean();
        final AtomicInteger cacheIdxToBeDestroyed = new AtomicInteger(-1);

        IgniteInternalFuture txLoadFut = startTxLoad(stopTxLoad, cacheIdxToBeDestroyed, clients, conc, iso);

        try {
            for (int i = 0; i < CACHE_CNT; ++i) {
                int clientIdx = (i % clients.size());

                IgniteInternalFuture destFut = GridTestUtils.runAsync(() ->
                    clients.get(clientIdx).destroyCache("test-cache-" + cacheIdxToBeDestroyed.incrementAndGet())
                );

                try {
                    destFut.get(15, TimeUnit.SECONDS);
                }
                catch (IgniteCheckedException e) {
                    fail("Looks like PME hangs [err=" + e + ']');
                }
            }
        }
        catch (Throwable t) {
            fail("Unexpected error [err=" + t + ']');
        }

        stopTxLoad.set(true);

        txLoadFut.get();
    }

    /**
     * Starts transactional load.
     *
     * @param stopTxLoad Boolean flag that is used to stop transactional load.
     * @param cacheIdxToBeDestroyed Variable that allows to get an index of destroyed cache.
     * @param clients Client nodes that are used for initiating transactions.
     * @param concurrency Transaction concurrency mode.
     * @param isolation Transaction isolation.
     * @return TxLoad future.
     */
    private IgniteInternalFuture startTxLoad(
        final AtomicBoolean stopTxLoad,
        final AtomicInteger cacheIdxToBeDestroyed,
        final List<Ignite> clients,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation) {
        final GridCompoundFuture fut = new GridCompoundFuture();

        for (Ignite c : clients) {
            for (int i = 0; i < CACHE_CNT; ++i)
                c.getOrCreateCache("test-cache-" + i);
        }

        clients.forEach(c -> {
            fut.add(GridTestUtils.runAsync(() -> {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                List<IgniteCache<Integer, byte[]>> caches = new ArrayList<>();

                for (int i = 0; i < CACHE_CNT; ++i) {
                    IgniteCache<Integer, byte[]> testCache = c.cache("test-cache-" + i);

                    if (testCache == null) {
                        throw new IllegalStateException(
                            "Cache test-cache-" + i + " is not started " +
                                "on client node " + c.configuration().getIgniteInstanceName());
                    }

                    caches.add(testCache);
                }

                byte[] val = new byte[128];

                while (!stopTxLoad.get()) {
                    try (Transaction tx = c.transactions().txStart(concurrency, isolation)) {
                        int cacheIdx = cacheIdxToBeDestroyed.get();

                        caches.get(Math.max(0, cacheIdx)).put(rnd.nextInt(), val);
                        caches.get(rnd.nextInt(Math.min(cacheIdx + 1, caches.size() - 1), caches.size())).put(rnd.nextInt(), val);

                        doSleep(200);

                        tx.commit();
                    }
                    // Expected exceptions:
                    catch (TransactionRollbackException | CacheException e) {
                        // Failed to prepare the transaction (transaction is marked as rolled back).
                        if (!X.hasCause(e, TransactionRollbackException.class))
                            throw e;
                    }
                    catch (IgniteException | IllegalStateException e) {
                        // Failed to perform cache operation (cache is stopped).
                    }
                }
            }, "tx-load-" + c.configuration().getIgniteInstanceName()));
        });

        fut.markInitialized();

        return fut;
    }

    /**
     * @param conc Concurrency mode.
     * @param iso Isolation level.
     * @param ig Client node.
     * @throws Exception If failed.
     */
    private void runCacheStopInMidTx(TransactionConcurrency conc, TransactionIsolation iso, Ignite ig) throws Exception {
        if ((conc == TransactionConcurrency.OPTIMISTIC) && (MvccFeatureChecker.forcedMvcc()))
            return;

        if (log.isInfoEnabled())
            log.info("Starting runCacheStopInMidTx [concurrency=" + conc + ", isolation=" + iso + ']');

        CountDownLatch destroyLatch = new CountDownLatch(1);

        CountDownLatch putLatch = new CountDownLatch(1);

        final IgniteCache<Integer, byte[]> cache = ig.getOrCreateCache(destroyCacheCfg);

        final IgniteCache<Integer, byte[]> cache2 = ig.getOrCreateCache(surviveCacheCfg);

        IgniteInternalFuture f0 = GridTestUtils.runAsync(() -> {
            try {
                putLatch.await();

                cache.destroy();

                destroyLatch.countDown();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });

        IgniteInternalFuture f1 = GridTestUtils.runAsync(() -> {
            byte[] val = new byte[1024];

            try (Transaction tx = ig.transactions().txStart(conc, iso, 1_000, 2)) {
                cache.put(100, val);

                cache2.put(100, val);

                putLatch.countDown();

                destroyLatch.await();

                tx.commit();
            }
            catch (IgniteException e) {
                assertTrue(X.hasCause(e, CacheInvalidStateException.class) ||
                    X.hasCause(e, CacheStoppedException.class) || X.hasCause(e, TransactionRollbackException.class) ||
                    X.hasCause(e, IgniteException.class));
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }

        }, "tx-load-thread");

        f1.get();
        f0.get();

        assertNull(cache2.get(100));
    }
}
