/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
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
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.junit.Test;

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

        cfg.setCacheConfiguration(ccfg1, ccfg2);

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
        testTxOnCacheStop(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxOnCacheStopWithMessageBlock() throws Exception {
        testTxOnCacheStop(true);
    }

    /**
     * @param block {@code True} To block GridNearTxPrepareRequest message.
     */
    public void testTxOnCacheStop(boolean block) throws Exception {
        startGridsMultiThreaded(2);

        Ignition.setClientMode(true);

        IgniteEx ig = startGrid("client");

        ig.cluster().active(true);

        for (TransactionConcurrency conc : TransactionConcurrency.values())
            for (TransactionIsolation iso : TransactionIsolation.values())
                runTxOnCacheStop(conc, iso, ig, block);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxOnCacheStopInMid() throws Exception {
        startGridsMultiThreaded(2);

        Ignition.setClientMode(true);

        IgniteEx ig = startGrid("client");

        ig.cluster().active(true);

        for (TransactionConcurrency conc : TransactionConcurrency.values())
            for (TransactionIsolation iso : TransactionIsolation.values())
                runCacheStopInMidTx(conc, iso, ig);
    }

    /**
     * @throws Exception If failed.
     */
    private void runTxOnCacheStop(TransactionConcurrency conc, TransactionIsolation iso, Ignite ig, boolean runConc)
        throws Exception {
        if ((conc == TransactionConcurrency.OPTIMISTIC) && (MvccFeatureChecker.forcedMvcc()))
            return;

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
    private void runCacheStopInMidTx(TransactionConcurrency conc, TransactionIsolation iso, Ignite ig) throws Exception {
        if ((conc == TransactionConcurrency.OPTIMISTIC) && (MvccFeatureChecker.forcedMvcc()))
            return;

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

        });

        f1.get();
        f0.get();

        assertNull(cache2.get(100));
    }
}
