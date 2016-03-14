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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessageV2;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionTimeoutException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
@SuppressWarnings("unchecked")
public class CacheTxLockTimeoutTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int SRVS = 4;

    /** */
    private static final String CACHE1 = "cache1";

    /** */
    private static final String CACHE2 = "cache2";

    /** */
    private static final String[] CACHES = {CACHE1, CACHE2};

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        if (getTestGridName(SRVS).equals(gridName))
            cfg.setClientMode(true);

        TestRecordingCommunicationSpi spi = new TestRecordingCommunicationSpi();

        spi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(spi);

        CacheConfiguration ccfg1 = new CacheConfiguration();

        ccfg1.setAtomicityMode(TRANSACTIONAL);
        ccfg1.setBackups(1);
        ccfg1.setWriteSynchronizationMode(FULL_SYNC);
        ccfg1.setName(CACHE1);

        CacheConfiguration ccfg2 = new CacheConfiguration();

        ccfg2.setAtomicityMode(TRANSACTIONAL);
        ccfg2.setCacheMode(REPLICATED);
        ccfg2.setWriteSynchronizationMode(FULL_SYNC);
        ccfg2.setName(CACHE2);

        cfg.setCacheConfiguration(ccfg1, ccfg2);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(SRVS);

        startGrid(SRVS);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDelayNearLockRequest() throws Exception {
        for (String cacheName : CACHES)
            delayNearLockRequest(cacheName);
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void delayNearLockRequest(String cacheName) throws Exception {
        Ignite ignite0 = ignite(0);

        final Integer key = primaryKey(ignite0.cache(cacheName));

        final Ignite client = ignite(SRVS);

        final IgniteCache cache = client.cache(cacheName);

        final IgniteTransactions txs = client.transactions();

        TestRecordingCommunicationSpi commSpi =
            (TestRecordingCommunicationSpi)client.configuration().getCommunicationSpi();

        commSpi.blockMessages(GridNearLockRequest.class, ignite0.name());

        try {
            log.info("Start tx: " + key);

            try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ, 500, 0)) {
                cache.put(key, 1);

                tx.commit();
            }

            fail();
        }
        catch (CacheException e) {
            assertTrue("Unexpected cause: " + e.getCause(), e.getCause() instanceof TransactionTimeoutException);

            log.info("Expected error: " + e);
        }

        commSpi.stopBlock();

        U.sleep(100);

        tryLockKey(cache, key);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDelayNearLockResponse() throws Exception {
        for (String cacheName : CACHES)
            delayNearLockResponse(cacheName);
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void delayNearLockResponse(String cacheName) throws Exception {
        Ignite ignite0 = ignite(0);

        final Integer key = primaryKey(ignite0.cache(cacheName));

        final Ignite client = ignite(SRVS);

        final IgniteCache cache = client.cache(cacheName);

        final IgniteTransactions txs = client.transactions();

        TestRecordingCommunicationSpi commSpi =
            (TestRecordingCommunicationSpi)ignite0.configuration().getCommunicationSpi();

        commSpi.blockMessages(GridNearLockResponse.class, client.name());

        try {
            log.info("Start tx: " + key);

            try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ, 500, 0)) {
                cache.put(key, 1);

                tx.commit();
            }

            fail();
        }
        catch (CacheException e) {
            assertTrue("Unexpected cause: " + e.getCause(), e.getCause() instanceof TransactionTimeoutException);

            log.info("Expected error: " + e);
        }

        commSpi.stopBlock();

        U.sleep(100);

        tryLockKey(cache, key);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLockTimeout() throws Exception {
        for (String cacheName : CACHES)
            lockTimeout(cacheName);
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void lockTimeout(String cacheName) throws Exception {
        Ignite ignite0 = ignite(0);

        final Integer key = primaryKey(ignite0.cache(cacheName));

        final Ignite client = ignite(SRVS);

        final IgniteCache cache = client.cache(cacheName);

        final IgniteTransactions txs = client.transactions();

        CountDownLatch releaseLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = lockKey(releaseLatch, cache, key);

        try {
            log.info("Start tx: " + key);

            try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ, 500, 0)) {
                cache.put(key, 1);

                tx.commit();
            }

            fail();
        }
        catch (CacheException e) {
            assertTrue("Unexpected cause: " + e.getCause(), e.getCause() instanceof TransactionTimeoutException);

            log.info("Expected error: " + e);
        }

        releaseLatch.countDown();

        fut.get();

        tryLockKey(cache, key);

        try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.put(key, 1);

            tx.commit();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLockTimeoutMultipleKeys() throws Exception {
        for (String cacheName : CACHES)
            lockTimeoutMultipleKeys(cacheName);
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void lockTimeoutMultipleKeys(String cacheName) throws Exception {
        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < SRVS; i++)
            keys.add(primaryKey(ignite(i).cache(cacheName)));

        final Ignite client = ignite(SRVS);

        final IgniteCache cache = client.cache(cacheName);

        final IgniteTransactions txs = client.transactions();

        CountDownLatch releaseLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = lockKey(releaseLatch, cache, keys.get(keys.size() - 1));

        try {
            log.info("Start tx: " + keys);

            try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ, 500, 0)) {
                for (Integer key : keys)
                    cache.put(key, 1);

                tx.commit();
            }

            fail();
        }
        catch (CacheException e) {
            assertTrue("Unexpected cause: " + e.getCause(), e.getCause() instanceof TransactionTimeoutException);

            log.info("Expected error: " + e);
        }

        releaseLatch.countDown();

        fut.get();

        tryLockKeys(cache, keys);

        try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ, 500, 0)) {
            for (Integer key : keys)
                cache.put(key, 1);

            tx.commit();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDhtLockResponseDelay() throws Exception {
        for (String cacheName : CACHES)
            dhtLockResponseDelay(cacheName);
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void dhtLockResponseDelay(final String cacheName) throws Exception {
        for (int i = 0; i < SRVS; i++) {
            TestRecordingCommunicationSpi commSpi =
                (TestRecordingCommunicationSpi)ignite(i).configuration().getCommunicationSpi();

            commSpi.blockMessages(new IgnitePredicate<GridIoMessage>() {
                @Override public boolean apply(GridIoMessage ioMsg) {
                    Message msg0 = ioMsg.message();

                    if (msg0.getClass().equals(GridDhtPartitionSupplyMessageV2.class)) {
                        GridDhtPartitionSupplyMessageV2 msg = (GridDhtPartitionSupplyMessageV2)ioMsg.message();

                        return msg.cacheId() == CU.cacheId(cacheName);
                    }
                    else if (msg0.getClass().equals(GridDhtLockResponse.class))
                        return true;

                    return false;
                }
            });
        }

        Ignite newSrv = startGrid(SRVS + 1);

        try {
            final Integer key = primaryKey(newSrv.cache(cacheName));

            final Ignite client = ignite(SRVS);

            final IgniteCache cache = client.cache(cacheName);

            final IgniteTransactions txs = client.transactions();

            try {
                log.info("Start tx: " + key);

                try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ, 500, 0)) {
                    cache.getAndPut(key, 1);

                    tx.commit();
                }

                fail();
            }
            catch (CacheException e) {
                assertTrue("Unexpected cause: " + e.getCause(), e.getCause() instanceof TransactionTimeoutException);

                log.info("Expected error: " + e);
            }

            for (int i = 0; i < SRVS; i++) {
                TestRecordingCommunicationSpi commSpi =
                    (TestRecordingCommunicationSpi) ignite(i).configuration().getCommunicationSpi();

                commSpi.stopBlock();
            }

            U.sleep(100);

            tryLockKey(cache, key);

            try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.getAndPut(key, 1);

                tx.commit();
            }
        }
        finally {
            stopGrid(SRVS + 1);
        }

        awaitPartitionMapExchange();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDhtLockRequestDelay1() throws Exception {
        for (String cacheName : CACHES)
            dhtLockRequestDelay(cacheName, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDhtLockRequestDelay2() throws Exception {
        for (String cacheName : CACHES)
            dhtLockRequestDelay(cacheName, true);
    }

    /**
     * @param cacheName Cache name.
     * @param delaySingle If {@code true} delays message ony for one of backups.
     * @throws Exception If failed.
     */
    private void dhtLockRequestDelay(final String cacheName, final boolean delaySingle) throws Exception {
        for (int i = 0; i < SRVS; i++) {
            TestRecordingCommunicationSpi commSpi =
                (TestRecordingCommunicationSpi)ignite(i).configuration().getCommunicationSpi();

            commSpi.blockMessages(new IgnitePredicate<GridIoMessage>() {
                @Override public boolean apply(GridIoMessage ioMsg) {
                    Message msg0 = ioMsg.message();

                    if (msg0.getClass().equals(GridDhtPartitionSupplyMessageV2.class)) {
                        GridDhtPartitionSupplyMessageV2 msg = (GridDhtPartitionSupplyMessageV2)ioMsg.message();

                        return msg.cacheId() == CU.cacheId(cacheName);
                    }

                    return false;
                }
            });
        }

        Ignite newSrv = startGrid(SRVS + 1);

        try {
            final Integer key = primaryKey(newSrv.cache(cacheName));

            final Ignite client = ignite(SRVS);

            final IgniteCache cache = client.cache(cacheName);

            final IgniteTransactions txs = client.transactions();

            TestRecordingCommunicationSpi commSpi =
                (TestRecordingCommunicationSpi)newSrv.configuration().getCommunicationSpi();

            commSpi.blockMessages(new IgnitePredicate<GridIoMessage>() {
                final AtomicBoolean blocked = new AtomicBoolean();

                @Override public boolean apply(GridIoMessage ioMsg) {
                    Message msg0 = ioMsg.message();

                    return msg0.getClass().equals(GridDhtLockRequest.class) &&
                        (!delaySingle || blocked.compareAndSet(false, true));
                }
            });

            try {
                log.info("Start tx: " + key);

                try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ, 500, 0)) {
                    cache.getAndPut(key, 1);

                    tx.commit();
                }

                fail();
            }
            catch (CacheException e) {
                assertTrue("Unexpected cause: " + e.getCause(), e.getCause() instanceof TransactionTimeoutException);

                log.info("Expected error: " + e);
            }

            commSpi.stopBlock();

            U.sleep(100);

            for (int i = 0; i < SRVS; i++)
                ((TestRecordingCommunicationSpi)ignite(i).configuration().getCommunicationSpi()).stopBlock();

            tryLockKey(cache, key);

            try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.getAndPut(key, 1);

                tx.commit();
            }
        }
        finally {
            stopGrid(SRVS + 1);
        }

        awaitPartitionMapExchange();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxTimeoutMultithreaded() throws Exception {
        txTimeoutMultithreaded(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxTimeoutMultithreadedNodeRestart() throws Exception {
        txTimeoutMultithreaded(true);
    }

    /**
     * @param restartNode Restart node flag.
     * @throws Exception If failed.
     */
    public void txTimeoutMultithreaded(boolean restartNode) throws Exception {
        final int KEYS = 5;

        final Ignite client = ignite(SRVS);

        final IgniteCache cache = client.cache(CACHE1);

        final IgniteTransactions txs = client.transactions();

        long stopTime = System.currentTimeMillis() + 30_000;

        int iter = 0;

        AtomicBoolean stop = new AtomicBoolean(false);

        IgniteInternalFuture<?> restartFut = restartNode ? restartFuture(stop, SRVS + 1) : null;

        try {
            while (System.currentTimeMillis() < stopTime) {
                log.info("Iteration: " + iter++);

                GridTestUtils.runMultiThreaded(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        for (int i = 0; i < 100; i++) {
                            try {
                                int key = rnd.nextInt(KEYS);

                                try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ, 10, 1)) {
                                    cache.put(key, 1);

                                    U.sleep(rnd.nextInt(1, 5));

                                    tx.commit();
                                }
                            }
                            catch (Exception e) {
                                log.info("Tx failed: " + e);

                                break;
                            }
                        }

                        return null;
                    }
                }, 10, "tx-thread");
            }
        }
        finally {
            stop.set(true);

            if (restartFut != null)
                restartFut.get();
        }
    }
    /**
     * @param stop Stop flag.
     * @param nodeIdx Node index..
     * @return Restart thread future.
     */
    private IgniteInternalFuture<?> restartFuture(final AtomicBoolean stop, final int nodeIdx) {
        return GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                while (!stop.get()) {
                    Ignite ignite = startGrid(nodeIdx);

                    assertFalse(ignite.configuration().isClientMode());

                    U.sleep(300);

                    stopGrid(nodeIdx);
                }

                return null;
            }
        }, "restart-thread");
    }

    /**
     * @param releaseLatch Release lock latch.
     * @param cache Cache.
     * @param key Key.
     * @return Future.
     * @throws Exception If failed.
     */
    private IgniteInternalFuture<?> lockKey(
        final CountDownLatch releaseLatch,
        final IgniteCache<Integer, Integer> cache,
        final Integer key) throws Exception {
        final CountDownLatch lockLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

                try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.put(key, 1);

                    log.info("Locked key: " + key);

                    lockLatch.countDown();

                    assertTrue(releaseLatch.await(100000, SECONDS));

                    log.info("Unlock key: " + key);

                    tx.commit();
                }

                return null;
            }
        }, "lock-thread");

        assertTrue(lockLatch.await(10, SECONDS));

        return fut;
    }

    /**
     * @param cache Cache.
     * @param key Key.
     * @throws Exception If failed.
     */
    private void tryLockKey(final IgniteCache cache, final Integer key) throws Exception {
        tryLockKeys(cache, F.asList(key));
    }

    /**
     * @param cache Cache.
     * @param keys Keys.
     * @throws Exception If failed.
     */
    private void tryLockKeys(final IgniteCache cache, final Collection<Integer> keys) throws Exception {
        GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Ignite ignite = (Ignite)cache.unwrap(Ignite.class);

                log.info("Try lock keys: " + keys);

                try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    for (Integer key : keys)
                        cache.put(key, 1);

                    tx.commit();
                }

                return null;
            }
        }, "tx-thread").get();
    }
}
