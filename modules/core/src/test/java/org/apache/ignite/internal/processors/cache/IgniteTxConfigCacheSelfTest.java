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

import javax.cache.CacheException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionTimeoutException;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 * Test checks that grid transaction configuration doesn't influence system caches.
 */
public class IgniteTxConfigCacheSelfTest extends GridCommonAbstractTest {
    /** Ip finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Test cache name. */
    private static final String CACHE_NAME = "cache_name";

    /** Timeout of transaction. */
    private static final long TX_TIMEOUT = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg =  super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        TcpCommunicationSpi commSpi = new TestCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setAtomicityMode(atomicityMode());
        ccfg.setBackups(1);

        cfg.setCacheConfiguration(ccfg);

        final TransactionConfiguration txCfg = new TransactionConfiguration();

        txCfg.setDefaultTxTimeout(TX_TIMEOUT);

        cfg.setTransactionConfiguration(txCfg);

        return cfg;
    }

    /**
     * @return Cache atomicity mode.
     */
    public CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Success if user tx was timed out.
     *
     * @throws Exception If failed.
     */
    public void testUserTxTimeout() throws Exception {
        final Ignite ignite = grid(0);

        final IgniteCache<Object, Object> cache = ignite.getOrCreateCache(CACHE_NAME);

        checkImplicitTxTimeout(cache);
        checkExplicitTxTimeout(cache, ignite);
    }

    /**
     * Success if system caches weren't timed out.
     *
     * @throws Exception If failed.
     */
    public void testSystemCacheTx() throws Exception {
        final Ignite ignite = grid(0);

        final IgniteInternalCache<Object, Object> utilCache = getSystemCache(ignite, CU.UTILITY_CACHE_NAME);

        checkImplicitTxSuccess(utilCache);
        checkStartTxSuccess(utilCache);

        final IgniteInternalCache<Object, Object> atomicsCache = getSystemCache(ignite, CU.ATOMICS_CACHE_NAME);

        checkImplicitTxSuccess(atomicsCache);
        checkStartTxSuccess(atomicsCache);
    }

    /**
     * Extract system cache from kernal.
     *
     * @param ignite Ignite instance.
     * @param cacheName System cache name.
     * @return Internal cache instance.
     */
    protected IgniteInternalCache<Object, Object> getSystemCache(final Ignite ignite, final String cacheName) {
        return ((IgniteKernal) ignite).context().cache().cache(cacheName);
    }

    /**
     * Success if implicit tx fails.
     *
     * @param cache Cache name.
     * @throws Exception If failed.
     */
    protected void checkImplicitTxTimeout(final IgniteCache<Object, Object> cache) throws Exception {
        TestCommunicationSpi.delay = true;

        Integer key = primaryKey(ignite(1).cache(CACHE_NAME));

        try {
            cache.put(key, 0);

            fail("Timeout exception must be thrown");
        }
        catch (CacheException ignored) {
            // No-op.
        }
        finally {
            TestCommunicationSpi.delay = false;
        }

        cache.clear();
    }

    /**
     * Success if explicit tx fails.
     *
     * @param cache Cache name.
     * @param ignite Ignite instance.
     * @throws Exception If failed.
     */
    protected void checkExplicitTxTimeout(final IgniteCache<Object, Object> cache, final Ignite ignite)
        throws Exception {
        try (final Transaction tx = ignite.transactions().txStart()) {
            assert tx != null;

            sleepForTxFailure();

            cache.put("key", "val");

            fail("Timeout exception must be thrown");
        }
        catch (CacheException e) {
            assert e.getCause() instanceof TransactionTimeoutException;
        }

        assert !cache.containsKey("key");
    }

    /**
     * Success if explicit tx doesn't fail.
     *
     * @param cache Cache instance.
     * @throws Exception If failed.
     */
    protected void checkStartTxSuccess(final IgniteInternalCache<Object, Object> cache) throws Exception {
        try (final IgniteInternalTx tx = CU.txStartInternal(cache.context(), cache, PESSIMISTIC, READ_COMMITTED)) {
            assert tx != null;

            sleepForTxFailure();

            cache.put("key", "val");

            tx.commit();
        }

        assert cache.containsKey("key");

        cache.clear();
    }

    /**
     * Success if implicit tx fails.
     *
     * @param cache Cache instance.
     * @throws Exception If failed.
     */
    protected void checkImplicitTxSuccess(final IgniteInternalCache<Object, Object> cache) throws Exception {
        cache.invoke("key", new EntryProcessor<Object, Object, Object>() {
            @Override public Object process(final MutableEntry<Object, Object> entry, final Object... args)
                throws EntryProcessorException {
                try {
                    sleepForTxFailure();
                } catch (InterruptedException e) {
                    throw new EntryProcessorException(e);
                }
                return null;
            }
        });

        cache.clear();
    }

    /**
     * Sleep multiple {@link #TX_TIMEOUT} times.
     *
     * @throws InterruptedException If interrupted.
     */
    private void sleepForTxFailure() throws InterruptedException {
        Thread.sleep(TX_TIMEOUT * 3);
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** Delay. */
        private static volatile boolean delay;

        /** {@inheritDoc} */
        @Override public void sendMessage(
            final ClusterNode node,
            final Message msg,
            final IgniteInClosure<IgniteException> ackC
        ) throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                Message msg0 = ((GridIoMessage)msg).message();

                if (msg0 instanceof GridNearTxPrepareRequest && delay) {
                    try {
                        U.sleep(TX_TIMEOUT * 2);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        e.printStackTrace();
                    }
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}
