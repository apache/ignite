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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.io.NotSerializableException;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class CacheEntryProcessorNonSerializableTest extends GridCommonAbstractTest {
    /** */
    private static final int EXPECTED_VALUE = 42;

    /** */
    private static final int WRONG_VALUE = -1;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 3;

    /** */
    public static final int ITERATION_CNT = 1;

    /** */
    public static final int KEYS = 10;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);
        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        cfg.setClientMode(client);
        //set custom marshaller to get expected exception
        cfg.setMarshaller(new JdkMarshaller());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(getServerNodeCount());

        client = true;

        startGrid(getServerNodeCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @return Server nodes.
     */
    private int getServerNodeCount() {
        return NODES;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticOnePhaseCommit() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(PRIMARY_SYNC, 1);

        doTestInvokeTest(ccfg, PESSIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);

        doTestInvokeTest(ccfg, PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticOnePhaseCommitWithNearCache() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(PRIMARY_SYNC, 1)
            .setNearConfiguration(new NearCacheConfiguration());

        doTestInvokeTest(ccfg, PESSIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);

        doTestInvokeTest(ccfg, PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticOnePhaseCommitFullSync() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(FULL_SYNC, 1);

        doTestInvokeTest(ccfg, PESSIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);

        doTestInvokeTest(ccfg, PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticOnePhaseCommitFullSyncWithNearCache() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(FULL_SYNC, 1)
            .setNearConfiguration(new NearCacheConfiguration());

        doTestInvokeTest(ccfg, PESSIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);

        doTestInvokeTest(ccfg, PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimistic() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(PRIMARY_SYNC, 2);

        doTestInvokeTest(ccfg, PESSIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);

        doTestInvokeTest(ccfg, PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticWithNearCache() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(PRIMARY_SYNC, 2)
            .setNearConfiguration(new NearCacheConfiguration());

        doTestInvokeTest(ccfg, PESSIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);

        doTestInvokeTest(ccfg, PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticFullSync() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(FULL_SYNC, 2);

        doTestInvokeTest(ccfg, PESSIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, PESSIMISTIC, REPEATABLE_READ);

        doTestInvokeTest(ccfg, PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticOnePhaseCommit() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(PRIMARY_SYNC, 1);

        doTestInvokeTest(ccfg, OPTIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, OPTIMISTIC, REPEATABLE_READ);

        doTestInvokeTest(ccfg, OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticOnePhaseCommitFullSync() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(FULL_SYNC, 1);

        doTestInvokeTest(ccfg, OPTIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, OPTIMISTIC, REPEATABLE_READ);

        doTestInvokeTest(ccfg, OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticOnePhaseCommitFullSyncWithNearCache() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(FULL_SYNC, 1)
            .setNearConfiguration(new NearCacheConfiguration());

        doTestInvokeTest(ccfg, OPTIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, OPTIMISTIC, REPEATABLE_READ);

        doTestInvokeTest(ccfg, OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimistic() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(PRIMARY_SYNC, 2);

        doTestInvokeTest(ccfg, OPTIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, OPTIMISTIC, REPEATABLE_READ);

        doTestInvokeTest(ccfg, OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticFullSync() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(FULL_SYNC, 2);

        doTestInvokeTest(ccfg, OPTIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, OPTIMISTIC, REPEATABLE_READ);

        doTestInvokeTest(ccfg, OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticFullSyncWithNearCache() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(FULL_SYNC, 2);

        doTestInvokeTest(ccfg, OPTIMISTIC, READ_COMMITTED);

        doTestInvokeTest(ccfg, OPTIMISTIC, REPEATABLE_READ);

        doTestInvokeTest(ccfg, OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void doTestInvokeTest(CacheConfiguration ccfg, TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation) throws Exception {
        IgniteEx cln = grid(getServerNodeCount());

        grid(0).createCache(ccfg);

        IgniteCache clnCache;

        if (ccfg.getNearConfiguration() != null)
            clnCache = cln.createNearCache(ccfg.getName(), ccfg.getNearConfiguration());
        else
            clnCache = cln.cache(ccfg.getName());

        putKeys(clnCache, EXPECTED_VALUE);

        try {
            // Explicit tx.
            for (int i = 0; i < ITERATION_CNT; i++) {
                try (final Transaction tx = cln.transactions().txStart(txConcurrency, txIsolation)) {
                    putKeys(clnCache, WRONG_VALUE);

                    clnCache.invoke(KEYS, new NonSerialazibleEntryProcessor());

                    GridTestUtils.assertThrowsWithCause(new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            tx.commit();

                            return null;
                        }
                    }, NotSerializableException.class);
                }

                checkKeys(clnCache, EXPECTED_VALUE);
            }

            // From affinity node.
            Ignite grid = grid(ThreadLocalRandom.current().nextInt(NODES));

            final IgniteCache cache = grid.cache(ccfg.getName());

            // Explicit tx.
            for (int i = 0; i < ITERATION_CNT; i++) {
                try (final Transaction tx = grid.transactions().txStart(txConcurrency, txIsolation)) {
                    putKeys(cache, WRONG_VALUE);

                    cache.invoke(KEYS, new NonSerialazibleEntryProcessor());

                    GridTestUtils.assertThrowsWithCause(new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            tx.commit();

                            return null;
                        }
                    }, NotSerializableException.class);
                }

                checkKeys(cache, EXPECTED_VALUE);
            }

            final IgniteCache clnCache0 = clnCache;

            // Implicit tx.
            for (int i = 0; i < ITERATION_CNT; i++) {
                GridTestUtils.assertThrowsWithCause(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        clnCache0.invoke(KEYS, new NonSerialazibleEntryProcessor());

                        return null;
                    }
                }, NotSerializableException.class);
            }

            checkKeys(clnCache, EXPECTED_VALUE);
        }
        finally {
            grid(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param cache Cache.
     * @param val Value.
     */
    private void putKeys(IgniteCache cache, int val) {
        cache.put(KEYS, val);
    }

    /**
     * @param cache Cache.
     * @param expVal Expected value.
     */
    private void checkKeys(IgniteCache cache, int expVal) {
        assertEquals(expVal, cache.get(KEYS));
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(CacheWriteSynchronizationMode wrMode, int backup) {
        return new CacheConfiguration("test-cache-" + wrMode + "-" + backup)
            .setAtomicityMode(TRANSACTIONAL)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setBackups(backup);
    }

    /**
     *
     */
    private static class NonSerialazibleEntryProcessor implements EntryProcessor<Integer, Integer, Integer> {
        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> entry, Object... arguments)
            throws EntryProcessorException {
            entry.setValue(42);

            return null;
        }
    }
}
