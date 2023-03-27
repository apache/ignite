/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

/**
 * Test checks a work of the TTL policy.
 */
public class EagerTtlTest extends GridCommonAbstractTest {
    /** Text will print to log when assertion error happens. */
    private static final String ASSERTION_ERR = "java.lang.AssertionError: Invalid topology version [topVer=" +
        AffinityTopologyVersion.NONE +
        ", group=" + DEFAULT_CACHE_NAME + ']';

    /** Text will appear when an assertion error happens. */
    private static final String ANY_ASSERTION_ERR = "java.lang.AssertionError";

    /** Expiration time. */
    private static final int EXPIRATION_TIME = 1_000;

    /** Count of entries. */
    private static final int ENTRIES = 100;

    /** Cache eager ttl flag. */
    private boolean eagerTtl;

    /** Listening logger. */
    private ListeningTestLogger listeningLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(listeningLog)
            .setClusterStateOnStart(ClusterState.INACTIVE)
            .setCacheConfiguration(getDefaultCacheConfiguration())
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(200L * 1024 * 1024)
                    .setPersistenceEnabled(true)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        listeningLog = new ListeningTestLogger(log);
    }

    /**
     * Creates a configuration for default cache.
     *
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> getDefaultCacheConfiguration() {
        return new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 16))
            .setEagerTtl(eagerTtl)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MILLISECONDS, EXPIRATION_TIME)));
    }

    /**
     * Checks of restart node with TTL cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testOneNodeRestartWithTtlCache() throws Exception {
        eagerTtl = true;

        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < ENTRIES; i++)
            cache.put(i, i);

        ignite.close();

        LogListener assertListener = LogListener.matches(ASSERTION_ERR).build();

        listeningLog.registerListener(assertListener);

        ignite = startGrid(0);

        CountDownLatch exchangeHangLatch = new CountDownLatch(1);

        ignite.context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
            @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                try {
                    exchangeHangLatch.await();
                }
                catch (InterruptedException e) {
                    log.error("Interrupted of waiting latch", e);

                    fail(e.getMessage());
                }
            }
        });

        IgniteInternalFuture<?> activeFut = GridTestUtils.runAsync(() -> ignite(0).cluster().state(ClusterState.ACTIVE));

        assertFalse(activeFut.isDone());

        assertFalse(GridTestUtils.waitForCondition(assertListener::check, 2_000));

        exchangeHangLatch.countDown();

        activeFut.get();

        awaitPartitionMapExchange();
    }

    /**
     * Checks that an assertion error does not happen during expiration in transactions.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNotEagerExpireOnPut() throws Exception {
        eagerTtl = false;

        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        LogListener assertListener = LogListener.matches(ANY_ASSERTION_ERR).build();

        listeningLog.registerListener(assertListener);

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < ENTRIES; i++)
            cache.put(i, i);

        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            U.sleep(EXPIRATION_TIME);

            for (int i = 0; i < ENTRIES; i++) {
                try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.OPTIMISTIC, isolation)) {
                    if (i % 2 == 0)
                        cache.putAll(Collections.singletonMap(i, isolation.ordinal() + 1));
                    else
                        cache.getAndPut(i, isolation.ordinal() + 1);

                    tx.commit();
                }
            }
        }

        assertFalse(GridTestUtils.waitForCondition(assertListener::check, 2_000));
    }
}
