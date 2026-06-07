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

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.cache.Cache;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.GridKernalState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests interaction between local continuous queries created with {@link ContinuousQuery#setLocal(boolean)} set to
 * {@code true} and transaction rollback counter cleanup.
 * <p>
 * When a transactional update is rolled back, {@code IgniteTxHandler} closes the corresponding partition update
 * counter gaps and calls {@code CacheContinuousQueryManager#skipUpdateCounter} for the rolled back counters. These
 * skipped counters are needed by distributed continuous queries to release pending events after gaps caused by
 * rolled back updates.
 * <p>
 * Local continuous queries are different: their regular update events are delivered directly to the local listener and
 * do not use the distributed partition recovery path based on skipped update counters. Therefore, local-only continuous
 * query listeners must not be passed into {@code skipUpdateCounter}. Otherwise, the local-only handler may reach the
 * unsupported {@code loc == true && locOnly == true} path in
 * {@code CacheContinuousQueryHandler.CacheContinuousQueryListener#skipUpdateCounter}.
 * <p>
 * This class verifies that local continuous queries are ignored by skipped-counter processing, while ordinary
 * distributed continuous queries continue to work correctly.
 */
@RunWith(Parameterized.class)
public class CacheContinuousQueryLocalTxRollbackTest extends GridCommonAbstractTest {
    /** */
    private static final int NODE_CNT = 3;

    /** */
    private static final int TX_THREADS = 10;

    /** */
    private static final int KEYS_PER_TX = 10;

    /** */
    private IgniteEx ignite;

    /** */
    @Parameterized.Parameter
    public CacheMode cacheMode;

    /** */
    @Parameterized.Parameters(name = "cacheMode={0}")
    public static Object[] params() {
        return new Object[] {REPLICATED, PARTITIONED};
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setCacheMode(cacheMode)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setReadFromBackup(true)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        if (cacheMode == PARTITIONED)
            cacheCfg.setBackups(NODE_CNT - 1);

        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)))
            .setFailureHandler(new StopNodeFailureHandler())
            .setCacheConfiguration(cacheCfg);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * Checks that local and distributed continuous queries behave correctly when transaction rollback closes
     * partition update counter gaps after a node failure.
     */
    @Test
    public void checkLocalAndDistributedCqAfterNodeFail() throws Exception {
        ignite = startGrids(NODE_CNT);

        ignite.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        AtomicBoolean stopTxLoad = new AtomicBoolean();
        AtomicBoolean nodeFailed = new AtomicBoolean();

        AtomicInteger updatesDistr = new AtomicInteger();
        AtomicInteger updatesALoc = new AtomicInteger();

        IgniteCache<Object, Object> cache1 = grid(1).cache(DEFAULT_CACHE_NAME);

        IgniteInternalFuture<?> txLoadFut = launchTxLoad(grid(1), cache1, stopTxLoad);

        doSleep(5_000);

        IgniteCache<Object, Object> cache0 = grid(0).cache(DEFAULT_CACHE_NAME);

        try (
            QueryCursor<Cache.Entry<Object, Object>> locCur = cache0.query(
                buildContinuousQuery(true, nodeFailed, updatesALoc));

            QueryCursor<Cache.Entry<Object, Object>> distrCur = cache0.query(
                buildContinuousQuery(false, nodeFailed, updatesDistr))
        ) {
            doSleep(3_000);

            failNode(NODE_CNT - 1);

            assertFalse("Cluster stopped/stopping after target node failure", waitForCondition(() ->
                ignite.context().gateway().getState() == GridKernalState.STOPPED ||
                    ignite.context().gateway().getState() == GridKernalState.STOPPING,
                10_000));

            assertEquals("Not enogh nodes sirvuved after target node failure",
                NODE_CNT - 1, ignite.cluster().nodes().size());

            nodeFailed.set(true);

            assertTrue(
                String.format("Failed to receive expected updates [localUpdates=%s, distributedUpdates=%s]",
                    updatesALoc.get(), updatesDistr.get()),
                waitForCondition(() -> updatesALoc.get() > 0 && updatesDistr.get() > 0, 3_000)
            );
        }
        finally {
            stopTxLoad.set(true);

            txLoadFut.get();
        }
    }

    /** */
    private IgniteInternalFuture<?> launchTxLoad(
        IgniteEx grid,
        IgniteCache<Object, Object> cache,
        AtomicBoolean stopTxLoad
    ) {
        return GridTestUtils.runMultiThreadedAsync(() -> {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            while (!stopTxLoad.get()) {
                try (
                    Transaction tx = grid.transactions().txStart(
                        TransactionConcurrency.PESSIMISTIC,
                        TransactionIsolation.REPEATABLE_READ
                    )) {
                    Map<Integer, Object> vals = rnd.ints()
                        .limit(KEYS_PER_TX)
                        .boxed()
                        .collect(Collectors.toMap(Function.identity(), Function.identity(), (a, b) -> a));

                    cache.putAll(vals);

                    tx.commit();
                }
                catch (Exception ignore) {
                    // No-op.
                }
            }
        }, TX_THREADS, "test-tx");
    }

    /** */
    private ContinuousQuery<Object, Object> buildContinuousQuery(
        boolean locOnly,
        AtomicBoolean nodeFailed,
        AtomicInteger updateCntr
    ) {
        return new ContinuousQuery<>()
            .setLocal(locOnly)
            .setLocalListener(new CacheEntryUpdatedListener<>() {
                @Override public void onUpdated(Iterable iterable) throws CacheEntryListenerException {
                    for (Object ignored : iterable) {
                        if (nodeFailed.get())
                            updateCntr.incrementAndGet();
                    }

                    doSleep(1);
                }
            });
    }

    /** */
    private void failNode(int lastNodeIdx) {
        ((TcpDiscoverySpi)grid(lastNodeIdx).configuration().getDiscoverySpi()).simulateNodeFailure();
    }
}
