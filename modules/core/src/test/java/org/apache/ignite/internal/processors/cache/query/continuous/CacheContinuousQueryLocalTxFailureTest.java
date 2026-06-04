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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.cache.Cache;
import javax.cache.event.EventType;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.TestFailureHandler;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

/**
 * Tests local continuous query interaction with transaction rollback after node failure.
 */
public class CacheContinuousQueryLocalTxFailureTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES = 3;

    /** */
    private final TestFailureHandler[] failureHnds = new TestFailureHandler[NODES];

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        int idx = getTestIgniteInstanceIndex(igniteInstanceName);

        TestFailureHandler failureHnd = new TestFailureHandler(false);

        failureHnds[idx] = failureHnd;

        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)))
            .setFailureHandler(failureHnd)
            .setCacheConfiguration(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                    .setCacheMode(CacheMode.PARTITIONED)
                    .setBackups(2)
                    .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                    .setReadFromBackup(true)
                    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalContinuousQueryOnTxRollbackAfterNodeFailed() throws Exception {
        startGrids(NODES).cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache1 = grid(1).cache(DEFAULT_CACHE_NAME);

        AtomicBoolean cancel = new AtomicBoolean();

        IgniteInternalFuture<?> txFut = GridTestUtils.runMultiThreadedAsync(() -> {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            while (!cancel.get()) {
                try (Transaction tx = grid(1).transactions().txStart(
                    TransactionConcurrency.PESSIMISTIC,
                    TransactionIsolation.REPEATABLE_READ
                )) {
                    Map<Integer, Object> vals = rnd.ints()
                        .distinct()
                        .limit(10)
                        .boxed()
                        .collect(Collectors.toMap(Function.identity(), Function.identity()));

                    cache1.putAll(vals);

                    tx.commit();
                }
                catch (Exception ignore) {
                    // No-op.
                }
            }
        }, 10, "test-tx");

        try {
            doSleep(5_000);

            IgniteCache<Object, Object> cache0 = grid(0).cache(DEFAULT_CACHE_NAME);

            try (QueryCursor<Cache.Entry<Object, Object>> cursor = cache0.query(
                new ContinuousQuery<>()
                    .setInitialQuery(new ScanQuery<>())
                    .setLocal(true)
                    .setLocalListener(evts -> {
                        for (Object ignored : evts) {
                            // No-op.
                        }
                    })
                    .setRemoteFilterFactory(() -> e ->
                        e.getValue() != null && !EventType.REMOVED.equals(e.getEventType()))
            )) {
                Iterator<Cache.Entry<Object, Object>> iter = cursor.iterator();

                while (iter.hasNext())
                    iter.next();

                doSleep(5_000);

                failNode(2);

                assertNotNull(failureHnds[2].awaitFailure(10_000));

                doSleep(5_000);
            }

            assertNoFailure(0);
            assertNoFailure(1);
        }
        finally {
            cancel.set(true);

            txFut.get();
        }
    }

    /**
     * @param idx Node index.
     */
    private void assertNoFailure(int idx) {
        FailureContext failureCtx = failureHnds[idx].failureContext();

        assertNull("Unexpected failure on node " + idx + ": " + failureCtx, failureCtx);
    }

    /**
     * @param idx Node index.
     */
    private void failNode(int idx) {
        ((TcpDiscoverySpi)grid(idx).configuration().getDiscoverySpi()).simulateNodeFailure();
    }
}
