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

package org.apache.ignite.internal.client.thin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.IndexQuery;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.metric.GridMetricManager.CLIENT_CONNECTOR_METRICS;

/**
 * The test is checking the public java api, which works with partition awareness:
 * <ul>
 *     <li>Single key operation.
 *     <li>ScanQuery/IndexQuery with specified part.
 * <ul/>
 */
@RunWith(Parameterized.class)
public class ThinClientAwarenessMetricsTest extends AbstractThinClientTest {
    /** Awareness. */
    @Parameterized.Parameter(0)
    public static boolean isAwareness;

    /** Transactional. */
    @Parameterized.Parameter(1)
    public static boolean isTransactioanl;

    /** WALMode values. */
    @Parameterized.Parameters(name = "awareness={0}, transactional={1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] {true, false},
            new Object[] {true, true},
            new Object[] {false, false},
            new Object[] {false, true}
        );
    }

    /** Number of server nodes. */
    private static final int SERVERS_CNT = 3;

    /** Number of entries. */
    private static final int ENTRIES_CNT = 1000;

    /** Thin client instance with awareness. */
    private static IgniteClient igniteClientWithAwareness;

    /** Thin client instance without awareness. */
    private static IgniteClient igniteClientWithoutAwareness;

    /** Server ignite intance. */
    private static Ignite server;

    /** Server ignite cache intance. */
    private static IgniteCache<Integer, Integer> serverCache;

    /** Thin client ignite cache intance with awareness. */
    private static ClientCache<Object, Object> clientCacheWithAwareness;

    /** Thin client ignite cache intance without awareness. */
    private static ClientCache<Object, Object> clientCacheWithoutAwareness;

    /** Thin client ignite cache intance. */
    private static ClientCache<Object, Object> clientCache;

    /** Thin client transaction. */
    private static ClientTransaction clientTx;

    /** Thin client inctance. */
    private static IgniteClient igniteClient;

    /** Number of partitions. */
    private static int parts;

    /** Number of hit in test. */
    private static long hit;

    /** Number of miss in test. */
    private static long miss;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        server = startGrids(SERVERS_CNT);

        serverCache = server.getOrCreateCache(new CacheConfiguration<Integer, Integer>()
            .setName(DEFAULT_CACHE_NAME)
            .setStatisticsEnabled(true)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setQueryEntities(Collections.singletonList(new QueryEntity(Integer.class, Integer.class))));

        igniteClientWithAwareness = Ignition.startClient(new ClientConfiguration()
            .setPartitionAwarenessEnabled(true)
            .setAddresses("127.0.0.1"));

        igniteClientWithoutAwareness = Ignition.startClient(new ClientConfiguration()
            .setPartitionAwarenessEnabled(false)
            .setAddresses("127.0.0.1"));

        clientCacheWithAwareness = igniteClientWithAwareness.cache(DEFAULT_CACHE_NAME);

        clientCacheWithoutAwareness = igniteClientWithoutAwareness.cache(DEFAULT_CACHE_NAME);

        parts = ((IgniteEx)server).cachex(DEFAULT_CACHE_NAME).context().config().getAffinity().partitions();
    }

    /** Check {@link ClientCache#get(Object)}. */
    @Test
    public void testGet() throws Exception {
        withFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    clientCache.get(i);
                }
            }
        });
    }

    /** Check {@link ClientCache#getAsync(Object)}. */
    @Test
    public void testGetAsync() throws Exception {
        withFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    try {
                        clientCache.getAsync(i).get();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }

    /** Check {@link ClientCache#replace(Object, Object)}. */
    @Test
    public void testReplace() throws Exception {
        withFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    clientCache.replace(i, i);
                }
            }
        });
    }

    /** Check {@link ClientCache#replaceAsync(Object, Object)}. */
    @Test
    public void testReplaceAsync() throws Exception {
        withFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    try {
                        clientCache.replaceAsync(i, i).get();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }

    /** Check {@link ClientCache#replace(Object, Object, Object)}. */
    @Test
    public void testReplaceIfEquals() throws Exception {
        withFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    clientCache.replace(i, i, i);
                }
            }
        });
    }

    /** Check {@link ClientCache#replaceAsync(Object, Object, Object)}. */
    @Test
    public void testReplaceIfEqualsAsync() throws Exception {
        withFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    try {
                        clientCache.replaceAsync(i, i, i).get();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }

    /** Check {@link ClientCache#containsKey(Object)}. */
    @Test
    public void testContainsKey() throws Exception {
        withFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    clientCache.containsKey(i);
                }
            }
        });
    }

    /** Check {@link ClientCache#containsKeyAsync(Object)}. */
    @Test
    public void testContainsKeyAsync() throws Exception {
        withFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    try {
                        clientCache.containsKeyAsync(i).get();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }

    /** Check {@link ClientCache#clear(Object)}. */
    @Test
    public void testClear() throws Exception {
        withFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    clientCache.clear(i);
                }
            }
        });
    }

    /** Check {@link ClientCache#clearAsync(java.lang.Object)}. */
    @Test
    public void testClearAsync() throws Exception {
        withFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    try {
                        clientCache.clearAsync(i).get();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }

    /** Check {@link ClientCache#getAndPutIfAbsent(Object, Object)}. */
    @Test
    public void testGetAndPutIfAbsent() throws Exception {
        withFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    clientCache.getAndPutIfAbsent(i, i);
                }
            }
        });
    }

    /** Check {@link ClientCache#getAndPutIfAbsentAsync(Object, Object)}. */
    @Test
    public void testGetAndPutIfAbsentAsync() throws Exception {
        withFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    try {
                        clientCache.getAndPutIfAbsentAsync(i, i).get();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }

    /** Check {@link ClientCache#getAndPut(Object, Object)}. */
    @Test
    public void testGetAndPut() throws Exception {
        withFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    clientCache.getAndPut(i, i);
                }
            }
        });
    }

    /** Check {@link ClientCache#getAndPutAsync(Object, Object)}. */
    @Test
    public void testGetAndPutAsync() throws Exception {
        withFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    try {
                        clientCache.getAndPutAsync(i, i).get();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }

    /** Check {@link ClientCache#getAndReplace(Object, Object)}. */
    @Test
    public void testGetAndReplace() throws Exception {
        withFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    clientCache.getAndReplace(i, i);
                }
            }
        });
    }

    /** Check {@link ClientCache#getAndReplaceAsync(Object, Object)}. */
    @Test
    public void testGetAndReplaceAsync() throws Exception {
        withFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    try {
                        clientCache.getAndReplaceAsync(i, i).get();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }

    /** Check {@link ClientCache#put(Object, Object)}. */
    @Test
    public void testPut() throws Exception {
        withoutFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    clientCache.put(i, i);
                }
            }
        });
    }

    /** Check {@link ClientCache#putAsync(Object, Object)}. */
    @Test
    public void testPuteAsync() throws Exception {
        withoutFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    try {
                        clientCache.putAsync(i, i).get();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }

    /** Check {@link ClientCache#putIfAbsent(Object, Object)}. */
    @Test
    public void testPutIfAbsent() throws Exception {
        withoutFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    clientCache.putIfAbsent(i, i);
                }
            }
        });
    }

    /** Check {@link ClientCache#putIfAbsentAsync(Object, Object)}. */
    @Test
    public void testPutIfAbsentAsync() throws Exception {
        withoutFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    try {
                        clientCache.putIfAbsentAsync(i, i).get();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }

    /** Check {@link ClientCache#getAndRemove(Object)}. */
    @Test
    public void testGetAndRemove() throws Exception {
        withFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    clientCache.getAndRemove(i);
                }
            }
        });
    }

    /** Check {@link ClientCache#getAndRemoveAsync(Object)}. */
    @Test
    public void testGetAndRemoveAsync() throws Exception {
        withFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    try {
                        clientCache.getAndRemoveAsync(i).get();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }

    /** Check {@link ClientCache#remove(Object)}. */
    @Test
    public void testRemove() throws Exception {
        withFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    clientCache.remove(i);
                }
            }
        });
    }

    /** Check {@link ClientCache#removeAsync(Object)}. */
    @Test
    public void testRemoveAsync() throws Exception {
        withFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    try {
                        clientCache.removeAsync(i).get();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }

    /** Check {@link ClientCache#remove(Object, Object)}. */
    @Test
    public void testRemoveIfEquals() {
        withFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    clientCache.remove(i, i);
                }
            }
        });
    }

    /** Check {@link ClientCache#removeAsync(Object, Object)}. */
    @Test
    public void testRemoveIfEqualsAsync() throws Exception {
        withFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < ENTRIES_CNT; i++) {
                    try {
                        clientCache.removeAsync(i, i).get();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }

    /** Check ScanQuery. */
    @Test
    public void testScanQuery() {
        withFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < parts; i++) {
                    clientCache.query(new ScanQuery<>().setPartition(i)).getAll();
                }
            }
        }, true);
    }

    /** Check ScanQuery. */
    @Test

    public void testIndexQuery() {
        withFillingTest(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < parts; i++) {
                    clientCache.query(new IndexQuery<Integer, Integer>(Integer.class).setPartition(i)).getAll();
                }
            }
        }, true);
    }

    /** Check metrics for tested cache. */
    private static void collectMetrics() {
        hit = 0;
        miss = 0;

        for (IgniteEx ignite : F.transform(G.allGrids(), ignite -> (IgniteEx)ignite)) {
            MetricRegistry mreg = ignite.context().metric().registry(CLIENT_CONNECTOR_METRICS);

            LongAdderMetric metricHit = mreg.findMetric(ClientListenerProcessor.AFFINITY_KEY_HIT);
            LongAdderMetric metricMiss = mreg.findMetric(ClientListenerProcessor.AFFINITY_KEY_MISS);

            hit += metricHit.value();
            miss += metricMiss.value();

            metricHit.reset();
            metricMiss.reset();
        }
    }

    /** Check metrics for all metrics, exclude query. */
    private static void checkMetrics() {
        if (isAwareness && !isTransactioanl) {
            assertWhenPAWorked();
        }
        else {
            assertWhenPANotWorked();
        }
    }

    /** Check metrics for Query metrics. */
    private static void checkIndexMetrics() {
        if (isAwareness) {
            assertWhenPAWorked();
        }
        else {
            assertWhenPANotWorked();
        }
    }

    /** Assert then partition awaraness is work. */
    private static void assertWhenPAWorked() {
        assertTrue(miss + " " + hit, miss == 0);
        assertTrue(miss + " " + hit, hit == ENTRIES_CNT);
    }

    /** Assert then partition awaraness is not work. */
    private static void assertWhenPANotWorked() {
        assertTrue(miss + " " + hit, miss > 0);
        assertTrue(miss + " " + hit, hit < ENTRIES_CNT);
        assertTrue(miss + " " + hit, hit + miss == ENTRIES_CNT);
    }

    /** Base test logic with filling data. */
    public static void withFillingTest(Runnable runnable, boolean... isQueryTest ) {
        emptyTest(true, runnable, isQueryTest);
    }

    /** Base test logic without filling data. */
    public static void withoutFillingTest(Runnable runnable, boolean... isQueryTest) {
        emptyTest(false, runnable, isQueryTest);
    }

    /** Base test logic. */
    public static void emptyTest(boolean toFill, Runnable runnable, boolean... isQueryTest) {
        boolean isQuery = false;

        if (isQueryTest.length != 0)
            isQuery = true;

        if (isAwareness) {
            igniteClient = igniteClientWithAwareness;

            clientCache = clientCacheWithAwareness;
        }
        else {
            igniteClient = igniteClientWithoutAwareness;

            clientCache = clientCacheWithoutAwareness;
        }

        if (toFill && serverCache.size(CachePeekMode.ALL) == 0) {
            for (int i = 0; i < ENTRIES_CNT; i++) {
                serverCache.put(i, i);
            }
        }

        if (!toFill && serverCache.size(CachePeekMode.ALL) != 0) {
            serverCache.clear();
        }

        if (isTransactioanl) {
            try (ClientTransaction clientTransaction = igniteClient.transactions().txStart()) {
                runnable.run();
            }
        }
        else {
            runnable.run();
        }

        collectMetrics();

        if (isQuery)
            checkIndexMetrics();
        else
            checkMetrics();
    }
}
