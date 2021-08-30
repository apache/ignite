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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import com.google.common.collect.Maps;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.internal.util.collections.Sets;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Tests for client nodes with slow discovery.
 */
@RunWith(Parameterized.class)
public class ClientSlowDiscoveryTransactionRemapTest extends ClientSlowDiscoveryAbstractTest {
    /** */
    @Parameterized.Parameters(name = "isolation = {0}, concurrency = {1}, operation = {2}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        List<IgniteInClosure<TestTransaction<Integer, Integer>>> operations = new ArrayList<>();

        operations.add(new NamedClosure<>(putRemoveSameKey, "putRemoveSameKey"));
        operations.add(new NamedClosure<>(putRemoveDifferentKey, "putRemoveDifferentKey"));
        operations.add(new NamedClosure<>(getPutSameKey, "getPutSameKey"));
        operations.add(new NamedClosure<>(getPutDifferentKey, "getPutDifferentKey"));
        operations.add(new NamedClosure<>(putAllRemoveAllSameKeys, "putAllRemoveAllSameKeys"));
        operations.add(new NamedClosure<>(putAllRemoveAllDifferentKeys, "putAllRemoveAllDifferentKeys"));
        operations.add(new NamedClosure<>(randomOperation, "random"));

        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                if (!shouldBeTested(concurrency, isolation))
                    continue;

                for (IgniteInClosure<TestTransaction<Integer, Integer>> operation : operations)
                    params.add(new Object[] {concurrency, isolation, operation});
            }
        }

        return params;
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @return {@code True} if pair concurrency - isolation should be tested.
     */
    private static boolean shouldBeTested(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        if (concurrency == PESSIMISTIC)
            return isolation == REPEATABLE_READ || isolation == READ_COMMITTED;
        return concurrency == OPTIMISTIC && isolation == SERIALIZABLE;
    }

    /** Keys set. */
    private static final int KEYS_SET = 64;

    /** Put remove same key. */
    private static IgniteInClosure<TestTransaction<Integer, Integer>> putRemoveSameKey = tx -> {
        tx.put(1, 1);
        tx.remove(1);
        tx.put(1, 100);
    };

    /** Put remove different key. */
    private static IgniteInClosure<TestTransaction<Integer, Integer>> putRemoveDifferentKey = tx -> {
        tx.put(1, 1);
        tx.remove(2);
    };

    /** Get put same key. */
    private static IgniteInClosure<TestTransaction<Integer, Integer>> getPutSameKey = tx -> {
        int val = tx.get(1);
        tx.put(1, val + 1);
    };

    /** Get put different key. */
    private static IgniteInClosure<TestTransaction<Integer, Integer>> getPutDifferentKey = tx -> {
        int val = tx.get(1);
        tx.put(2, val + 1);
    };

    /** Put all remove all same keys. */
    private static IgniteInClosure<TestTransaction<Integer, Integer>> putAllRemoveAllSameKeys = tx -> {
        tx.putAll(Maps.asMap(Sets.newSet(1, 2, 3, 4, 5), k -> k));
        tx.removeAll(Sets.newSet(1, 2, 3, 4, 5));
    };

    /** Put all remove all different keys. */
    private static IgniteInClosure<TestTransaction<Integer, Integer>> putAllRemoveAllDifferentKeys = tx -> {
        tx.putAll(Maps.asMap(Sets.newSet(1, 2, 3, 4, 5), k -> k));
        tx.removeAll(Sets.newSet(6, 7, 8, 9, 10));
    };

    /** Random operation. */
    private static IgniteInClosure<TestTransaction<Integer, Integer>> randomOperation = tx -> {
        long seed = ThreadLocalRandom.current().nextLong();
        log.info("Seed: " + seed);
        Random random = new Random(seed);

        for (int it = 0; it < 10; it++) {
            int operation = random.nextInt(TestTransaction.POSSIBLE_OPERATIONS);

            switch (operation) {
                // Get:
                case 0: {
                    int key = random.nextInt(KEYS_SET);

                    tx.get(key);

                    break;
                }
                // Put:
                case 1: {
                    int key = random.nextInt(KEYS_SET);
                    int val = random.nextInt(KEYS_SET);

                    tx.put(key, val);

                    break;
                }
                // Remove:
                case 2: {
                    int key = random.nextInt(KEYS_SET);

                    tx.remove(key);

                    break;
                }
                // Put All:
                case 3: {
                    tx.putAll(
                        random.ints(5, 0, KEYS_SET)
                            .boxed()
                            .distinct()
                            .collect(toMap(
                                    k -> k, k -> k)
                            )
                    );

                    break;
                }
                // Remove All:
                case 4: {
                    tx.removeAll(
                        random.ints(5, 0, KEYS_SET).boxed().collect(toSet())
                    );

                    break;
                }
            }
        }
    };

    /**
     * Interface to work with cache operations within transaction.
     */
    private static interface TestTransaction<K, V> {
        /** Possible operations. */
        static int POSSIBLE_OPERATIONS = 5;

        /**
         * @param key Key.
         * @return Value.
         */
        V get(K key);

        /**
         * @param key Key.
         * @param val Value.
         */
        void put(K key, V val);

        /**
         * @param key Key.
         */
        void remove(K key);

        /**
         * @param map Map.
         */
        void putAll(Map<K, V> map);

        /**
         * @param keys Keys.
         */
        void removeAll(Set<K> keys);
    }

    /**
     * Closure with possibility to set name to have proper print in test parameters.
     */
    private static class NamedClosure<K, V> implements IgniteInClosure<TestTransaction<K, V>> {
        /** Closure. */
        private final IgniteInClosure<TestTransaction<K, V>> c;

        /** Name. */
        private final String name;

        /**
         * @param c Closure.
         * @param name Name.
         */
        public NamedClosure(IgniteInClosure<TestTransaction<K, V>> c, String name) {
            this.c = c;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public void apply(TestTransaction<K, V> kvTestTransaction) {
            c.apply(kvTestTransaction);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return name;
        }
    }

    /**
     * Implementation for transaction operations backed by Ignite cache.
     */
    private static class TestTransactionEngine<K, V> implements TestTransaction<K, V> {
        /** Removed. */
        private final Object RMV = new Object();

        /** Cache. */
        private final IgniteCache<K, V> cache;

        /** Map to consistency check. */
        private final Map<K, Object> map;

        /**
         * @param cache Cache.
         */
        TestTransactionEngine(IgniteCache<K, V> cache) {
            this.cache = cache;
            this.map = new HashMap<>();
        }

        /** {@inheritDoc} */
        @Override public V get(K key) {
            return cache.get(key);
        }

        /** {@inheritDoc} */
        @Override public void put(K key, V val) {
            map.put(key, val);
            cache.put(key, val);
        }

        /** {@inheritDoc} */
        @Override public void remove(K key) {
            map.put(key, RMV);
            cache.remove(key);
        }

        /** {@inheritDoc} */
        @Override public void putAll(Map<K, V> map) {
            for (Map.Entry<K, V> entry : map.entrySet())
                this.map.put(entry.getKey(), entry.getValue());
            cache.putAll(map);
        }

        /** {@inheritDoc} */
        @Override public void removeAll(Set<K> keys) {
            for (K key : keys)
                map.put(key, RMV);
            cache.removeAll(keys);
        }

        /**
         * Consistency check for transaction operations.
         */
        public void consistencyCheck() {
            for (Map.Entry<K, Object> entry : map.entrySet()) {
                if (entry.getValue() == RMV)
                    Assert.assertNull("Value is not null for key: " + entry.getKey(), cache.get(entry.getKey()));
                else
                    Assert.assertEquals("Values are different for key: " + entry.getKey(),
                        entry.getValue(),
                        cache.get(entry.getKey())
                    );
            }
        }
    }

    /** Concurrency. */
    @Parameterized.Parameter(0)
    public TransactionConcurrency concurrency;

    /** Isolation. */
    @Parameterized.Parameter(1)
    public TransactionIsolation isolation;

    /** Operation. */
    @Parameterized.Parameter(2)
    public IgniteInClosure<TestTransaction<?, ?>> operation;

    /** Client disco spi block. */
    private CountDownLatch clientDiscoSpiBlock;

    /** Client node to perform operations. */
    private IgniteEx clnt;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Before
    public void before() throws Exception {
        NodeJoinInterceptingDiscoverySpi clientDiscoSpi = new NodeJoinInterceptingDiscoverySpi();

        clientDiscoSpiBlock = new CountDownLatch(1);

        // Delay node join of second client.
        clientDiscoSpi.interceptor = msg -> {
            if (msg.nodeId().toString().endsWith("2"))
                U.awaitQuiet(clientDiscoSpiBlock);
        };

        discoverySpiSupplier = () -> clientDiscoSpi;

        clnt = startClientGrid(1);

        for (int k = 0; k < 64; k++)
            clnt.cache(CACHE_NAME).put(k, 0);

        discoverySpiSupplier = TcpDiscoverySpi::new;

        startClientGrid(2);
    }

    /** */
    @After
    public void after() throws Exception {
        // Stop client nodes.
        stopGrid(1);
        stopGrid(2);
    }

    /** */
    @Test
    public void testTransactionRemap() throws Exception {
        TestTransactionEngine engine = new TestTransactionEngine<>(clnt.cache(CACHE_NAME));

        IgniteInternalFuture<?> txFut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = clnt.transactions().txStart(concurrency, isolation)) {
                operation.apply(engine);

                tx.commit();
            }
        });

        try {
            txFut.get(1, TimeUnit.SECONDS);
        }
        catch (IgniteFutureTimeoutCheckedException te) {
            // Expected.
        }
        finally {
            clientDiscoSpiBlock.countDown();
        }

        // After resume second client join, transaction should succesfully await new affinity and commit.
        txFut.get();

        // Check consistency after transaction commit.
        engine.consistencyCheck();
    }

    /** */
    @Test
    public void testTransactionRemapWithTimeout() throws Exception {
        TestTransactionEngine engine = new TestTransactionEngine<>(clnt.cache(CACHE_NAME));

        IgniteInternalFuture<?> txFut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = clnt.transactions().txStart(concurrency, isolation, 1_000, 1_000_000)) {
                operation.apply(engine);

                tx.commit();
            }
        });

        try {
            txFut.get(2, TimeUnit.SECONDS);
        }
        catch (IgniteFutureTimeoutCheckedException te) {
            // Expected.
        }
        finally {
            clientDiscoSpiBlock.countDown();
        }

        // After resume second client join, transaction should be timed out and rolled back.
        if (concurrency == PESSIMISTIC) {
            assertThrowsWithCause((Callable<Object>) txFut::get, TransactionTimeoutException.class);

            // Check that initial data is not changed by rollbacked transaction.
            for (int k = 0; k < KEYS_SET; k++)
                Assert.assertEquals("Cache consistency is broken for key: " + k, 0, clnt.cache(CACHE_NAME).get(k));
        }
        else {
            txFut.get();

            engine.consistencyCheck();
        }
    }
}
