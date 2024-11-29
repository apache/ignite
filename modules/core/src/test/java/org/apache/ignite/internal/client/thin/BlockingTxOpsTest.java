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

import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Thin client blocking transactional operations tests.
 */
@RunWith(Parameterized.class)
public class BlockingTxOpsTest extends AbstractThinClientTest {
    /** Default tx timeout value. */
    private static final long TX_TIMEOUT = 5_000L;

    /** */
    private static final int THREADS_CNT = 5;

    /** */
    private int poolSize;

    /** */
    @Parameterized.Parameter(0)
    public TransactionConcurrency txConcurrency;

    /** */
    @Parameterized.Parameter(1)
    public TransactionIsolation txIsolation;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "concurrency={0}, isolation={1}")
    public static List<Object[]> params() {
        return F.asList(
            new Object[]{ PESSIMISTIC, REPEATABLE_READ },
            new Object[]{ OPTIMISTIC, SERIALIZABLE }
        );
    }


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getClientConnectorConfiguration().setThreadPoolSize(poolSize);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();
        stopAllGrids();
    }

    /**
     * Tests different blocking operations in transaction.
     */
    @Test
    public void testBlockingOps() throws Exception {
        for (int i : F.asList(1, THREADS_CNT - 1)) {
            poolSize = i;

            try (Ignite ignore = startGrid(0)) {
                try (IgniteClient client = startClient(0)) {
                    ClientCache<Object, Object> cache = client.getOrCreateCache(new ClientCacheConfiguration()
                        .setName("test")
                        .setAtomicityMode(TRANSACTIONAL)
                    );

                    // Contains operation.
                    checkOpMultithreaded(client,
                        () -> cache.put(0, 0),
                        () -> assertTrue(cache.containsKey(0)),
                        null
                    );

                    // Contains keys operation.
                    checkOpMultithreaded(client,
                        () -> cache.putAll(F.asMap(0, 0, 1, 1)),
                        () -> assertTrue(cache.containsKeys(new TreeSet<>(F.asList(0, 1)))),
                        null
                    );

                    // Get keys operation.
                    checkOpMultithreaded(client,
                        () -> cache.putAll(F.asMap(0, 0, 1, 1)),
                        () -> assertEquals(F.asMap(0, 0, 1, 1), cache.getAll(new TreeSet<>(F.asList(0, 1)))),
                        null
                    );

                    // Get and put if absent operation.
                    checkOpMultithreaded(client,
                        () -> cache.put(0, 0),
                        () -> assertEquals(0, cache.getAndPutIfAbsent(0, 0)),
                        null
                    );

                    // Get and put absent operation.
                    checkOpMultithreaded(client,
                        () -> cache.put(0, 0),
                        () -> assertEquals(0, cache.getAndPut(0, 0)),
                        null
                    );

                    // Get and remove operation.
                    checkOpMultithreaded(client,
                        () -> cache.put(0, 0),
                        () -> cache.getAndRemove(0),
                        () -> assertFalse(cache.containsKey(0))
                    );

                    // Get and replace operation.
                    checkOpMultithreaded(client,
                        () -> cache.put(0, 0),
                        () -> cache.getAndReplace(0, 0),
                        () -> assertTrue(cache.containsKey(0))
                    );

                    // Get operation.
                    checkOpMultithreaded(client,
                        () -> cache.put(0, 0),
                        () -> assertEquals(0, cache.get(0)),
                        null
                    );

                    // Put keys operation.
                    checkOpMultithreaded(client,
                        null,
                        () -> cache.putAll(F.asMap(0, 0, 1, 1)),
                        () -> assertEquals(F.asMap(0, 0, 1, 1), cache.getAll(new TreeSet<>(F.asList(0, 1))))
                    );

                    // Put if absent operation
                    checkOpMultithreaded(client,
                        () -> cache.put(0, 0),
                        () -> cache.putIfAbsent(0, 1),
                        () -> assertEquals(0, cache.get(0))
                    );

                    // Put operation
                    checkOpMultithreaded(client,
                        null,
                        () -> cache.put(0, 0),
                        () -> assertEquals(0, cache.get(0))
                    );

                    // Remove all operation.
                    checkOpMultithreaded(client,
                        () -> cache.putAll(F.asMap(0, 0, 1, 1)),
                        () -> cache.removeAll(),
                        () -> assertEquals(0, cache.size())
                    );

                    // Remove if equals operation.
                    checkOpMultithreaded(client,
                        () -> cache.put(0, 0),
                        () -> cache.remove(0, 1),
                        () -> assertEquals(0, cache.get(0))
                    );

                    // Remove operation.
                    checkOpMultithreaded(client,
                        () -> cache.put(0, 0),
                        () -> cache.remove(0),
                        () -> assertFalse(cache.containsKey(0))
                    );

                    // Remove keys operation.
                    checkOpMultithreaded(client,
                        () -> cache.putAll(F.asMap(0, 0, 1, 1)),
                        () -> cache.removeAll(new TreeSet<>(F.asList(0, 1))),
                        () -> assertFalse(cache.containsKeys(new TreeSet<>(F.asList(0, 1))))
                    );

                    // Replace if equals operation.
                    checkOpMultithreaded(client,
                        () -> cache.put(0, 0),
                        () -> cache.replace(0, 0, 1),
                        () -> assertEquals(1, cache.get(0))
                    );

                    // Replace operation.
                    checkOpMultithreaded(client,
                        () -> cache.put(0, 0),
                        () -> cache.replace(0, 1),
                        () -> assertEquals(1, cache.get(0))
                    );

                    // Invoke operation.
                    checkOpMultithreaded(client,
                        null,
                        () -> cache.invoke(0, new TestEntryProcessor(), 0),
                        () -> assertEquals(0, cache.get(0))
                    );

                    // Invoke all operation.
                    checkOpMultithreaded(client,
                        null,
                        () -> cache.invokeAll(new TreeSet<>(F.asList(0, 1)), new TestEntryProcessor(), 0),
                        () -> assertEquals(F.asMap(0, 0, 1, 0), cache.getAll(new TreeSet<>(F.asList(0, 1))))
                    );
                }
            }
        }
    }

    /** */
    private void checkOpMultithreaded(IgniteClient client, Runnable init, Runnable op, Runnable check) throws Exception {
        if (init != null)
            init.run();

        GridTestUtils.runMultiThreaded(() -> {
            for (int i = 0; i < 50; i++) {
                // Mix implicit and explicit transactions.
                if (ThreadLocalRandom.current().nextBoolean()) {
                    while (true) {
                        try (ClientTransaction tx = client.transactions().txStart(txConcurrency, txIsolation, TX_TIMEOUT)) {
                            op.run();

                            try {
                                tx.commit();

                                break;
                            }
                            catch (Exception e) {
                                if (!e.getMessage().contains("Failed to prepare transaction"))
                                    throw e;
                            }
                        }
                    }
                }
                else
                    op.run();
            }
        }, THREADS_CNT, "tx-thread");

        if (check != null)
            check.run();
    }

    /**
     * Tests transactional consistency on concurrent operations executed using async methods on server side.
     */
    @Test
    public void testTransactionalConsistency() throws Exception {
        poolSize = THREADS_CNT;

        startGrids(3);
        IgniteClient client = startClient(0, 1, 2);

        ClientCache<Integer, Integer> cache = client.getOrCreateCache(new ClientCacheConfiguration()
            .setName("test")
            .setAtomicityMode(TRANSACTIONAL)
            .setBackups(1)
        );

        int iterations = 1_000;
        int keys = 10;

        GridTestUtils.runMultiThreaded(() -> {
            for (int i = 0; i < iterations; i++) {
                try (ClientTransaction tx = client.transactions().txStart(txConcurrency, txIsolation, TX_TIMEOUT)) {
                    int key1 = ThreadLocalRandom.current().nextInt(keys);
                    int key2 = ThreadLocalRandom.current().nextInt(keys);
                    int sum = ThreadLocalRandom.current().nextInt(100);

                    if (key1 < key2) { // Avoid deadlocks
                        Integer val1 = cache.get(key1);
                        cache.put(key1, (val1 == null ? 0 : val1) - sum);
                        Integer val2 = cache.get(key2);
                        cache.put(key2, (val2 == null ? 0 : val2) + sum);
                    }
                    else {
                        Integer val2 = cache.get(key2);
                        cache.put(key2, (val2 == null ? 0 : val2) + sum);
                        Integer val1 = cache.get(key1);
                        cache.put(key1, (val1 == null ? 0 : val1) - sum);
                    }

                    if (ThreadLocalRandom.current().nextBoolean())
                        try {
                            tx.commit();
                        }
                        catch (Exception e) {
                            if (!e.getMessage().contains("Failed to prepare transaction"))
                                throw e;
                        }
                    else
                        tx.rollback();
                }
            }
        }, THREADS_CNT, "tx-thread");

        int sum = 0;

        for (int i = 0; i < keys; i++) {
            Integer val = cache.get(i);

            if (val != null)
                sum += val;
        }

        assertEquals(0, sum);
    }

    /**
     * Tests async commit future chaining with incompleted last operation async future.
     */
    @Test
    public void testCommitFutureChaining() throws Exception {
        poolSize = 1;

        try (Ignite ignore = startGrid(0)) {
            try (IgniteClient client = startClient(0)) {
                ClientCache<Integer, Integer> cache = client.getOrCreateCache(new ClientCacheConfiguration()
                    .setName("test")
                    .setAtomicityMode(TRANSACTIONAL)
                    .setBackups(1)
                );

                int iterations = 100;

                GridTestUtils.runMultiThreaded(() -> {
                    for (int i = 0; i < iterations; i++) {
                        if (ThreadLocalRandom.current().nextBoolean()) {
                            try (ClientTransaction tx = client.transactions().txStart(txConcurrency, txIsolation, TX_TIMEOUT)) {
                                cache.putAsync(0, 0);
                                tx.commit();
                            }
                        }
                        else
                            cache.put(0, 0);
                    }
                }, THREADS_CNT, "tx-thread");
            }
        }
    }

    /** */
    static class TestEntryProcessor implements EntryProcessor<Object, Object, Object> {
        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Object, Object> e, Object... args) {
            if (args != null && args.length >= 1)
                e.setValue(args[0]);

            return null;
        }
    }
}
