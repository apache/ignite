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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@RunWith(Parameterized.class)
public class GridExchangeFreeCellularSwitchComplexOperationsTest extends GridExchangeFreeCellularSwitchAbstractTest {
    /** Concurrency. */
    @Parameterized.Parameter(0)
    public TransactionConcurrency concurrency;

    /** Isolation. */
    @Parameterized.Parameter(1)
    public TransactionIsolation isolation;

    /** Start from. */
    @Parameterized.Parameter(2)
    public TransactionCoordinatorNode startFrom;

    /**
     *
     */
    @Parameterized.Parameters(name = "Isolation = {0}, Concurrency = {1}, Started from = {2}")
    public static Collection<Object[]> runConfig() {
        ArrayList<Object[]> params = new ArrayList<>();
        for (TransactionConcurrency concurrency : TransactionConcurrency.values())
            for (TransactionIsolation isolation : TransactionIsolation.values())
                for (TransactionCoordinatorNode from : TransactionCoordinatorNode.values())
                    params.add(new Object[] {concurrency, isolation, from});

        return params;
    }

    /**
     *
     */
    @Test
    public void testComplexOperationsRecoveryOnCellularSwitch() throws Exception {
        int nodes = 6;

        startGridsMultiThreaded(nodes);

        blockRecoveryMessages();

        Ignite failed = G.allGrids().get(new Random().nextInt(nodes));

        Integer partKey = primaryKey(failed.getOrCreateCache(PART_CACHE_NAME));

        List<Ignite> backupNodes = backupNodes(partKey, PART_CACHE_NAME);
        List<Ignite> nearNodes = new ArrayList<>(G.allGrids());

        nearNodes.remove(failed);
        nearNodes.removeAll(backupNodes);

        assertTrue(Collections.disjoint(backupNodes, nearNodes));
        assertEquals(nodes / 2 - 1, backupNodes.size()); // Cell 1.
        assertEquals(nodes / 2, nearNodes.size()); // Cell 2.

        Ignite orig;

        switch (startFrom) {
            case PRIMARY:
                orig = failed;

                break;

            case BACKUP:
                orig = backupNodes.get(0);

                break;

            case NEAR:
                orig = nearNodes.get(0);

                break;

            case CLIENT:
                orig = startClientGrid();

                break;

            default:
                throw new UnsupportedOperationException();
        }

        int recFutsCnt = 7;

        CountDownLatch prepLatch = new CountDownLatch(recFutsCnt);
        CountDownLatch commitLatch = new CountDownLatch(1);

        Set<Integer> partSet = new GridConcurrentHashSet<>();
        Set<Integer> replSet = new GridConcurrentHashSet<>();

        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        AtomicInteger cnt = new AtomicInteger();

        BiFunction<Ignite, String, Integer> nextPrimaryKey = (ignite, cacheName) -> {
            int idx = cnt.getAndIncrement();

            return primaryKeys(ignite.getOrCreateCache(cacheName), idx + 1).get(idx);
        };

        BiConsumer<String, Set<Integer>> singlePutEverywhere = (cacheName, globSet) -> {
            try {
                Transaction tx = orig.transactions().txStart(concurrency, isolation);

                Set<Integer> set = new HashSet<>();

                for (Ignite ignite : G.allGrids()) {
                    if (ignite.configuration().isClientMode())
                        continue;

                    set.add(nextPrimaryKey.apply(ignite, cacheName));
                }

                globSet.addAll(set);

                IgniteCache<Integer, Integer> cache = orig.getOrCreateCache(cacheName);

                for (Integer key : set)
                    cache.put(key, key);

                ((TransactionProxyImpl<?, ?>)tx).tx().prepare(true);

                prepLatch.countDown();

                commitLatch.await();

                if (orig != failed)
                    ((TransactionProxyImpl<?, ?>)tx).commit();
            }
            catch (Exception e) {
                fail("Should not happen [exception=" + e + "]");
            }
        };

        futs.add(multithreadedAsync(() -> singlePutEverywhere.accept(PART_CACHE_NAME, partSet), 1));
        futs.add(multithreadedAsync(() -> singlePutEverywhere.accept(REPL_CACHE_NAME, replSet), 1));

        Consumer<Integer> putEverywhereToBoth = (putPerTx) -> {
            try {
                Transaction tx = orig.transactions().txStart(concurrency, isolation);

                Set<Integer> pSet = new HashSet<>();
                Set<Integer> rSet = new HashSet<>();

                for (int i = 0; i < putPerTx; i++)
                    for (Ignite ignite : G.allGrids()) {
                        if (ignite.configuration().isClientMode())
                            continue;

                        pSet.add(nextPrimaryKey.apply(ignite, PART_CACHE_NAME));
                        rSet.add(nextPrimaryKey.apply(ignite, REPL_CACHE_NAME));
                    }

                partSet.addAll(pSet);
                replSet.addAll(rSet);

                IgniteCache<Integer, Integer> pCache = orig.getOrCreateCache(PART_CACHE_NAME);
                IgniteCache<Integer, Integer> rCache = orig.getOrCreateCache(REPL_CACHE_NAME);

                for (Integer key : pSet)
                    pCache.put(key, key);

                for (Integer key : rSet)
                    rCache.put(key, key);

                ((TransactionProxyImpl<?, ?>)tx).tx().prepare(true);

                prepLatch.countDown();

                commitLatch.await();

                if (orig != failed)
                    ((TransactionProxyImpl<?, ?>)tx).commit();
            }
            catch (Exception e) {
                fail("Should not happen [exception=" + e + "]");
            }
        };

        futs.add(multithreadedAsync(() -> putEverywhereToBoth.accept(1), 1));
        futs.add(multithreadedAsync(() -> putEverywhereToBoth.accept(2), 1));
        futs.add(multithreadedAsync(() -> putEverywhereToBoth.accept(10), 1));

        Consumer<Boolean> singleTxPerCell = (partAtCell1) -> {
            try {
                Transaction tx = orig.transactions().txStart(concurrency, isolation);

                Integer pKey = partAtCell1 ? nextPrimaryKey.apply(failed, PART_CACHE_NAME) :
                    nextPrimaryKey.apply(nearNodes.get(0), PART_CACHE_NAME);

                Integer rKey = partAtCell1 ? nextPrimaryKey.apply(nearNodes.get(0), REPL_CACHE_NAME) :
                    nextPrimaryKey.apply(failed, REPL_CACHE_NAME);

                IgniteCache<Integer, Integer> pCache = orig.getOrCreateCache(PART_CACHE_NAME);
                IgniteCache<Integer, Integer> rCache = orig.getOrCreateCache(REPL_CACHE_NAME);

                pCache.put(pKey, pKey);
                rCache.put(rKey, rKey);

                partSet.add(pKey);
                replSet.add((rKey));

                ((TransactionProxyImpl<?, ?>)tx).tx().prepare(true);

                prepLatch.countDown();

                commitLatch.await();

                if (orig != failed)
                    ((TransactionProxyImpl<?, ?>)tx).commit();
            }
            catch (Exception e) {
                fail("Should not happen [exception=" + e + "]");
            }
        };

        futs.add(multithreadedAsync(() -> singleTxPerCell.accept(true), 1));
        futs.add(multithreadedAsync(() -> singleTxPerCell.accept(false), 1));

        prepLatch.await();

        assertEquals(futs.size(), recFutsCnt);

        failed.close(); // Stopping node.

        awaitForSwitchOnNodeLeft(failed);

        Consumer<Ignite> partTxRun = (ignite) -> {
            try {
                IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(PART_CACHE_NAME);

                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                    Integer key = nextPrimaryKey.apply(ignite, PART_CACHE_NAME);

                    partSet.add(key);

                    cache.put(key, key);

                    tx.commit();
                }
            }
            catch (Exception e) {
                fail("Should not happen [exception=" + e + "]");
            }
        };

        Consumer<Ignite> replTxRun = (ignite) -> {
            try {
                IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(REPL_CACHE_NAME);

                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                    Integer key = nextPrimaryKey.apply(ignite, REPL_CACHE_NAME);

                    replSet.add(key);

                    cache.put(key, key);

                    tx.commit();
                }
            }
            catch (Exception e) {
                fail("Should not happen [exception=" + e + "]");
            }
        };

        for (Ignite backup : backupNodes) {
            futs.add(multithreadedAsync(() -> partTxRun.accept(backup), 1));
            futs.add(multithreadedAsync(() -> replTxRun.accept(backup), 1));
        }

        for (Ignite near : nearNodes) {
            futs.add(multithreadedAsync(() -> partTxRun.accept(near), 1));
            futs.add(multithreadedAsync(() -> replTxRun.accept(near), 1));
        }

        // Allowing recovery.
        for (Ignite ignite : G.allGrids()) {
            TestRecordingCommunicationSpi spi =
                (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

            spi.stopBlock(true, (t) -> true);
        }

        commitLatch.countDown();

        for (IgniteInternalFuture<?> fut : futs)
            fut.get();

        for (Ignite node : G.allGrids()) {
            IgniteCache<Integer, Integer> partCache = node.getOrCreateCache(PART_CACHE_NAME);
            IgniteCache<Integer, Integer> replCache = node.getOrCreateCache(REPL_CACHE_NAME);

            for (Integer key : partSet)
                assertEquals(key, partCache.get(key));

            for (Integer key : replSet)
                assertEquals(key, replCache.get(key));
        }

        // Final check that any transactions are absent.
        checkTransactionsCount(
            null, 0,
            null, 0,
            backupNodes, 0,
            nearNodes, 0,
            null);
    }
}
