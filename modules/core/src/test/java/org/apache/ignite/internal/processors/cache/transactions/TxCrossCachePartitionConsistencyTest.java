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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Test scenario:
 * 1. Two keys from the same partition but in different caches are enlisted in a transaction
 * (caches have different affinity settings).
 * 2. Random node is restarted under load.
 *
 * Success: partitions are consistent, total balances invariant is held.
 */
public class TxCrossCachePartitionConsistencyTest extends GridCommonAbstractTest {
    /** Cache 1. */
    private static final String CACHE1 = DEFAULT_CACHE_NAME;

    /** Cache 2. */
    private static final String CACHE2 = DEFAULT_CACHE_NAME + "2";

    /** */
    private static final int MB = 1024 * 1024;

    /** */
    private static final int NODES_CNT = 3;

    /** */
    private static final int PARTS_CNT = 64;

    /** */
    private boolean persistenceEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(CACHE1, 2), cacheConfiguration(CACHE2, 1));

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().
            setWalSegmentSize(8 * MB).setWalMode(LOG_ONLY).setPageSize(1024).
            setCheckpointFrequency(MILLISECONDS.convert(365, DAYS)).
            setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(persistenceEnabled).
                setInitialSize(100 * MB).setMaxSize(300 * MB)));

        return cfg;
    }

    /**
     * @param name Name.
     */
    protected CacheConfiguration<Object, Object> cacheConfiguration(String name, int backups) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(name);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setBackups(backups);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setOnheapCacheEnabled(false);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT));

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testCrossCacheTxFailoverVolatile() throws Exception {
        doTestCrossCacheTxFailover(false);
    }

    /** */
    @Test
    public void testCrossCacheTxFailoverPersistent() throws Exception {
        doTestCrossCacheTxFailover(true);
    }

    /**
     * @param persistenceEnabled {@code True} if persistence is enabled.
     */
    private void doTestCrossCacheTxFailover(boolean persistenceEnabled) throws Exception {
        this.persistenceEnabled = persistenceEnabled;

        try {
            IgniteEx crd = startGrids(NODES_CNT);

            if (persistenceEnabled)
                crd.cluster().active(true);

            awaitPartitionMapExchange();

            Ignite client = startClientGrid("client");

            AtomicBoolean stop = new AtomicBoolean();

            final long balance = 1_000_000_000;

            List<Integer> keys = IntStream.range(0, PARTS_CNT).boxed().collect(Collectors.toList());

            preload(crd, keys, balance);

            Random r = new Random();

            BooleanSupplier stopPred = stop::get;

            IgniteInternalFuture<?> restartFut = multithreadedAsync(() -> {
                doSleep(2_000);

                Ignite restartNode = grid(r.nextInt(3));

                String name = restartNode.name();

                if (persistenceEnabled) {
                    stopGrid(true, name);

                    resetBaselineTopology();
                }
                else
                    stopGrid(name, true);

                try {
                    doSleep(2_000);

                    startGrid(name);

                    if (persistenceEnabled)
                        resetBaselineTopology();

                    awaitPartitionMapExchange();
                }
                catch (Exception e) {
                    fail(X.getFullStackTrace(e));
                }
                finally {
                    stop.set(true);
                }
            }, 1, "node-restarter");

            IgniteInternalFuture<?> txFut = doRandomUpdates(r, client, keys, stopPred);

            txFut.get();
            restartFut.get();

            awaitPartitionMapExchange();

            assertPartitionsSame(idleVerify(client, CACHE1, CACHE2));

            long s = 0;

            for (Integer key : keys) {
                Deposit o = (Deposit)client.cache(CACHE1).get(key);
                Deposit o2 = (Deposit)client.cache(CACHE2).get(key);

                s += o.balance;
                s += o2.balance;
            }

            assertEquals(keys.size() * 2L * balance, s);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param r Random.
     * @param near Near node.
     * @param keys Primary keys.
     * @param stopPred Stop predicate.
     * @return Finish future.
     */
    private IgniteInternalFuture<?> doRandomUpdates(Random r, Ignite near, List<Integer> keys, BooleanSupplier stopPred)
        throws Exception {
        IgniteCache<Integer, Deposit> cache1 = near.cache(CACHE1);
        IgniteCache<Integer, Deposit> cache2 = near.cache(CACHE2);

        return multithreadedAsync(() -> {
            while (!stopPred.getAsBoolean()) {
                int key = r.nextInt(keys.size());

                try (Transaction tx = near.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 0)) {
                    Deposit d1 = cache1.get(key);

                    assertNotNull(d1);

                    Deposit d2 = cache2.get(key);

                    d1.balance += 20;
                    d2.balance -= 20;

                    cache1.put(key, d1);
                    cache2.put(key, d2);

                    tx.commit();
                }
                catch (Exception e) {
                    assertTrue(X.getFullStackTrace(e), X.hasCause(e, ClusterTopologyException.class) ||
                        X.hasCause(e, TransactionRollbackException.class));
                }
            }
        }, Runtime.getRuntime().availableProcessors(), "tx-update-thread");
    }

    /**
     * @param node Node.
     * @param keys Keys.
     * @param balance Balance.
     */
    private void preload(IgniteEx node, List<Integer> keys, long balance) {
        try (IgniteDataStreamer<Object, Object> ds = node.dataStreamer(CACHE1)) {
            ds.allowOverwrite(true);

            for (Integer key : keys)
                ds.addData(key, new Deposit(key, balance));
        }

        try (IgniteDataStreamer<Object, Object> ds = node.dataStreamer(CACHE2)) {
            ds.allowOverwrite(true);

            for (Integer key : keys)
                ds.addData(key, new Deposit(key, balance));
        }
    }

    /**
     * @param skipCheckpointOnStop Skip checkpoint on stop.
     * @param name Grid instance.
     */
    protected void stopGrid(boolean skipCheckpointOnStop, String name) {
        IgniteEx grid = grid(name);

        if (skipCheckpointOnStop) {
            GridCacheDatabaseSharedManager db =
                (GridCacheDatabaseSharedManager)grid.context().cache().context().database();

            db.enableCheckpoints(false);
        }

        stopGrid(grid.name(), skipCheckpointOnStop);
    }

    /** Deposit. */
    private static class Deposit {
        /** User id. */
        public long userId;

        /** Balance. */
        public long balance;

        /**
         * @param userId User id.
         * @param balance Balance.
         */
        public Deposit(long userId, long balance) {
            this.userId = userId;
            this.balance = balance;
        }
    }
}
