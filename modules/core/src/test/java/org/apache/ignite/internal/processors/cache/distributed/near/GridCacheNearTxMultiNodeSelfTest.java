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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests near transactions.
 */
public class GridCacheNearTxMultiNodeSelfTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int GRID_CNT = 3;

    /** Number of backups for partitioned tests. */
    protected int backups = 1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        // Default cache configuration.
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setNearConfiguration(new NearCacheConfiguration());
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setBackups(backups);
        cacheCfg.setRebalanceMode(SYNC);

        cfg.setCacheConfiguration(cacheCfg);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        backups = 1;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings( {"unchecked"})
    public void testTxCleanup() throws Exception {
        backups = 1;

        Ignite ignite = startGrids(GRID_CNT);

        try {
            Integer mainKey = 0;

            ClusterNode priNode = ignite.cluster().mapKeyToNode(null, mainKey);
            ClusterNode backupNode = F.first(F.view(ignite.affinity(null).mapKeyToPrimaryAndBackups(mainKey),
                F.notIn(F.asList(priNode))));
            ClusterNode otherNode = F.first(ignite.cluster().forPredicate(F.notIn(F.asList(priNode, backupNode))).nodes());

            assert priNode != backupNode;
            assert backupNode != otherNode;
            assert priNode != otherNode;

            final Ignite priIgnite = grid(priNode);
            Ignite backupIgnite = grid(backupNode);
            Ignite otherIgnite = grid(otherNode);

            List<Ignite> ignites = F.asList(otherIgnite, priIgnite, backupIgnite);

            int cntr = 0;

            // Update main key from all nodes.
            for (Ignite g : ignites)
                g.cache(null).put(mainKey, ++cntr);

            info("Updated mainKey from all nodes.");

            int keyCnt = 200;

            Set<Integer> keys = new TreeSet<>();

            // Populate cache from all nodes.
            for (int i = 1; i <= keyCnt; i++) {
                keys.add(i);

                Ignite g = F.rand(ignites);

                g.cache(null).put(new AffinityKey<>(i, mainKey), Integer.toString(cntr++));
            }

            IgniteCache cache = priIgnite.cache(null);

            Transaction tx = priIgnite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

            try {
                cache.get(mainKey);

                cache.removeAll(keys);

                cache.put(mainKey, ++cntr);

                tx.commit();
            }
            catch (Error | Exception e) {
                error("Transaction failed: " + tx, e);

                throw e;
            } finally {
                tx.close();
            }

            stopGrid(priIgnite.name(), true);
            stopGrid(backupIgnite.name(), true);

            Ignite newIgnite = startGrid(GRID_CNT);

            ignites = F.asList(otherIgnite, newIgnite);

            for (Ignite g : ignites) {
                GridNearCacheAdapter near = ((IgniteKernal)g).internalCache().context().near();
                GridDhtCacheAdapter dht = near.dht();

                checkTm(g, near.context().tm());
                checkTm(g, dht.context().tm());
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxReadersUpdate() throws Exception {
        startGridsMultiThreaded(GRID_CNT);

        try {
            testReadersUpdate(OPTIMISTIC, REPEATABLE_READ);

            testReadersUpdate(PESSIMISTIC, REPEATABLE_READ);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     * @throws Exception If failed.
     */
    private void testReadersUpdate(TransactionConcurrency concurrency, TransactionIsolation isolation) throws Exception {
        Ignite ignite = grid(0);
        IgniteCache<Integer, Integer> cache = ignite.cache(null);

        try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
            for (int i = 0; i < 100; i++)
                cache.put(i, 1);

            tx.commit();
        }

        // Create readers.
        for (int g = 0; g < GRID_CNT; g++) {
            IgniteCache<Integer, Integer> c = grid(g).cache(null);

            for (int i = 0; i < 100; i++)
                assertEquals((Integer)1, c.get(i));
        }

        try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
            for (int i = 0; i < 100; i++)
                cache.put(i, 2);

            tx.commit();
        }

        for (int g = 0; g < GRID_CNT; g++) {
            IgniteCache<Integer, Integer> c = grid(g).cache(null);

            for (int i = 0; i < 100; i++)
                assertEquals((Integer)2, c.get(i));
        }
    }

    /**
     * @param g Grid.
     * @param tm Transaction manager.
     */
    @SuppressWarnings( {"unchecked"})
    private void checkTm(Ignite g, IgniteTxManager tm) {
        Collection<IgniteInternalTx> txs = tm.txs();

        info(">>> Number of transactions in the set [size=" + txs.size() +
            ", nodeId=" + g.cluster().localNode().id() + ']');

        for (IgniteInternalTx tx : txs)
            assert tx.done() : "Transaction is not finished: " + tx;
    }
}