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

import javax.cache.expiry.Duration;
import javax.cache.expiry.TouchedExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Tests for partitioned cache automatic eviction.
 */
public class GridCachePartitionedEvictionSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final boolean TEST_INFO = true;

    /** */
    private static final int GRID_CNT = 2;

    /** */
    private static final int EVICT_CACHE_SIZE = 1;

    /** */
    private static final int KEY_CNT = 100;

    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.getTransactionConfiguration().setTxSerializableEnabled(true);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        c.setDiscoverySpi(spi);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setWriteSynchronizationMode(FULL_SYNC);

        FifoEvictionPolicy plc = new FifoEvictionPolicy();
        plc.setMaxSize(EVICT_CACHE_SIZE);
        cc.setEvictionPolicy(plc);

        FifoEvictionPolicy nearPlc = new FifoEvictionPolicy();
        nearPlc.setMaxSize(EVICT_CACHE_SIZE);
        cc.getNearConfiguration().setNearEvictionPolicy(nearPlc);

        cc.setSwapEnabled(false);

        // We set 1 backup explicitly.
        cc.setBackups(1);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @param node Node.
     * @return Cache.
     */
    private IgniteCache<String, Integer> cache(ClusterNode node) {
        return G.ignite(node.id()).cache(null);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testEvictionTxPessimisticReadCommitted() throws Exception {
        doTestEviction(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testEvictionTxPessimisticRepeatableRead() throws Exception {
        doTestEviction(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testEvictionTxPessimisticSerializable() throws Exception {
        doTestEviction(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testEvictionTxOptimisticReadCommitted() throws Exception {
        doTestEviction(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testEvictionTxOptimisticRepeatableRead() throws Exception {
        doTestEviction(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testEvictionTxOptimisticSerializable() throws Exception {
        doTestEviction(OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     * @param concurrency Tx concurrency.
     * @param isolation Tx isolation.
     */
    private void doTestEviction(TransactionConcurrency concurrency, TransactionIsolation isolation)
        throws Exception {
        assert concurrency != null;
        assert isolation != null;

        // This condition should be "true", otherwise the test doesn't make sense.
        assert KEY_CNT >= EVICT_CACHE_SIZE;

        GridDhtCacheAdapter<String, Integer> dht0 = dht(jcache(0));
        GridDhtCacheAdapter<String, Integer> dht1 = dht(jcache(1));

        Affinity<String> aff = dht0.affinity();

        TouchedExpiryPolicy plc = new TouchedExpiryPolicy(new Duration(MILLISECONDS, 10));

        for (int kv = 0; kv < KEY_CNT; kv++) {
            String key = String.valueOf(kv);

            ClusterNode node = aff.mapKeyToNode(key);

            IgniteCache<String, Integer> c = cache(node);

            IgniteTransactions txs = G.ignite(node.id()).transactions();

            try (Transaction tx = txs.txStart(concurrency, isolation)) {
                assert c.get(key) == null;

                c.withExpiryPolicy(plc).put(key, 1);

                assertEquals(Integer.valueOf(kv), c.get(key));

                tx.commit();
            }
        }

        if (TEST_INFO) {
            info("Printing keys in dht0...");

            for (String key : dht0.keySet())
                info("[key=" + key + ", primary=" +
                    F.eqNodes(grid(0).localNode(), aff.mapKeyToNode(key)) + ']');

            info("Printing keys in dht1...");

            for (String key : dht1.keySet())
                info("[key=" + key + ", primary=" +
                    F.eqNodes(grid(1).localNode(), aff.mapKeyToNode(key)) + ']');
        }

        assertEquals(EVICT_CACHE_SIZE, dht0.size());
        assertEquals(EVICT_CACHE_SIZE, dht1.size());

        assertEquals(0, near(jcache(0)).nearSize());
        assertEquals(0, near(jcache(1)).nearSize());
    }
}