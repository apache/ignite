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

import java.util.Arrays;
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
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

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
@WithSystemProperty(key = "IGNITE_UNWIND_THROTTLING_TIMEOUT", value = "5")
public class GridCachePartitionedEvictionSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int GRID_CNT = 2;

    /** */
    private static final int EVICT_CACHE_SIZE = 1;

    /** */
    private static final int KEY_CNT = 100;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        c.getTransactionConfiguration().setTxSerializableEnabled(true);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setWriteSynchronizationMode(FULL_SYNC);

        FifoEvictionPolicy plc = new FifoEvictionPolicy();
        plc.setMaxSize(EVICT_CACHE_SIZE);
        cc.setEvictionPolicy(plc);
        cc.setOnheapCacheEnabled(true);

        FifoEvictionPolicy nearPlc = new FifoEvictionPolicy();
        nearPlc.setMaxSize(EVICT_CACHE_SIZE);
        cc.getNearConfiguration().setNearEvictionPolicy(nearPlc);

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
        return G.ignite(node.id()).cache(DEFAULT_CACHE_NAME);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEvictionTxPessimisticReadCommitted() throws Exception {
        doTestEviction(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEvictionTxPessimisticRepeatableRead() throws Exception {
        doTestEviction(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEvictionTxPessimisticSerializable() throws Exception {
        doTestEviction(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEvictionTxOptimisticReadCommitted() throws Exception {
        doTestEviction(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEvictionTxOptimisticRepeatableRead() throws Exception {
        doTestEviction(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @Test
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

        TouchedExpiryPolicy plc = new TouchedExpiryPolicy(new Duration(MILLISECONDS, 37));

        for (int kv = 0; kv < KEY_CNT; kv++) {
            Thread.sleep(40);

            String key = String.valueOf(kv);

            ClusterNode node = aff.mapKeyToNode(key);

            IgniteCache<String, Integer> c = cache(node);

            IgniteTransactions txs = G.ignite(node.id()).transactions();

            try (Transaction tx = txs.txStart(concurrency, isolation)) {
                assert c.get(key) == null;

                c.withExpiryPolicy(plc).put(key, kv);

                assertEquals(Integer.valueOf(kv), c.get(key));

                tx.commit();
            }
        }

        boolean[] seen = {false, false, false, false};

        long started = System.currentTimeMillis();

        for (int i = 0; i < 1000; i++) {
            long dht0Keys = 0, dht1Keys = 0;

            seen[2] |= dht0.size() == EVICT_CACHE_SIZE;
            seen[3] |= dht1.size() == EVICT_CACHE_SIZE;

            info("Printing keys in dht0...");

            for (String key : dht0.keySet())
                info("[key=" + key + ", primary=" +
                    F.eqNodes(grid(0).localNode(), aff.mapKeyToNode(key)) + ", " + dht0Keys++ + ']');

            info("Printing keys in dht1...");

            for (String key : dht1.keySet())
                info("[key=" + key + ", primary=" +
                    F.eqNodes(grid(1).localNode(), aff.mapKeyToNode(key)) + ", " + dht1Keys++ + ']');

            seen[0] |= dht0Keys == EVICT_CACHE_SIZE;
            seen[1] |= dht1Keys == EVICT_CACHE_SIZE;

            if (seen[0] && seen[1] && seen[2] && seen[3] || System.currentTimeMillis() - started > 100)
                break;
        }

        assertTrue(Arrays.toString(seen), seen[0] && seen[1] && seen[2] && seen[3]);

        assertEquals(0, near(jcache(0)).nearSize());
        assertEquals(0, near(jcache(1)).nearSize());
    }
}
