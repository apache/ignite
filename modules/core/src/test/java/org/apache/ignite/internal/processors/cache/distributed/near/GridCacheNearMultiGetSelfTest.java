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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.apache.log4j.Level;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.NONE;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Test getting the same value twice within the same transaction.
 */
public class GridCacheNearMultiGetSelfTest extends GridCommonAbstractTest {
    /** Cache debug flag. */
    private static final boolean CACHE_DEBUG = false;

    /** Number of gets. */
    private static final int GET_CNT = 5;

    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @SuppressWarnings({"ConstantConditions"})
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.getTransactionConfiguration().setTxSerializableEnabled(true);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setBackups(1);
        cc.setAtomicityMode(TRANSACTIONAL);

        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        cc.setRebalanceMode(NONE);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);
        spi.setMaxMissedHeartbeats(Integer.MAX_VALUE);

        c.setDiscoverySpi(spi);

        c.setCacheConfiguration(cc);

        if (CACHE_DEBUG)
            resetLog4j(Level.DEBUG, false, GridCacheProcessor.class.getPackage().getName());

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 0; i < GRID_CNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (int i = 0; i < GRID_CNT; i++) {
            Ignite g = grid(i);

            IgniteCache<Integer, String> c = g.cache(null);

            c.removeAll();

            assertEquals("Cache size mismatch for grid [grid=" + g.name() + ", entrySet=" + entrySet(c) + ']',
                0, c.size());
        }
    }

    /** @return {@code True} if debug enabled. */
    private boolean isTestDebug() {
        return true;
    }

    /** @throws Exception If failed. */
    public void testOptimisticReadCommittedNoPut() throws Exception {
        checkDoubleGet(OPTIMISTIC, READ_COMMITTED, false);
    }

    /** @throws Exception If failed. */
    public void testOptimisticReadCommittedWithPut() throws Exception {
        checkDoubleGet(OPTIMISTIC, READ_COMMITTED, true);
    }

    /** @throws Exception If failed. */
    public void testOptimisticReadCommitted() throws Exception {
        checkDoubleGet(OPTIMISTIC, READ_COMMITTED, false);
        checkDoubleGet(OPTIMISTIC, READ_COMMITTED, true);
    }

    /** @throws Exception If failed. */
    public void testOptimisticRepeatableReadNoPut() throws Exception {
        checkDoubleGet(OPTIMISTIC, REPEATABLE_READ, false);
    }

    /** @throws Exception If failed. */
    public void testOptimisticRepeatableReadWithPut() throws Exception {
        checkDoubleGet(OPTIMISTIC, REPEATABLE_READ, true);
    }

    /** @throws Exception If failed. */
    public void testOptimisticRepeatableRead() throws Exception {
        checkDoubleGet(OPTIMISTIC, REPEATABLE_READ, false);
        checkDoubleGet(OPTIMISTIC, REPEATABLE_READ, true);
    }

    /** @throws Exception If failed. */
    public void testOptimisticSerializableNoPut() throws Exception {
        checkDoubleGet(OPTIMISTIC, SERIALIZABLE, false);
    }

    /** @throws Exception If failed. */
    public void testOptimisticSerializableWithPut() throws Exception {
        checkDoubleGet(OPTIMISTIC, SERIALIZABLE, true);
    }

    /** @throws Exception If failed. */
    public void testOptimisticSerializable() throws Exception {
        checkDoubleGet(OPTIMISTIC, SERIALIZABLE, false);
        checkDoubleGet(OPTIMISTIC, SERIALIZABLE, true);
    }

    /** @throws Exception If failed. */
    public void testPessimisticReadCommittedNoPut() throws Exception {
        checkDoubleGet(PESSIMISTIC, READ_COMMITTED, false);
    }

    /** @throws Exception If failed. */
    public void testPessimisticReadCommittedWithPut() throws Exception {
        checkDoubleGet(PESSIMISTIC, READ_COMMITTED, true);
    }

    /** @throws Exception If failed. */
    public void testPessimisticReadCommitted() throws Exception {
        checkDoubleGet(PESSIMISTIC, READ_COMMITTED, false);
        checkDoubleGet(PESSIMISTIC, READ_COMMITTED, true);
    }

    /** @throws Exception If failed. */
    public void testPessimisticRepeatableReadNoPut() throws Exception {
        checkDoubleGet(PESSIMISTIC, REPEATABLE_READ, false);
    }

    /** @throws Exception If failed. */
    public void testPessimisticRepeatableReadWithPut() throws Exception {
        checkDoubleGet(PESSIMISTIC, REPEATABLE_READ, true);
    }

    /** @throws Exception If failed. */
    public void testPessimisticRepeatableRead() throws Exception {
        checkDoubleGet(PESSIMISTIC, REPEATABLE_READ, false);
        checkDoubleGet(PESSIMISTIC, REPEATABLE_READ, true);
    }

    /** @throws Exception If failed. */
    public void testPessimisticSerializableNoPut() throws Exception {
        checkDoubleGet(PESSIMISTIC, SERIALIZABLE, false);
    }

    /** @throws Exception If failed. */
    public void testPessimisticSerializableWithPut() throws Exception {
        checkDoubleGet(PESSIMISTIC, SERIALIZABLE, true);
    }

    /** @throws Exception If failed. */
    public void testPessimisticSerializable() throws Exception {
        checkDoubleGet(PESSIMISTIC, SERIALIZABLE, false);
        checkDoubleGet(PESSIMISTIC, SERIALIZABLE, true);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param put If {@code true}, then value will be pre-stored in cache.
     * @throws Exception If failed.
     */
    private void checkDoubleGet(TransactionConcurrency concurrency, TransactionIsolation isolation, boolean put)
        throws Exception {
        IgniteEx ignite = grid(0);
        IgniteCache<Integer, String> cache = ignite.cache(null);

        Integer key = 1;

        String val = null;

        if (put)
            cache.put(key, val = Integer.toString(key));

        Transaction tx = ignite.transactions().txStart(concurrency, isolation, 0, 0);

        try {
            if (isTestDebug()) {
                info("Started transaction.");

                Affinity<Integer> aff = affinity(cache);

                int part = aff.partition(key);

                if (isTestDebug())
                    info("Key affinity [key=" + key + ", partition=" + part + ", affinity=" +
                        U.toShortString(aff.mapKeyToPrimaryAndBackups(key)) + ']');
            }

            for (int i = 0; i < GET_CNT; i++) {
                if (isTestDebug())
                    info("Reading key [key=" + key + ", i=" + i + ']');

                String v = cache.get(key);

                assertEquals("Value mismatch for put [val=" + val + ", v=" + v + ", put=" + put + ']', val, v);

                if (isTestDebug())
                    info("Read value for key (will read again) [key=" + key + ", val=" + v + ", i=" + i + ']');
            }

            if (isTestDebug())
                info("Committing transaction.");

            tx.commit();

            if (isTestDebug())
                info("Committed transaction: " + tx);
        }
        catch (TransactionOptimisticException e) {
            if (concurrency != OPTIMISTIC || isolation != SERIALIZABLE) {
                error("Received invalid optimistic failure.", e);

                throw e;
            }

            if (isTestDebug())
                info("Optimistic transaction failure (will rollback) [msg=" + e.getMessage() +
                    ", tx=" + tx.xid() + ']');

            try {
                tx.rollback();
            }
            catch (IgniteException ex) {
                error("Failed to rollback optimistic failure: " + tx, ex);

                throw ex;
            }
        }
        catch (Exception e) {
            error("Transaction failed (will rollback): " + tx, e);

            tx.rollback();

            throw e;
        }
        catch (Error e) {
            error("Error when executing transaction (will rollback): " + tx, e);

            tx.rollback();

            throw e;
        }
        finally {
            Transaction t = ignite.transactions().tx();

            assert t == null : "Thread should not have transaction upon completion ['t==tx'=" + (t == tx) +
                ", t=" + t + (t != tx ? "tx=" + tx : "tx=''") + ']';
        }
    }
}