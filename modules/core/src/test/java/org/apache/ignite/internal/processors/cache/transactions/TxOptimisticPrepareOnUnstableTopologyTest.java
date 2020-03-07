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

import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 * Tests optimistic prepare on unstable topology.
 */
public class TxOptimisticPrepareOnUnstableTopologyTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_NAME = "part_cache";

    /** */
    private static final int STARTUP_DELAY = 500;

    /** */
    private static final int GRID_CNT = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setName(CACHE_NAME);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(PRIMARY_SYNC);
        ccfg.setBackups(2);
        ccfg.setCacheMode(PARTITIONED);

        c.setCacheConfiguration(ccfg);

        return c;
    }

    /**
     *
     */
    @Test
    public void testPrepareOnUnstableTopology() throws Exception {
        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            doPrepareOnUnstableTopology(4, false, isolation, 0);
            doPrepareOnUnstableTopology(4, true, isolation, 0);
            doPrepareOnUnstableTopology(4, false, isolation, TimeUnit.DAYS.toMillis(1));
            doPrepareOnUnstableTopology(4, true, isolation, TimeUnit.DAYS.toMillis(1));
        }
    }

    /**
     * @param keys Keys.
     * @param testClient Test client.
     * @param isolation Isolation.
     * @param timeout Timeout.
     */
    private void doPrepareOnUnstableTopology(int keys, boolean testClient, TransactionIsolation isolation,
        long timeout) throws Exception {
        GridCompoundFuture<Void, Object> compFut = new GridCompoundFuture<>();

        AtomicBoolean stopFlag = new AtomicBoolean();

        try {
            int clientIdx = testClient ? 1 : -1;

            try {
                for (int i = 0; i < GRID_CNT; i++) {
                    IgniteEx grid;

                    if (clientIdx == i)
                        grid = startClientGrid(i);
                    else
                        grid = startGrid(i);

                    assertEquals(clientIdx == i, grid.configuration().isClientMode().booleanValue());

                    IgniteInternalFuture<Void> fut = runCacheOperationsAsync(grid, stopFlag, isolation, timeout, keys);

                    compFut.add(fut);

                    U.sleep(STARTUP_DELAY);
                }
            }
            finally {
                stopFlag.set(true);
            }

            compFut.markInitialized();

            compFut.get();

            for (int i = 0; i < GRID_CNT; i++) {
                IgniteTxManager tm = ((IgniteKernal)grid(i)).internalCache(CACHE_NAME).context().tm();

                assertEquals("txMap is not empty: " + i, 0, tm.idMapSize());
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param node Node.
     * @param isolation Isolation.
     * @param timeout Timeout.
     * @param keys Number of keys.
     * @return Future representing pending completion of the operation.
     */
    private IgniteInternalFuture<Void> runCacheOperationsAsync(
        Ignite node,
        AtomicBoolean stopFlag,
        TransactionIsolation isolation,
        long timeout,
        final int keys
    ) {
        return GridTestUtils.runAsync(() -> {
            while (!stopFlag.get()) {
                TreeMap<Integer, String> vals = generateValues(keys);

                try {
                    try (Transaction tx = node.transactions().txStart(TransactionConcurrency.OPTIMISTIC, isolation,
                        timeout, keys)) {

                        IgniteCache<Object, Object> cache = node.cache(CACHE_NAME);

                        // Put or remove.
                        if (ThreadLocalRandom.current().nextDouble(1) < 0.65)
                            cache.putAll(vals);
                        else
                            cache.removeAll(vals.keySet());

                        tx.commit();
                    }
                    catch (Exception e) {
                        U.error(log(), "Failed cache operation.", e);
                    }

                    U.sleep(100);
                }
                catch (Exception e) {
                    U.error(log(), "Failed unlock.", e);
                }
            }

            return null;
        });
    }

    /**
     * @param cnt Number of keys to generate.
     * @return Map.
     */
    private TreeMap<Integer, String> generateValues(int cnt) {
        TreeMap<Integer, String> res = new TreeMap<>();

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        while (res.size() < cnt) {
            int key = rnd.nextInt(0, 100);

            res.put(key, String.valueOf(key));
        }

        return res;
    }
}
