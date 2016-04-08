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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.transactions.TxDeadlockException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.internal.util.typedef.X.hasCause;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests deadlock detection for pessimistic transactions.
 */
public class TxPessimisticDeadlockDetectionTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Client mode flag. */
    private static boolean client;

    /** Cache mode. */
    private static CacheMode cacheMode;

    /** Near config. */
    private static NearCacheConfiguration nearCfg;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (isDebug()) {
            TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

            discoSpi.failureDetectionTimeoutEnabled(false);

            cfg.setDiscoverySpi(discoSpi);
        }

        cfg.setClientMode(client);

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setName(CACHE_NAME);
        ccfg.setCacheMode(cacheMode);
        ccfg.setBackups(1);
        ccfg.setNearConfiguration(nearCfg);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 15 * 60_000L;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeadlocksPartitioned() throws Exception {
        cacheMode = PARTITIONED;
        nearCfg = null;

        doTestDeadlocks(false);
        doTestDeadlocks(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testDeadlocksPartitionedNear() throws Exception {
        cacheMode = PARTITIONED;
        nearCfg = new NearCacheConfiguration();

        doTestDeadlocks(false);
        doTestDeadlocks(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeadlocksReplicated() throws Exception {
        cacheMode = REPLICATED;
        nearCfg = null;

        doTestDeadlocks(false);
        doTestDeadlocks(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeadlocksLocal() throws Exception {
        cacheMode = LOCAL;
        nearCfg = null;

        doTestDeadlock(1, true, false, false);
        doTestDeadlock(1, true, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestDeadlocks(boolean userThreadTx) throws Exception {
        doTestDeadlock(2, true, false, userThreadTx);
        doTestDeadlock(3, true, false, userThreadTx);
        doTestDeadlock(4, true, false, userThreadTx);
        doTestDeadlock(10, true, false, userThreadTx);

        doTestDeadlock(2, true, true, userThreadTx);
        doTestDeadlock(3, true, true, userThreadTx);
        doTestDeadlock(4, true, true, userThreadTx);
        doTestDeadlock(10, true, true, userThreadTx);

        doTestDeadlock(2, false, false, userThreadTx);
        doTestDeadlock(3, false, false, userThreadTx);
        doTestDeadlock(4, false, false, userThreadTx);
        doTestDeadlock(10, false, false, userThreadTx);

        doTestDeadlock(2, false, true, userThreadTx);
        doTestDeadlock(3, false, true, userThreadTx);
        doTestDeadlock(4, false, true, userThreadTx);
        doTestDeadlock(10, false, true, userThreadTx);
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestDeadlock(
        final int nodesCnt,
        boolean lockPrimaryFirst,
        final boolean clientTx,
        final boolean userThreadTx
    ) throws Exception {
        try {
            log.info(">>> Test deadlock [nodesCnt=" + nodesCnt + ", lockPrimaryFirst=" + lockPrimaryFirst +
                    ", clientTx=" + clientTx + ", userThreadTx=" + userThreadTx + ']');

            client = false;

            startGrids(nodesCnt);

            if (clientTx) {
                client = true;

                for (int i = 0; i < nodesCnt; i++)
                    startGrid(i + nodesCnt);
            }

            final AtomicInteger threadCnt = new AtomicInteger();

            final CountDownLatch latch = new CountDownLatch(cacheMode != LOCAL ? nodesCnt : 2);
            final CountDownLatch startLatch = new CountDownLatch(cacheMode != LOCAL ? nodesCnt : 2);
            final CountDownLatch txLatch = new CountDownLatch(cacheMode != LOCAL ? nodesCnt : 2);

            final AtomicBoolean deadlock = new AtomicBoolean();

            final List<List<Integer>> keySets = generateKeys(nodesCnt, !lockPrimaryFirst);

            GridTestUtils.runMultiThreadedAsync(new Runnable() {
                @Override public void run() {
                    int threadNum = threadCnt.incrementAndGet();

                    Ignite ignite = cacheMode == LOCAL ?
                        ignite(0) : ignite(clientTx ? threadNum - 1 + nodesCnt : threadNum - 1);

                    IgniteCache<Integer, Integer> cache = ignite.cache(CACHE_NAME);

                    List<Integer> keys = keySets.get(threadNum - 1);

                    startLatch.countDown();

                    try {
                        startLatch.await();
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    try (Transaction tx =
                             ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, userThreadTx ? 0 : 1500, 0)
                    ) {
                        Integer key = keys.get(0);

                        log.info(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode() +
                            ", tx=" + tx + ", key=" + key + ']');

                        cache.put(key, 0);

                        latch.countDown();

                        latch.await();

                        key = keys.get(1);

                        ClusterNode primaryNode =
                            ((IgniteCacheProxy)cache).context().affinity().primary(key, AffinityTopologyVersion.NONE);

                        List<Integer> primaryKeys = primaryKeys(grid(primaryNode).cache(CACHE_NAME), 5, key + (100 *
                            threadNum));

                        Map<Integer, Integer> entries = new HashMap<>();

                        entries.put(key, 0);

                        for (Integer k : primaryKeys) {
                            entries.put(k, 1);

                            entries.put(k + 1, 2);

                            entries.put(k + 13, 2);
                        }

                        log.info(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode() +
                            ", tx=" + tx + ", entries=" + entries + ']');

                        cache.putAll(entries);

                        tx.commit();
                    }
                    catch (Exception e) {
                        if (!userThreadTx) {
                            // At least one stack trace should contain TxDeadlockException.
                            if (hasCause(e, IgniteTxTimeoutCheckedException.class) &&
                                    hasCause(e, TxDeadlockException.class)
                            ) {
                                if (deadlock.compareAndSet(false, true))
                                    U.error(log, "At least one stack trace should contain TxDeadlockException: ", e);
                            }
                        }
                    }
                    finally {
                        if (!userThreadTx)
                            txLatch.countDown();
                    }
                }
            }, cacheMode == LOCAL ? 2 : nodesCnt, "tx-thread");

            if (userThreadTx) {
                latch.await();

                U.sleep(500);

                Ignite ignite = ignite(0);

                try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 1500, 0)) {
                    IgniteCache<Integer, Integer> cache = ignite.cache(CACHE_NAME);

                    Integer key = primaryKey(ignite.cache(CACHE_NAME));

                    log.info(">>> Performs put from user thread [node=" + ((IgniteKernal)ignite).localNode() +
                        ", tx=" + tx + ", key=" + key + ']');

                    cache.put(key, 42);

                    tx.commit();
                }
                catch (Exception e) {
                    U.error(log, "Stack trace should contain TxDeadlockException: ", e);

                    if (hasCause(e, IgniteTxTimeoutCheckedException.class) && hasCause(e, TxDeadlockException.class))
                        deadlock.set(true);
                }
            }
            else
                txLatch.await();

            U.sleep(1000);

            assertTrue(deadlock.get());
        }
        catch (Exception e) {
            U.error(log, "Unexpected exception: ", e);

            fail();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param nodesCnt Nodes count.
     */
    private List<List<Integer>> generateKeys(int nodesCnt, boolean reverse) throws IgniteCheckedException {
        List<List<Integer>> keySets = new ArrayList<>();

        if (cacheMode != LOCAL) {
            for (int i = 0; i < nodesCnt; i++) {
                List<Integer> keys = new ArrayList<>(2);

                keys.add(primaryKey(ignite(i).cache(CACHE_NAME)));
                keys.add(primaryKey(ignite(i == nodesCnt - 1 ? 0 : i + 1).cache(CACHE_NAME)));

                if (reverse)
                    Collections.reverse(keys);

                keySets.add(keys);
            }
        }
        else {
            List<Integer> keys = primaryKeys(ignite(0).cache(CACHE_NAME), 2);

            keySets.add(new ArrayList<>(keys));

            Collections.reverse(keys);

            keySets.add(keys);
        }

        return keySets;
    }
}
