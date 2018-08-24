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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionTimeoutException;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion.NONE;
import static org.apache.ignite.internal.util.typedef.X.cause;
import static org.apache.ignite.internal.util.typedef.X.hasCause;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests deadlock detection for pessimistic transactions.
 */
public class TxPessimisticDeadlockDetectionTest extends AbstractDeadlockDetectionTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Nodes count (actually two times more nodes will started: server + client). */
    private static final int NODES_CNT = 4;

    /** Ordinal start key. */
    private static final Integer ORDINAL_START_KEY = 1;

    /** Custom start key. */
    private static final IncrementalTestObject CUSTOM_START_KEY = new KeyObject(1);

    /** Client mode flag. */
    private static boolean client;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (isDebug()) {
            TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

            discoSpi.failureDetectionTimeoutEnabled(false);

            cfg.setDiscoverySpi(discoSpi);
        }

        DataStorageConfiguration memCfg = new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration()
                .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_MAX_SIZE * 10)
                .setName("dfltPlc"));

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        client = false;

        startGridsMultiThreaded(NODES_CNT);

        client = true;

        for (int i = 0; i < NODES_CNT; i++)
            startGrid(i + NODES_CNT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeadlocksPartitioned() throws Exception {
        for (CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values()) {
            doTestDeadlocks(createCache(PARTITIONED, syncMode, false), ORDINAL_START_KEY);
            doTestDeadlocks(createCache(PARTITIONED, syncMode, false), CUSTOM_START_KEY);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeadlocksPartitionedNear() throws Exception {
        for (CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values()) {
            doTestDeadlocks(createCache(PARTITIONED, syncMode, true), ORDINAL_START_KEY);
            doTestDeadlocks(createCache(PARTITIONED, syncMode, true), CUSTOM_START_KEY);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeadlocksReplicated() throws Exception {
        for (CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values()) {
            doTestDeadlocks(createCache(REPLICATED, syncMode, false), ORDINAL_START_KEY);
            doTestDeadlocks(createCache(REPLICATED, syncMode, false), CUSTOM_START_KEY);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeadlocksLocal() throws Exception {
        for (CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values()) {
            IgniteCache cache = null;

            try {
                cache = createCache(LOCAL, syncMode, false);

                awaitPartitionMapExchange();

                doTestDeadlock(2, true, true, false, ORDINAL_START_KEY);
                doTestDeadlock(2, true, true, false, CUSTOM_START_KEY);
            }
            finally {
                if (cache != null)
                    cache.destroy();
            }
        }
    }

    /**
     * @param cacheMode Cache mode.
     * @param syncMode Write sync mode.
     * @param near Near.
     * @return Created cache.
     */
    @SuppressWarnings("unchecked")
    private IgniteCache createCache(CacheMode cacheMode, CacheWriteSynchronizationMode syncMode, boolean near)
        throws IgniteInterruptedCheckedException, InterruptedException {
        awaitPartitionMapExchange();

        int minorTopVer = grid(0).context().discovery().topologyVersionEx().minorTopologyVersion();

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setName(CACHE_NAME);
        ccfg.setCacheMode(cacheMode);
        ccfg.setBackups(1);
        ccfg.setNearConfiguration(near ? new NearCacheConfiguration() : null);
        ccfg.setWriteSynchronizationMode(syncMode);

        if (cacheMode == LOCAL)
            ccfg.setDataRegionName("dfltPlc");

        IgniteCache cache = ignite(0).createCache(ccfg);

        if (near) {
            for (int i = 0; i < NODES_CNT; i++) {
                Ignite client = ignite(i + NODES_CNT);

                assertTrue(client.configuration().isClientMode());

                client.createNearCache(ccfg.getName(), new NearCacheConfiguration<>());
            }
        }

        waitForLateAffinityAssignment(minorTopVer);

        return cache;
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestDeadlocks(IgniteCache cache, Object startKey) throws Exception {
        try {
            awaitPartitionMapExchange();

            doTestDeadlock(2, false, true, true, startKey);
            doTestDeadlock(2, false, false, false, startKey);
            doTestDeadlock(2, false, false, true, startKey);

            doTestDeadlock(3, false, true, true, startKey);
            doTestDeadlock(3, false, false, false, startKey);
            doTestDeadlock(3, false, false, true, startKey);

            doTestDeadlock(4, false, true, true, startKey);
            doTestDeadlock(4, false, false, false, startKey);
            doTestDeadlock(4, false, false, true, startKey);
        }
        catch (Exception e) {
            U.error(log, "Unexpected exception: ", e);

            fail();
        }
        finally {
            if (cache != null)
                cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestDeadlock(
        final int txCnt,
        final boolean loc,
        boolean lockPrimaryFirst,
        final boolean clientTx,
        final Object startKey
    ) throws Exception {
        log.info(">>> Test deadlock [txCnt=" + txCnt + ", loc=" + loc + ", lockPrimaryFirst=" + lockPrimaryFirst +
            ", clientTx=" + clientTx + ", startKey=" + startKey.getClass().getName() + ']');

        final AtomicInteger threadCnt = new AtomicInteger();

        final CyclicBarrier barrier = new CyclicBarrier(txCnt);

        final AtomicReference<TransactionDeadlockException> deadlockErr = new AtomicReference<>();

        final List<List<Object>> keySets = generateKeys(txCnt, startKey, loc, !lockPrimaryFirst);

        final Set<Object> involvedKeys = new GridConcurrentHashSet<>();
        final Set<Object> involvedLockedKeys = new GridConcurrentHashSet<>();
        final Set<IgniteInternalTx> involvedTxs = new GridConcurrentHashSet<>();

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                int threadNum = threadCnt.incrementAndGet();

                Ignite ignite = loc ? ignite(0) : ignite(clientTx ? threadNum - 1 + txCnt : threadNum - 1);

                IgniteCache<Object, Integer> cache = ignite.cache(CACHE_NAME).withAllowAtomicOpsInTx();

                List<Object> keys = keySets.get(threadNum - 1);

                int txTimeout = 500 + txCnt * 100;

                try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, txTimeout, 0)) {
                    involvedTxs.add(((TransactionProxyImpl)tx).tx());

                    Object key = keys.get(0);

                    involvedKeys.add(key);

                    Object k;

                    log.info(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode() +
                        ", tx=" + tx + ", key=" + key + ']');

                    cache.put(key, 0);

                    involvedLockedKeys.add(key);

                    barrier.await();

                    key = keys.get(1);

                    ClusterNode primaryNode =
                        ((IgniteCacheProxy)cache).context().affinity().primaryByKey(key, NONE);

                    List<Object> primaryKeys =
                        primaryKeys(grid(primaryNode).cache(CACHE_NAME), 5, incrementKey(key, 100 * threadNum));

                    Map<Object, Integer> entries = new HashMap<>();

                    involvedKeys.add(key);

                    entries.put(key, 0);

                    for (Object o : primaryKeys) {
                        involvedKeys.add(o);

                        entries.put(o, 1);

                        k = incrementKey(o, + 13);

                        involvedKeys.add(k);

                        entries.put(k, 2);
                    }

                    log.info(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode() +
                        ", tx=" + tx + ", entries=" + entries + ']');

                    cache.putAll(entries);

                    tx.commit();
                }
                catch (Throwable e) {
                    // At least one stack trace should contain TransactionDeadlockException.
                    if (hasCause(e, TransactionTimeoutException.class) &&
                        hasCause(e, TransactionDeadlockException.class)
                        ) {
                        if (deadlockErr.compareAndSet(null, cause(e, TransactionDeadlockException.class)))
                            U.error(log, "At least one stack trace should contain " +
                                TransactionDeadlockException.class.getSimpleName(), e);
                    }
                }
            }
        }, loc ? 2 : txCnt, "tx-thread");

        try {
            fut.get();
        }
        catch (IgniteCheckedException e) {
            U.error(null, "Unexpected exception", e);

            fail();
        }

        U.sleep(1000);

        TransactionDeadlockException deadlockE = deadlockErr.get();

        assertNotNull(deadlockE);

        checkAllTransactionsCompleted(involvedKeys, NODES_CNT * 2, CACHE_NAME);

        // Check deadlock report
        String msg = deadlockE.getMessage();

        for (IgniteInternalTx tx : involvedTxs)
            assertTrue(msg.contains(
                "[txId=" + tx.xidVersion() + ", nodeId=" + tx.nodeId() + ", threadId=" + tx.threadId() + ']'));

        for (Object key : involvedKeys) {
            if (involvedLockedKeys.contains(key))
                assertTrue(msg.contains("[key=" + key + ", cache=" + CACHE_NAME + ']'));
            else
                assertFalse(msg.contains("[key=" + key));
        }
    }

    /**
     * @param nodesCnt Nodes count.
     * @param loc Local cache.
     */
    private <T> List<List<T>> generateKeys(int nodesCnt, T startKey, boolean loc, boolean reverse) throws IgniteCheckedException {
        List<List<T>> keySets = new ArrayList<>();

        if (loc) {
            List<T> keys = primaryKeys(ignite(0).cache(CACHE_NAME), 2, startKey);

            keySets.add(new ArrayList<>(keys));

            Collections.reverse(keys);

            keySets.add(keys);
        }
        else {
            for (int i = 0; i < nodesCnt; i++) {
                List<T> keys = new ArrayList<>(2);

                keys.add(primaryKey(ignite(i).cache(CACHE_NAME), startKey));
                keys.add(primaryKey(ignite(i == nodesCnt - 1 ? 0 : i + 1).cache(CACHE_NAME), startKey));

                if (reverse)
                    Collections.reverse(keys);

                keySets.add(keys);
            }
        }

        return keySets;
    }
}
