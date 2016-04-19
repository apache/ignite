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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionDeadlockException;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion.NONE;
import static org.apache.ignite.internal.util.typedef.X.hasCause;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests deadlock detection for pessimistic transactions.
 */
public class TxPessimisticDeadlockDetectionTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Nodes count (actually two times more nodes will started: server + client). */
    private static final int NODES_CNT = 4;

    /** No op transformer. */
    private static final NoOpTransformer NO_OP_TRANSFORMER = new NoOpTransformer();

    /** Wrapping transformer. */
    private static final WrappingTransformer WRAPPING_TRANSFORMER = new WrappingTransformer();

    /** Client mode flag. */
    private static boolean client;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(new BinaryMarshaller());

        if (isDebug()) {
            TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

            discoSpi.failureDetectionTimeoutEnabled(false);

            cfg.setDiscoverySpi(discoSpi);
        }

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        client = false;

        startGrids(NODES_CNT);

        client = true;

        for (int i = 0; i < NODES_CNT; i++)
            startGrid(i + NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeadlocksPartitioned() throws Exception {
        for (CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values()) {
            doTestDeadlocks(createCache(PARTITIONED, syncMode, false), NO_OP_TRANSFORMER);
            doTestDeadlocks(createCache(PARTITIONED, syncMode, false), WRAPPING_TRANSFORMER);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void _testDeadlocksPartitionedNear() throws Exception {
        for (CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values()) {
            doTestDeadlocks(createCache(PARTITIONED, syncMode, true), NO_OP_TRANSFORMER);
            doTestDeadlocks(createCache(PARTITIONED, syncMode, true), WRAPPING_TRANSFORMER);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeadlocksReplicated() throws Exception {
        for (CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values()) {
            doTestDeadlocks(createCache(REPLICATED, syncMode, false), NO_OP_TRANSFORMER);
            doTestDeadlocks(createCache(REPLICATED, syncMode, false), WRAPPING_TRANSFORMER);
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

                doTestDeadlock(2, true, true, false, NO_OP_TRANSFORMER);
                doTestDeadlock(2, true, true, false, WRAPPING_TRANSFORMER);
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
     */
    @SuppressWarnings("unchecked")
    private IgniteCache createCache(CacheMode cacheMode, CacheWriteSynchronizationMode syncMode, boolean near) {
        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setName(CACHE_NAME);
        ccfg.setCacheMode(cacheMode);
        ccfg.setBackups(1);
        ccfg.setNearConfiguration(near ? new NearCacheConfiguration() : null);
        ccfg.setWriteSynchronizationMode(syncMode);

        return ignite(0).getOrCreateCache(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestDeadlocks(IgniteCache cache, IgniteClosure<Integer, Object> transformer) throws Exception {
        try {
            awaitPartitionMapExchange();

            doTestDeadlock(2, false, true, true, transformer);
            doTestDeadlock(2, false, false, false, transformer);
            doTestDeadlock(2, false, false, true, transformer);

            doTestDeadlock(3, false, true, true, transformer);
            doTestDeadlock(3, false, false, false, transformer);
            doTestDeadlock(3, false, false, true, transformer);

            doTestDeadlock(4, false, true, true, transformer);
            doTestDeadlock(4, false, false, false, transformer);
            doTestDeadlock(4, false, false, true, transformer);
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
        final IgniteClosure<Integer, Object> transformer
    ) throws Exception {
        log.info(">>> Test deadlock [txCnt=" + txCnt + ", loc=" + loc + ", lockPrimaryFirst=" + lockPrimaryFirst +
            ", clientTx=" + clientTx + ", transformer=" + transformer.getClass().getName() + ']');

        final AtomicInteger threadCnt = new AtomicInteger();

        final CountDownLatch latch = new CountDownLatch(txCnt);
        final CountDownLatch startLatch = new CountDownLatch(txCnt);

        final AtomicBoolean deadlock = new AtomicBoolean();

        final List<List<Integer>> keySets = generateKeys(txCnt, loc, !lockPrimaryFirst);

        final Set<Integer> involvedKeys = new GridConcurrentHashSet<>();

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                int threadNum = threadCnt.incrementAndGet();

                Ignite ignite = loc ? ignite(0) : ignite(clientTx ? threadNum - 1 + txCnt : threadNum - 1);

                IgniteCache<Object, Integer> cache = ignite.cache(CACHE_NAME);

                List<Integer> keys = keySets.get(threadNum - 1);

                startLatch.countDown();

                try {
                    startLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }

                try (Transaction tx =
                         ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 500 + txCnt * 100, 0)
                ) {
                    Integer key = keys.get(0);

                    involvedKeys.add(key);

                    Object k;

                    log.info(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode() +
                        ", tx=" + tx + ", key=" + transformer.apply(key) + ']');

                    cache.put(transformer.apply(key), 0);

                    latch.countDown();

                    latch.await();

                    key = keys.get(1);

                    ClusterNode primaryNode =
                        ((IgniteCacheProxy)cache).context().affinity().primary(key, NONE);

                    List<Integer> primaryKeys =
                        primaryKeys(grid(primaryNode).cache(CACHE_NAME), 5, key + (100 * threadNum));

                    Map<Object, Integer> entries = new HashMap<>();

                    involvedKeys.add(key);

                    entries.put(transformer.apply(key), 0);

                    for (Integer i : primaryKeys) {
                        involvedKeys.add(i);

                        entries.put(transformer.apply(i), 1);

                        k = transformer.apply(i + 13);

                        involvedKeys.add(i + 13);

                        entries.put(k, 2);
                    }

                    log.info(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode() +
                        ", tx=" + tx + ", entries=" + entries + ']');

                    cache.putAll(entries);

                    tx.commit();
                }
                catch (Throwable e) {
                    // At least one stack trace should contain TransactionDeadlockException.
                    if (hasCause(e, IgniteTxTimeoutCheckedException.class) &&
                        hasCause(e, TransactionDeadlockException.class)
                        ) {
                        if (deadlock.compareAndSet(false, true))
                            U.error(log, "At least one stack trace should contain " +
                                TransactionDeadlockException.class.getSimpleName(), e);
                    }
                }
            }
        }, loc ? 2 : txCnt, "tx-thread");

        fut.get();

        assertTrue(deadlock.get());

        U.sleep(1000);

        for (int i = 0; i < NODES_CNT * 2; i++) {
            Ignite ignite = ignite(i);

            int cacheId = ((IgniteCacheProxy)ignite.cache(CACHE_NAME)).context().cacheId();

            Collection<IgniteInternalTx> activeTxs =
                ((IgniteKernal)ignite).context().cache().context().tm().activeTransactions();

            for (IgniteInternalTx tx : activeTxs) {
                Collection<IgniteTxEntry> entries = tx.allEntries();

                for (IgniteTxEntry entry : entries) {
                    if (entry.cacheId() == cacheId)
                        fail("Transaction still exists: " + tx);
                }
            }

            GridCacheAdapter<Object, Integer> intCache = internalCache(i, CACHE_NAME);

            for (Integer key : involvedKeys) {
                CacheEntry<Object, Integer> entry = intCache.getEntry(transformer.apply(key));

                if (entry != null) {
                    GridCacheMapEntry e = (GridCacheMapEntry)entry;

                    assertNull(e.mvccAllLocal());
                }
            }
        }
    }

    /**
     * @param nodesCnt Nodes count.
     * @param loc Local cache.
     */
    private List<List<Integer>> generateKeys(int nodesCnt, boolean loc, boolean reverse) throws IgniteCheckedException {
        List<List<Integer>> keySets = new ArrayList<>();

        if (loc) {
            List<Integer> keys = primaryKeys(ignite(0).cache(CACHE_NAME), 2);

            keySets.add(new ArrayList<>(keys));

            Collections.reverse(keys);

            keySets.add(keys);
        }
        else {
            for (int i = 0; i < nodesCnt; i++) {
                List<Integer> keys = new ArrayList<>(2);

                keys.add(primaryKey(ignite(i).cache(CACHE_NAME)));
                keys.add(primaryKey(ignite(i == nodesCnt - 1 ? 0 : i + 1).cache(CACHE_NAME)));

                if (reverse)
                    Collections.reverse(keys);

                keySets.add(keys);
            }
        }

        return keySets;
    }

    /**
     *
     */
    private static class NoOpTransformer implements IgniteClosure<Integer, Object> {
        /** {@inheritDoc} */
        @Override public Object apply(Integer val) {
            return val;
        }
    }

    /**
     *
     */
    private static class WrappingTransformer implements IgniteClosure<Integer, Object> {
        /** {@inheritDoc} */
        @Override public Object apply(Integer val) {
            return new KeyObject(val);
        }
    }

    /**
     *
     */
    private static class KeyObject implements Serializable {
        /** Id. */
        private int id;

        /** Name. */
        private String name;

        /**
         * @param id Id.
         */
        public KeyObject(int id) {
            this.id = id;
            this.name = "KeyObject" + id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "KeyObject{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            KeyObject obj = (KeyObject)o;

            if (id != obj.id)
                return false;

            return name.equals(obj.name);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }
}