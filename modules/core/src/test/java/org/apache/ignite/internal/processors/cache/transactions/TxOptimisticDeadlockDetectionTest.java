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
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMap;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionTimeoutException;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion.NONE;
import static org.apache.ignite.internal.util.typedef.X.cause;
import static org.apache.ignite.internal.util.typedef.X.hasCause;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class TxOptimisticDeadlockDetectionTest extends GridCommonAbstractTest {
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

        if (isDebug()) {
            TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

            discoSpi.failureDetectionTimeoutEnabled(false);

            cfg.setDiscoverySpi(discoSpi);
        }

        TcpCommunicationSpi commSpi = new TestCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        cfg.setClientMode(client);

        // Test spi blocks message send, this can cause hang with striped pool.
        cfg.setStripedPoolSize(-1);

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
    public void testDeadlocksPartitionedNear() throws Exception {
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
     * @param cacheMode Cache mode.
     * @param syncMode Write sync mode.
     * @param near Near.
     * @return Created cache.
     */
    @SuppressWarnings("unchecked")
    private IgniteCache createCache(CacheMode cacheMode, CacheWriteSynchronizationMode syncMode, boolean near) {
        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setName(CACHE_NAME);
        ccfg.setCacheMode(cacheMode);
        ccfg.setBackups(1);
        ccfg.setNearConfiguration(near ? new NearCacheConfiguration() : null);
        ccfg.setWriteSynchronizationMode(syncMode);

        IgniteCache cache = ignite(0).createCache(ccfg);

        if (near) {
            for (int i = 0; i < NODES_CNT; i++) {
                Ignite client = ignite(i + NODES_CNT);

                assertTrue(client.configuration().isClientMode());

                client.createNearCache(ccfg.getName(), new NearCacheConfiguration<>());
            }
        }

        return cache;
    }

    /**
     * @param cache Cache.
     * @param transformer Transformer closure.
     * @throws Exception If failed.
     */
    private void doTestDeadlocks(IgniteCache cache, IgniteClosure<Integer, Object> transformer) throws Exception {
        try {
            awaitPartitionMapExchange();

            doTestDeadlock(3, false, true, true, transformer);
            doTestDeadlock(3, false, false, false, transformer);
            doTestDeadlock(3, false, false, true, transformer);

            doTestDeadlock(4, false, true, true, transformer);
            doTestDeadlock(4, false, false, false, transformer);
            doTestDeadlock(4, false, false, true, transformer);
        }
        catch (Throwable e) {
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

        TestCommunicationSpi.init(txCnt);

        final AtomicInteger threadCnt = new AtomicInteger();

        final CyclicBarrier barrier = new CyclicBarrier(txCnt);

        final AtomicReference<TransactionDeadlockException> deadlockErr = new AtomicReference<>();

        final List<List<Integer>> keySets = generateKeys(txCnt, loc, !lockPrimaryFirst);

        final Set<Integer> involvedKeys = new GridConcurrentHashSet<>();
        final Set<Integer> involvedLockedKeys = new GridConcurrentHashSet<>();
        final Set<IgniteInternalTx> involvedTxs = new GridConcurrentHashSet<>();

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                int threadNum = threadCnt.incrementAndGet();

                Ignite ignite = loc ? ignite(0) : ignite(clientTx ? threadNum - 1 + txCnt : threadNum - 1);

                IgniteCache<Object, Integer> cache = ignite.cache(CACHE_NAME);

                List<Integer> keys = keySets.get(threadNum - 1);

                int txTimeout = 500 + txCnt * 100;

                try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, REPEATABLE_READ, txTimeout, 0)) {
                    IgniteInternalTx tx0 = ((TransactionProxyImpl)tx).tx();

                    involvedTxs.add(tx0);

                    Integer key = keys.get(0);

                    involvedKeys.add(key);

                    Object k;

                    log.info(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode().id() +
                        ", tx=" + tx.xid() + ", key=" + transformer.apply(key) + ']');

                    cache.put(transformer.apply(key), 0);

                    involvedLockedKeys.add(key);

                    barrier.await();

                    key = keys.get(1);

                    ClusterNode primaryNode =
                        ((IgniteCacheProxy)cache).context().affinity().primaryByKey(key, NONE);

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

                    log.info(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode().id() +
                        ", tx=" + tx.xid() + ", entries=" + entries + ']');

                    cache.putAll(entries);

                    tx.commit();
                }
                catch (Throwable e) {
                    log.info("Expected exception: " + e);

                    e.printStackTrace(System.out);

                    // At least one stack trace should contain TransactionDeadlockException.
                    if (hasCause(e, TransactionTimeoutException.class) &&
                        hasCause(e, TransactionDeadlockException.class)) {
                        if (deadlockErr.compareAndSet(null, cause(e, TransactionDeadlockException.class))) {
                            log.info("At least one stack trace should contain " +
                                TransactionDeadlockException.class.getSimpleName());

                            e.printStackTrace(System.out);
                        }
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

        assertNotNull("Failed to detect deadlock", deadlockE);

        boolean fail = false;

        // Check transactions, futures and entry locks state.
        for (int i = 0; i < NODES_CNT * 2; i++) {
            Ignite ignite = ignite(i);

            int cacheId = ((IgniteCacheProxy)ignite.cache(CACHE_NAME)).context().cacheId();

            GridCacheSharedContext<Object, Object> cctx = ((IgniteKernal)ignite).context().cache().context();

            IgniteTxManager txMgr = cctx.tm();

            Collection<IgniteInternalTx> activeTxs = txMgr.activeTransactions();

            for (IgniteInternalTx tx : activeTxs) {
                Collection<IgniteTxEntry> entries = tx.allEntries();

                for (IgniteTxEntry entry : entries) {
                    if (entry.cacheId() == cacheId) {
                        fail = true;

                        U.error(log, "Transaction still exists: " + "\n" + tx.xidVersion() +
                            "\n" + tx.nearXidVersion() + "\n nodeId=" + cctx.localNodeId() + "\n tx=" + tx);
                    }
                }
            }

            Collection<IgniteInternalFuture<?>> futs = txMgr.deadlockDetectionFutures();

            assertTrue(futs.isEmpty());

            GridCacheAdapter<Object, Integer> intCache = internalCache(i, CACHE_NAME);

            GridCacheConcurrentMap map = intCache.map();

            for (Integer key : involvedKeys) {
                Object key0 = transformer.apply(key);

                KeyCacheObject keyCacheObj = intCache.context().toCacheKeyObject(key0);

                GridCacheMapEntry entry = map.getEntry(keyCacheObj);

                if (entry != null)
                    assertNull("Entry still has locks " + entry, entry.mvccAllLocal());
            }
        }

        if (fail)
            fail("Some transactions still exist");

        // Check deadlock report
        String msg = deadlockE.getMessage();

        for (IgniteInternalTx tx : involvedTxs)
            assertTrue(msg.contains(
                "[txId=" + tx.xidVersion() + ", nodeId=" + tx.nodeId() + ", threadId=" + tx.threadId() + ']'));

        for (Integer key : involvedKeys) {
            if (involvedLockedKeys.contains(key))
                assertTrue(msg.contains("[key=" + transformer.apply(key) + ", cache=" + CACHE_NAME + ']'));
            else
                assertFalse(msg.contains("[key=" + transformer.apply(key)));
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

                int n1 = i + 1;
                int n2 = n1 + 1;

                int i1 = n1 < nodesCnt ? n1 : n1 - nodesCnt;
                int i2 = n2 < nodesCnt ? n2 : n2 - nodesCnt;

                keys.add(primaryKey(ignite(i1).cache(CACHE_NAME)));
                keys.add(primaryKey(ignite(i2).cache(CACHE_NAME)));

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

            return id == obj.id && name.equals(obj.name);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** Tx count. */
        private static volatile int TX_CNT;

        /** Tx ids. */
        private static final Set<GridCacheVersion> TX_IDS = new GridConcurrentHashSet<>();

        /**
         * @param txCnt Tx count.
         */
        private static void init(int txCnt) {
            TX_CNT = txCnt;
            TX_IDS.clear();
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(
            final ClusterNode node,
            final Message msg,
            final IgniteInClosure<IgniteException> ackC
        ) throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                Message msg0 = ((GridIoMessage)msg).message();

                if (msg0 instanceof GridNearTxPrepareRequest) {
                    final GridNearTxPrepareRequest req = (GridNearTxPrepareRequest)msg0;

                    GridCacheVersion txId = req.version();

                    if (TX_IDS.contains(txId)) {
                        while (TX_IDS.size() < TX_CNT) {
                            try {
                                U.sleep(50);
                            }
                            catch (IgniteInterruptedCheckedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
                else if (msg0 instanceof GridNearTxPrepareResponse) {
                    GridNearTxPrepareResponse res = (GridNearTxPrepareResponse)msg0;

                    GridCacheVersion txId = res.version();

                    TX_IDS.add(txId);
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}
