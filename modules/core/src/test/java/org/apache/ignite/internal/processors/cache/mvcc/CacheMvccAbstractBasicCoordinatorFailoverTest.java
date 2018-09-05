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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.TestCacheNodeExcludingFilter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetRequest;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccSnapshotResponse;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Base class for Mvcc coordinator failover test.
 */
public abstract class CacheMvccAbstractBasicCoordinatorFailoverTest extends CacheMvccAbstractTest {
    /**
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    protected void coordinatorFailureSimple(
        final TransactionConcurrency concurrency,
        final TransactionIsolation isolation,
        ReadMode readMode,
        WriteMode writeMode
    ) throws Exception {
        testSpi = true;

        startGrids(3);

        client = true;

        final Ignite client = startGrid(3);

        final IgniteCache cache = client.createCache(
            cacheConfiguration(cacheMode(), FULL_SYNC, 0, DFLT_PARTITION_COUNT)
                .setIndexedTypes(Integer.class, Integer.class));

        final Integer key1 = primaryKey(jcache(1));
        final Integer key2 = primaryKey(jcache(2));

        TestRecordingCommunicationSpi crdSpi = TestRecordingCommunicationSpi.spi(ignite(0));

        crdSpi.blockMessages(MvccSnapshotResponse.class, client.name());

        IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable() {
            @Override public Object call() throws Exception {
                try {
                    try (Transaction tx = client.transactions().txStart(concurrency, isolation)) {
                        writeByMode(cache, key1, 1, writeMode, INTEGER_CODEC);
                        writeByMode(cache, key2, 2, writeMode, INTEGER_CODEC);

                        tx.commit();
                    }

                    fail();
                }
                catch (ClusterTopologyException e) {
                    info("Expected exception: " + e);
                }
                catch (CacheException e) {
                    info("Expected exception: " + e);
                }
                catch (Throwable e) {
                    fail("Unexpected exception: " + e);
                }

                return null;
            }
        }, "tx-thread");

        crdSpi.waitForBlocked();

        stopGrid(0);

        fut.get();

        assertNull(readByMode(cache, key1, readMode, INTEGER_CODEC));
        assertNull(readByMode(cache, key2, readMode, INTEGER_CODEC));

        try (Transaction tx = client.transactions().txStart(concurrency, isolation)) {
            writeByMode(cache, key1, 1, writeMode, INTEGER_CODEC);
            writeByMode(cache, key2, 2, writeMode, INTEGER_CODEC);

            tx.commit();
        }

        assertEquals(1, readByMode(cache, key1, readMode, INTEGER_CODEC));
        assertEquals(2, readByMode(cache, key2, readMode, INTEGER_CODEC));
    }

    /**
     * @param readDelay {@code True} if delays get requests.
     * @param readInTx {@code True} to read inside transaction.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    protected void readInProgressCoordinatorFails(boolean readDelay,
        final boolean readInTx,
        final TransactionConcurrency concurrency,
        final TransactionIsolation isolation,
        ReadMode readMode,
        WriteMode writeMode,
        @Nullable IgniteInClosure<CacheConfiguration> cfgC) throws Exception {
        final int COORD_NODES = 5;
        final int SRV_NODES = 4;

        if (readDelay)
            testSpi = true;

        startGrids(COORD_NODES);

        startGridsMultiThreaded(COORD_NODES, SRV_NODES);

        client = true;

        Ignite client = startGrid(COORD_NODES + SRV_NODES);

        final List<String> cacheNames = new ArrayList<>();

        final int KEYS = 100;

        final Map<Integer, Integer> vals = new HashMap<>();

        for (int i = 0; i < KEYS; i++)
            vals.put(i, 0);

        String[] exclude = new String[COORD_NODES];

        for (int i = 0; i < COORD_NODES; i++)
            exclude[i] = testNodeName(i);

        for (CacheConfiguration ccfg : cacheConfigurations()) {
            ccfg.setName("cache-" + cacheNames.size());

            if (cfgC != null)
                cfgC.apply(ccfg);

            // First server nodes are 'dedicated' coordinators.
            ccfg.setNodeFilter(new TestCacheNodeExcludingFilter(exclude));

            cacheNames.add(ccfg.getName());

            IgniteCache cache = client.createCache(ccfg);

            boolean updated = false;

            while (!updated) {
                try (Transaction tx = client.transactions().txStart(concurrency, isolation)) {
                    tx.timeout(TX_TIMEOUT);

                    writeAllByMode(cache, vals, writeMode, INTEGER_CODEC);

                    tx.commit();

                    updated = true;
                }
                catch (Exception e) {
                    handleTxException(e);
                }
            }
        }

        if (readDelay) {
            for (int i = COORD_NODES; i < COORD_NODES + SRV_NODES + 1; i++) {
                TestRecordingCommunicationSpi.spi(ignite(i)).closure(new IgniteBiInClosure<ClusterNode, Message>() {
                    @Override public void apply(ClusterNode node, Message msg) {
                        if (msg instanceof GridNearGetRequest)
                            doSleep(ThreadLocalRandom.current().nextLong(50) + 1);
                    }
                });
            }
        }

        final AtomicBoolean done = new AtomicBoolean();

        try {
            final AtomicInteger readNodeIdx = new AtomicInteger(0);

            IgniteInternalFuture getFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    try {
                        Ignite node = ignite(COORD_NODES + (readNodeIdx.getAndIncrement() % (SRV_NODES + 1)));

                        int cnt = 0;

                        while (!done.get() && !Thread.currentThread().isInterrupted()) {
                            for (String cacheName : cacheNames) {
                                IgniteCache cache = node.cache(cacheName);

                                Map<Integer, Integer> res = null;

                                if (readInTx) {
                                    try (Transaction tx = node.transactions().txStart(concurrency, isolation)) {
                                        tx.timeout(TX_TIMEOUT);

                                        res = readAllByMode(cache, vals.keySet(), readMode, INTEGER_CODEC);

                                        tx.commit();
                                    }
                                    catch (Exception e) { // TODO Remove catch clause when IGNITE-8841 implemented.
                                        handleTxException(e);
                                    }
                                }
                                else
                                    res = readAllByMode(cache, vals.keySet(), readMode, INTEGER_CODEC);

                                if (readInTx) { // TODO IGNITE-8841
                                    assertTrue("res.size=" + (res == null ? 0 : res.size()) + ", res=" + res, res == null || vals.size() == res.size());
                                }
                                else {
                                    assertEquals(vals.size(), res.size());

                                    Integer val0 = null;

                                    for (Integer val : res.values()) {
                                        if (val0 == null)
                                            val0 = val;
                                        else
                                            assertEquals(val0, val);
                                    }
                                }
                            }

                            cnt++;
                        }

                        log.info("Finished [node=" + node.name() + ", readCnt=" + cnt + ']');

                        return null;
                    }
                    catch (Throwable e) {
                        error("Unexpected error: " + e, e);

                        throw e;
                    }
                }
            }, ((SRV_NODES + 1) + 1) * 2, "get-thread");

            IgniteInternalFuture putFut1 = GridTestUtils.runAsync(new Callable() {
                @Override public Void call() throws Exception {
                    Ignite node = ignite(COORD_NODES);

                    List<IgniteCache> caches = new ArrayList<>();

                    for (String cacheName : cacheNames)
                        caches.add(node.cache(cacheName));

                    Integer val = 1;

                    while (!done.get()) {
                        Map<Integer, Integer> vals = new HashMap<>();

                        for (int i = 0; i < KEYS; i++)
                            vals.put(i, val);

                        for (IgniteCache cache : caches) {
                            try {
                                try (Transaction tx = node.transactions().txStart(concurrency, isolation)) {
                                    tx.timeout(TX_TIMEOUT);

                                    writeAllByMode(cache, vals, writeMode, INTEGER_CODEC);

                                    tx.commit();
                                }
                            }
                            catch (Exception e) {
                                handleTxException(e);
                            }
                        }

                        val++;
                    }

                    return null;
                }
            }, "putAll-thread");

            IgniteInternalFuture putFut2 = GridTestUtils.runAsync(new Callable() {
                @Override public Void call() throws Exception {
                    Ignite node = ignite(COORD_NODES);

                    IgniteCache cache = node.cache(cacheNames.get(0));

                    Integer val = 0;

                    while (!done.get()) {
                        try {
                            try (Transaction tx = node.transactions().txStart(concurrency, isolation)) {
                                tx.timeout(TX_TIMEOUT);

                                writeByMode(cache, Integer.MAX_VALUE, val, writeMode, INTEGER_CODEC);

                                tx.commit();
                            }
                        }
                        catch (Exception e) {
                            handleTxException(e);
                        }

                        val++;
                    }

                    return null;
                }
            }, "put-thread");

            for (int i = 0; i < COORD_NODES && !getFut.isDone(); i++) {
                U.sleep(3000);

                stopGrid(i);

                awaitPartitionMapExchange();
            }

            done.set(true);

            getFut.get();
            putFut1.get();
            putFut2.get();

            for (Ignite node : G.allGrids())
                checkActiveQueriesCleanup(node);
        }
        finally {
            done.set(true);
        }
    }

    /**
     * @param concurrency Tx concurrency level.
     * @param isolation Tx isolation level.
     * @param cfgC Cache cfg closure.
     * @param readMode Read mode.
     * @param writeMode Write mode.
     * @throws Exception  If failed.
     */
    protected void txInProgressCoordinatorChangeSimple(
        final TransactionConcurrency concurrency,
        final TransactionIsolation isolation,
        @Nullable IgniteInClosure<CacheConfiguration> cfgC,
        ReadMode readMode,
        WriteMode writeMode) throws Exception {
        MvccProcessorImpl.coordinatorAssignClosure(new CoordinatorAssignClosure());

        Ignite srv0 = startGrids(4);

        client = true;

        startGrid(4);

        client = false;

        nodeAttr = CRD_ATTR;

        int crdIdx = 5;

        startGrid(crdIdx);

        CacheConfiguration ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 0, DFLT_PARTITION_COUNT).
            setNodeFilter(new CoordinatorNodeFilter());

        if (cfgC != null)
            cfgC.apply(ccfg);

        srv0.createCache(ccfg);

        Set<Integer> keys = F.asSet(1, 2, 3);

        for (int i = 0; i < 5; i++) {
            Ignite node = ignite(i);

            info("Test with node: " + node.name());

            IgniteCache cache = node.cache(DEFAULT_CACHE_NAME);

            try (Transaction tx = node.transactions().txStart(concurrency, isolation)) {
                assertTrue(readAllByMode(cache, keys, readMode, INTEGER_CODEC).isEmpty());

                startGrid(crdIdx + 1);

                stopGrid(crdIdx);

                crdIdx++;

                tx.commit();
            }
            catch (Exception e) {
                handleTxException(e);
            }

            checkActiveQueriesCleanup(ignite(crdIdx));
        }
    }

    /**
     * @param fromClient {@code True} if read from client node, otherwise from server node.
     * @throws Exception If failed.
     */
    protected void readInProgressCoordinatorFailsSimple(boolean fromClient,
        @Nullable IgniteInClosure<CacheConfiguration> cfgC,
        ReadMode readMode,
        WriteMode writeMode) throws Exception {
        for (boolean readInTx : new boolean[]{false, true}) {
            for (int i = 1; i <= 3; i++) {
                readInProgressCoordinatorFailsSimple(fromClient, i, readInTx,cfgC, readMode, writeMode);

                afterTest();
            }
        }
    }

    /**
     * @param fromClient {@code True} if read from client node, otherwise from server node.
     * @param crdChangeCnt Number of coordinator changes.
     * @param readInTx {@code True} to read inside transaction.
     * @param cfgC Cache configuration closure.
     * @param readMode Read mode.
     * @param writeMode Write mode.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void readInProgressCoordinatorFailsSimple(boolean fromClient,
        int crdChangeCnt,
        final boolean readInTx,
        @Nullable IgniteInClosure<CacheConfiguration> cfgC,
        ReadMode readMode,
        WriteMode writeMode) throws Exception {
        info("readInProgressCoordinatorFailsSimple [fromClient=" + fromClient +
            ", crdChangeCnt=" + crdChangeCnt +
            ", readInTx=" + readInTx + ']');

        TransactionConcurrency concurrency = readMode == ReadMode.GET ? OPTIMISTIC : PESSIMISTIC; // TODO IGNITE-7184
        TransactionIsolation isolation = readMode == ReadMode.GET ? SERIALIZABLE : REPEATABLE_READ; // TODO IGNITE-7184

        testSpi = true;

        client = false;

        final int SRVS = 3;
        final int COORDS = crdChangeCnt + 1;

        startGrids(SRVS + COORDS);

        client = true;

        assertTrue(startGrid(SRVS + COORDS).configuration().isClientMode());

        final Ignite getNode = fromClient ? ignite(SRVS + COORDS) : ignite(COORDS);

        String[] excludeNodes = new String[COORDS];

        for (int i = 0; i < COORDS; i++)
            excludeNodes[i] = testNodeName(i);

        CacheConfiguration ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 0, DFLT_PARTITION_COUNT).
            setNodeFilter(new TestCacheNodeExcludingFilter(excludeNodes));

        if (cfgC != null)
            cfgC.apply(ccfg);

        final IgniteCache cache = getNode.createCache(ccfg);

        final Set<Integer> keys = new HashSet<>();

        List<Integer> keys1 = primaryKeys(jcache(COORDS), 10);
        List<Integer> keys2 = primaryKeys(jcache(COORDS + 1), 10);

        keys.addAll(keys1);
        keys.addAll(keys2);

        Map<Integer, Integer> vals = new HashMap();

        for (Integer key : keys)
            vals.put(key, -1);

        try (Transaction tx = getNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            writeAllByMode(cache, vals, writeMode, INTEGER_CODEC);

            tx.commit();
        }

        final TestRecordingCommunicationSpi getNodeSpi = TestRecordingCommunicationSpi.spi(getNode);

        getNodeSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                String msgClsName = msg.getClass().getSimpleName();

                return msgClsName.matches("GridNearGetRequest|GridH2QueryRequest|GridCacheQueryRequest");
            }
        });

        IgniteInternalFuture getFut = GridTestUtils.runAsync(new Callable() {
            @Override public Object call() throws Exception {
                Map<Integer, Integer> res = null;

                if (readInTx) {
                    try (Transaction tx = getNode.transactions().txStart(concurrency, isolation)) {
                        res = readAllByMode(cache, keys, readMode, INTEGER_CODEC);

                        tx.rollback();
                    }
                    catch (Exception e) {
                        handleTxException(e);
                    }
                }
                else
                    res = readAllByMode(cache, keys, readMode, INTEGER_CODEC);

                assertTrue((res != null || readInTx) || (res != null && 20 == res.size()));

                if (res != null) {
                    Integer val = null;

                    for (Integer val0 : res.values()) {
                        assertNotNull(val0);

                        if (val == null)
                            val = val0;
                        else
                            assertEquals("res=" + res, val, val0);
                    }
                }

                return null;
            }
        }, "get-thread");

        getNodeSpi.waitForBlocked();

        for (int i = 0; i < crdChangeCnt; i++)
            stopGrid(i);

        for (int i = 0; i < 10; i++) {
            vals = new HashMap();

            for (Integer key : keys)
                vals.put(key, i);

            while (true) {
                try (Transaction tx = getNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    writeAllByMode(cache, vals, writeMode, INTEGER_CODEC);

                    tx.commit();

                    break;
                }
                catch (Exception e) {
                    handleTxException(e);
                }
            }
        }

        getNodeSpi.stopBlock(true);

        getFut.get();

        for (Ignite node : G.allGrids())
            checkActiveQueriesCleanup(node);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    protected void checkCoordinatorChangeActiveQueryClientFails_Simple(@Nullable IgniteInClosure<CacheConfiguration> cfgC,
        ReadMode readMode,
        WriteMode writeMode) throws Exception {
        testSpi = true;

        client = false;

        final int SRVS = 3;
        final int COORDS = 1;

        startGrids(SRVS + COORDS);

        client = true;

        Ignite client = startGrid(SRVS + COORDS);

        CacheConfiguration ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 0, DFLT_PARTITION_COUNT).
            setNodeFilter(new TestCacheNodeExcludingFilter(testNodeName(0)));

        if (cfgC != null)
            cfgC.apply(ccfg);

        final IgniteCache cache = client.createCache(ccfg);

        final Map<Integer, Integer> vals = new HashMap();

        for (int i = 0; i < 100; i++)
            vals.put(i, i);

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            writeAllByMode(cache, vals, writeMode, INTEGER_CODEC);

            tx.commit();
        }

        final TestRecordingCommunicationSpi clientSpi = TestRecordingCommunicationSpi.spi(client);

        clientSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                String msgClsName = msg.getClass().getSimpleName();

                return msgClsName.matches("GridNearGetRequest|GridH2QueryRequest|GridCacheQueryRequest");
            }
        });

        IgniteInternalFuture getFut = GridTestUtils.runAsync(new Callable() {
            @Override public Object call() throws Exception {
                Map res = readAllByMode(cache, vals.keySet(), readMode, INTEGER_CODEC);

                assertEquals(vals, res);

                return null;
            }
        }, "get-thread");

        clientSpi.waitForBlocked();

        stopGrid(0);

        clientSpi.stopBlock(true);

        getFut.get();

        for (Ignite node : G.allGrids())
            checkActiveQueriesCleanup(node);
    }
}
