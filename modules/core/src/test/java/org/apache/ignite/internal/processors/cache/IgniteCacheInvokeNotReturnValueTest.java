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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 *
 */
public class IgniteCacheInvokeNotReturnValueTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Keys count. */
    private static final int KEYS_CNT = 10;

    /** Keys count. */
    private static final int ITERATION_CNT = 100;

    /** Cache name. */
    private static final String CACHE_NAME = "test-cache-name";

    /** Nodes count. */
    private static final int NODES = 3;

    /** */
    private boolean client;

    /** */
    private static volatile boolean fail;

    /** */
    private static boolean readFromBackup = true;

    /** */
    private static UUID clientNodeId;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(getServerNodeCount());

        client = true;

        startGrid(getServerNodeCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        fail = false;
        readFromBackup = true;
        clientNodeId = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpCommunicationSpi commSpi = new DebugCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setCommunicationSpi(commSpi);
        cfg.setClientMode(client);

        return cfg;
    }

    /**
     * @return Count nodes.
     */
    private int getServerNodeCount() {
        return NODES;
    }

    /**
     * @throws Exception If failed.
     */
    public void testOnePhaseTx() throws Exception {
        doTestInvoke(new CacheConfiguration<Integer, Integer>()
            .setName(CACHE_NAME)
            .setBackups(1)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testOnePhaseTxPrimarySync() throws Exception {
        doTestInvoke(new CacheConfiguration<Integer, Integer>()
            .setBackups(1)
            .setName(CACHE_NAME)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testTwoBackup() throws Exception {
        doTestInvoke(new CacheConfiguration<Integer, Integer>()
            .setBackups(2)
            .setName(CACHE_NAME)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testTwoBackupNonAllowReadFromBackup() throws Exception {
        readFromBackup = false;

        doTestInvoke(new CacheConfiguration<Integer, Integer>()
            .setBackups(2)
            .setName(CACHE_NAME)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
            .setReadFromBackup(readFromBackup)
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testTwoBackupChainInvoke() throws Exception {
        doTestInvokeChain(new CacheConfiguration<Integer, Integer>()
            .setName(CACHE_NAME)
            .setBackups(2)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testTwoBackupChainInvokeAll() throws Exception {
        doTestInvokeAllChain(new CacheConfiguration<Integer, Integer>()
            .setName(CACHE_NAME)
            .setBackups(2)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testTwoBackupChainInvokeAllPrimarySync() throws Exception {
        doTestInvokeAllChain(new CacheConfiguration<Integer, Integer>()
            .setName(CACHE_NAME)
            .setBackups(2)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testTwoBackupChainInvokeAllNonAllowReadFromBackup() throws Exception {
        readFromBackup = false;

        doTestInvokeAllChain(new CacheConfiguration<Integer, Integer>()
            .setName(CACHE_NAME)
            .setBackups(2)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
            .setReadFromBackup(readFromBackup)
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testTwoBackupChainInvokeAllPrimarySyncNonAllowReadFromBackup() throws Exception {
        readFromBackup = false;

        doTestInvokeAllChain(new CacheConfiguration<Integer, Integer>()
            .setName(CACHE_NAME)
            .setBackups(2)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
            .setReadFromBackup(readFromBackup)
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testTwoBackupChainPrimarySyncInvoke() throws Exception {
        doTestInvokeChain(new CacheConfiguration<Integer, Integer>()
            .setName(CACHE_NAME)
            .setBackups(2)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testThreeBackup() throws Exception {
        doTestInvoke(new CacheConfiguration<Integer, Integer>()
            .setBackups(3)
            .setName(CACHE_NAME)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testTwoBackupPrimarySync() throws Exception {
        doTestInvoke(new CacheConfiguration<Integer, Integer>()
            .setBackups(2)
            .setName(CACHE_NAME)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testThreeBackupPrimarySync() throws Exception {
        doTestInvoke(new CacheConfiguration<Integer, Integer>()
            .setBackups(3)
            .setName(CACHE_NAME)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
        );
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestInvoke(CacheConfiguration<Integer, Integer> ccfg) throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).createCache(ccfg);

        try {
            invoke(cache, null, false, false);

            invoke(cache, null, true, false);

            invoke(cache, null, false, true);

            invoke(cache, PESSIMISTIC, false, false);

            invoke(cache, PESSIMISTIC, true, false);

            invoke(cache, PESSIMISTIC, false, true);

            invoke(cache, OPTIMISTIC, false, false);

            invoke(cache, OPTIMISTIC, true, false);

            invoke(cache, OPTIMISTIC, false, true);
        }
        catch (Exception e) {
            log.error("Test failed. ", e);

            fail("Test failed.");
        }
        finally {
            for (int i = 0; i <= NODES; i++) {
                Transaction tx = grid(i).transactions().tx();

                if (tx != null)
                    tx.close();
            }

            grid(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestInvokeChain(CacheConfiguration<Integer, Integer> ccfg) throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).createCache(ccfg);

        try {
            invokeChain(cache, null, false, false);

            invokeChain(cache, null, true, false);

            invokeChain(cache, null, false, true);

            invokeChain(cache, PESSIMISTIC, false, false);

            invokeChain(cache, PESSIMISTIC, true, false);

            invokeChain(cache, PESSIMISTIC, false, true);

            invokeChain(cache, OPTIMISTIC, true, false);

            invokeChain(cache, OPTIMISTIC, false, false);

            invokeChain(cache, OPTIMISTIC, false, true);
        }
        finally {
            for (int i = 0; i <= NODES; i++) {
                Transaction tx = grid(i).transactions().tx();

                if (tx != null)
                    tx.close();
            }

            grid(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestInvokeAllChain(CacheConfiguration<Integer, Integer> ccfg) throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).createCache(ccfg);

        try {
            invokeAllChain(cache, null, false);

            invokeAllChain(cache, null, true);

            invokeAllChain(cache, PESSIMISTIC, false);

            invokeAllChain(cache, PESSIMISTIC, true);

            invokeAllChain(cache, OPTIMISTIC, true);

            invokeAllChain(cache, OPTIMISTIC, false);
        }
        finally {
            for (int i = 0; i <= NODES; i++) {
                Transaction tx = grid(i).transactions().tx();

                if (tx != null)
                    tx.close();
            }

            grid(0).destroyCache(ccfg.getName());
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isDebug() {
        return true;
    }

    /**
     * @param cache0 Cache.
     * @param txMode Not null transaction concurrency mode if explicit transaction should be started.
     * @param client Operations will be performed from client node.
     * @throws Exception If failed.
     */
    private void invoke(IgniteCache<Integer, Integer> cache0, @Nullable TransactionConcurrency txMode, boolean client,
        boolean primary) throws Exception {
        IncrementProcessor incProcessor = new IncrementProcessor();

        List<Integer> keys = new ArrayList<>(KEYS_CNT);

        for (int i = 0; i < KEYS_CNT; i++)
            keys.add(i);

        for (final Integer key : keys) {
            log.info("Test invoke [key=" + key + ", txMode=" + txMode + ']');

            Ignite ignite = getAffinityOrClientNode(cache0, client, primary, key);

            IgniteCache<Integer, Integer> cache = ignite.<Integer, Integer>cache(cache0.getName())
                .withSendValueToBackup();

            cache.remove(key);

            Transaction tx = startTx(ignite, txMode);

            DebugCommunicationSpi.maps.clear();

            Integer res = cache.invoke(key, incProcessor);

            if (tx != null) {
                List<Message> msgs = DebugCommunicationSpi.maps.get(GridNearLockResponse.class);

                assertFalse(msgs.isEmpty());
                assertTrue(msgs.size() == 1);

                GridNearLockResponse lockRes = (GridNearLockResponse)msgs.get(0);

                assertNull(lockRes.value(0));

                tx.commit();

                DebugCommunicationSpi.maps.clear();
            }

            assertEquals(-1, (int)res);
            checkValue(key, 1);

            tx = startTx(ignite, txMode);

            res = cache.invoke(key, incProcessor);

            if (tx != null) {
                List<Message> msgs = DebugCommunicationSpi.maps.get(GridNearLockResponse.class);

                assertFalse(msgs.isEmpty());
                assertTrue(msgs.size() == 1);

                GridNearLockResponse lockRes = (GridNearLockResponse)msgs.get(0);

                assertNull(lockRes.value(0));

                tx.commit();

                DebugCommunicationSpi.maps.clear();
            }

            assertEquals(1, (int)res);

            checkValue(key, 2);

            tx = startTx(ignite, txMode);

            res = cache.invoke(key, incProcessor);

            if (tx != null)
                tx.commit();

            assertEquals(2, (int)res);

            checkValue(key, 3);

            tx = startTx(ignite, txMode);

            res = cache.invoke(key, new ArgumentsSumProcessor(), 10, 20, 30);

            if (tx != null)
                tx.commit();

            assertEquals(3, (int)res);

            checkValue(key, 63);

            tx = startTx(ignite, txMode);

            String strRes = cache.invoke(key, new ToStringProcessor());

            if (tx != null)
                tx.commit();

            assertEquals("63", strRes);

            checkValue(key, 63);

            tx = startTx(ignite, txMode);

            TestValue testVal = cache.invoke(key, new UserClassValueProcessor());

            if (tx != null)
                tx.commit();

            assertEquals("63", testVal.value());

            checkValue(key, 63);

            tx = startTx(ignite, txMode);

            Collection<TestValue> testValCol = cache.invoke(key, new CollectionReturnProcessor());

            if (tx != null)
                tx.commit();

            assertEquals(10, testValCol.size());

            for (TestValue val : testValCol)
                assertEquals("64", val.value());

            checkValue(key, 63);

            tx = startTx(ignite, txMode);

            final IgniteCache<Integer, Integer> cache00 = cache;

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    cache00.invoke(key, new ExceptionProcessor(63));

                    return null;
                }
            }, EntryProcessorException.class, "Test processor exception.");

            if (tx != null)
                tx.commit();

            checkValue(key, 63);

            IgniteCache<Integer, Integer> asyncCache = cache.withAsync();

            assertTrue(asyncCache.isAsync());

            assertNull(asyncCache.invoke(key, incProcessor));

            IgniteFuture<Integer> fut = asyncCache.future();

            assertNotNull(fut);

            assertEquals(63, (int)fut.get());

            checkValue(key, 64);

            tx = startTx(ignite, txMode);

            assertNull(cache.invoke(key, new RemoveProcessor(64)));

            if (tx != null)
                tx.commit();

            checkValue(key, null);

            assertFalse("Entry processor was invoked from backup", fail);
        }
    }

    /**
     * @param cache0 Cache.
     * @param txMode Not null transaction concurrency mode if explicit transaction should be started.
     * @param client Operations will be performed from client node.
     * @throws Exception If failed.
     */
    private void invokeChain(IgniteCache<Integer, Integer> cache0, @Nullable TransactionConcurrency txMode,
        boolean client, boolean primary) throws Exception {
        EntryProcessor<Integer, Integer, Integer> incProcessor = new IncrementProcessor();

        Collection<Integer> keys = new ArrayList<>(KEYS_CNT);

        for (int i = 0; i < KEYS_CNT; i++)
            keys.add(i);

        for (final Integer key : keys) {
            Ignite ignite = getAffinityOrClientNode(cache0, client, primary, key);

            IgniteCache<Integer, Integer> cache = ignite.<Integer, Integer>cache(cache0.getName())
                .withSendValueToBackup();

            log.info("Test invoke [key=" + key + ", txMode=" + txMode + ']');

            cache.remove(key);

            Transaction tx = startTx(ignite, txMode);

            Integer res = cache.invoke(key, incProcessor);

            if (txMode != null)
                assertEquals(1, (int)cache.get(key));
            else
                checkValue(key, 1);

            clientNodeId = ignite.cluster().localNode().id();

            Integer res1 = cache.invoke(key, incProcessor);

            if (txMode != null)
                assertEquals(2, (int)cache.get(key));
            else
                checkValue(key, 2);

            Integer res2 = cache.invoke(key, incProcessor);

            if (txMode != null)
                assertEquals(3, (int)cache.get(key));
            else
                checkValue(key, 3);

            if (tx != null)
                tx.commit();

            assertEquals(-1, (int)res);
            assertEquals(1, (int)res1);
            assertEquals(2, (int)res2);

            checkValue(key, 3);

            assertFalse("Entry processor was invoked from backup.", fail);

            clientNodeId = null;
        }

        cache0.removeAll();
    }

    /**
     * @param cache0 Cache.
     * @param client Need client node.
     * @param primary Need primary node.
     * @param key Key.
     * @return Ignite.
     */
    private Ignite getAffinityOrClientNode(IgniteCache<Integer, Integer> cache0, boolean client, boolean primary,
        Integer key) {
        Ignite ignite = null;
        boolean backup = !primary && !client;

        if (client)
            ignite = grid(NODES);
        else {
            for (int i = 0; i < NODES; i++) {
                ClusterNode node = grid(i).cluster().localNode();

                if (primary && affinity(cache0).isPrimary(node, key)) {
                    ignite = grid(i);

                    break;
                }
                else if (backup && affinity(cache0).isBackup(node, key)) {
                    ignite = grid(i);

                    break;
                }
            }
        }

        assert ignite != null;

        return ignite;
    }

    /**
     * @param cache0 Cache.
     * @param txMode Not null transaction concurrency mode if explicit transaction should be started.
     * @param client Operations will be performed from client node.
     * @throws Exception If failed.
     */
    private void invokeAllChain(IgniteCache<Integer, Integer> cache0, @Nullable TransactionConcurrency txMode,
        boolean client) throws Exception {
        EntryProcessor<Integer, Integer, Integer> incProcessor = new IncrementProcessor();

        Set<Integer> keys = new LinkedHashSet<>();

        int keysCnt = KEYS_CNT;

        for (int i = 0; i < NODES; i++) {
            ClusterNode node = grid(i).cluster().localNode();

            for (int key = 0; key < keysCnt; key++) {
                if (affinity(cache0).isPrimary(node, key))
                    keys.add(key);
            }
        }

        keysCnt = keys.size();

        for (int i = 0; i < ITERATION_CNT; i++) {
            Ignite ignite = client ? grid(NODES) : grid(ThreadLocalRandom.current().nextInt(NODES - 1));

            IgniteCache<Integer, Integer> cache = ignite.<Integer, Integer>cache(cache0.getName())
                .withSendValueToBackup();

            int expCntr = 0;

            log.info("Test invoke [keys=" + Arrays.toString(keys.toArray()) + ", txMode=" + txMode + ']');
            log.info("Originating node: " + cache.unwrap(Ignite.class).cluster().localNode());

            cache.removeAll(keys);

            for (Integer key : keys)
                cache.put(key, 0);

            Transaction tx = startTx(ignite, txMode);

            // First
            expCntr += keysCnt;
            Map<Integer, EntryProcessorResult<Integer>> res = cache.invokeAll(keys, incProcessor);

            checkValues(txMode, keys, cache, 1);

            clientNodeId = ignite.cluster().localNode().id();

            // Second
            expCntr += keysCnt;
            Map<Integer, EntryProcessorResult<Integer>> res1 = cache.invokeAll(keys, incProcessor);

            checkValues(txMode, keys, cache, 2);

            // Third
            expCntr += keysCnt;
            Map<Integer, EntryProcessorResult<Integer>> res2 = cache.invokeAll(keys, incProcessor);

            checkValues(txMode, keys, cache, 3);

            if (tx != null)
                tx.commit();

            checkResults(keys, new HashMap<>(res), 0);
            checkResults(keys, new HashMap<>(res1), 1);
            checkResults(keys, new HashMap<>(res2), 2);

            checkValues(txMode, keys, cache, 3, true);

            assertFalse("Entry processor was invoked from backup.", fail);

            clientNodeId = null;

            log.info("Iteration cnt: " + i);
        }

        cache0.removeAll();
    }

    /**
     * @param keys Keys.
     * @param res Invoke results.
     * @param val Expected value.
     */
    private void checkResults(Set<Integer> keys, Map<Integer, EntryProcessorResult<Integer>> res, Integer val) {
        for (Integer key : keys) {
            EntryProcessorResult<Integer> r = res.remove(key);

            assertNotNull(r);
            assertEquals(val, r.get());
        }

        assertTrue(res.isEmpty());
    }

    /**
     * @param txMode Transaction mode.
     * @param keys Keys.
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void checkValues(@Nullable TransactionConcurrency txMode, Set<Integer> keys,
        IgniteCache<Integer, Integer> cache, Integer val) throws Exception {
        checkValues(txMode, keys, cache, val, false);
    }

    /**
     * @param txMode Transaction mode.
     * @param keys Keys.
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void checkValues(@Nullable TransactionConcurrency txMode, Set<Integer> keys,
        IgniteCache<Integer, Integer> cache, Integer val, boolean checkOnAllNodes) throws Exception {
        for (Integer key : keys) {
            if (txMode != null && !checkOnAllNodes)
                assertEquals(val, cache.get(key));
            else
                checkValue(key, val);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAllTwoBackupFullSync() throws Exception {
        doTestInvokeAll(new CacheConfiguration<Integer, Integer>()
            .setBackups(2)
            .setName(CACHE_NAME)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAllTwoBackupPrimarySync() throws Exception {
        doTestInvokeAll(new CacheConfiguration<Integer, Integer>()
            .setBackups(2)
            .setName(CACHE_NAME)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAllTwoBackupFullSyncNonAllowReadFromBackup() throws Exception {
        readFromBackup = false;

        doTestInvokeAll(new CacheConfiguration<Integer, Integer>()
            .setBackups(2)
            .setName(CACHE_NAME)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
            .setReadFromBackup(readFromBackup)
        );
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestInvokeAll(CacheConfiguration<Integer, Integer> ccfg) throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).createCache(ccfg);

        try {
            invokeAll(cache, null);

            invokeAll(cache, PESSIMISTIC);

            invokeAll(cache, OPTIMISTIC);
        }
        finally {
            for (int i = 0; i <= NODES; i++) {
                Transaction tx = grid(i).transactions().tx();

                if (tx != null)
                    tx.close();
            }

            grid(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param cache Cache.
     * @param txMode Not null transaction concurrency mode if explicit transaction should be started.
     * @throws Exception If failed.
     */
    private void invokeAll(IgniteCache<Integer, Integer> cache, @Nullable TransactionConcurrency txMode)
        throws Exception {
        Set<Integer> keys = new LinkedHashSet<>();

        for (int i = 0; i < NODES; i++) {
            ClusterNode node = grid(i).cluster().localNode();

            for (int key = 0; key < KEYS_CNT; key++) {
                if (affinity(cache).isPrimary(node, key))
                    keys.add(key);
            }
        }

        invokeAll(cache, keys, txMode);
    }

    /**
     * @param cache Cache.
     * @param keys Keys.
     * @param txMode Not null transaction concurrency mode if explicit transaction should be started.
     * @throws Exception If failed.
     */
    private void invokeAll(IgniteCache<Integer, Integer> cache, Set<Integer> keys,
        @Nullable TransactionConcurrency txMode) throws Exception {
        cache.removeAll(keys);

        cache = cache.withSendValueToBackup();

        log.info("Test invokeAll [keys=" + keys + ", txMode=" + txMode + ']');

        IncrementProcessor incProcessor = new IncrementProcessor();

        clientNodeId = cache.unwrap(Ignite.class).cluster().localNode().id();

        {
            Transaction tx = startTx(cache.unwrap(Ignite.class), txMode);

            Map<Integer, EntryProcessorResult<Integer>> resMap = cache.invokeAll(keys, incProcessor);

            if (tx != null)
                tx.commit();

            Map<Object, Object> exp = new HashMap<>();

            for (Integer key : keys)
                exp.put(key, -1);

            checkResult(resMap, exp);

            for (Integer key : keys)
                checkValue(key, 1);
        }

        clientNodeId = null;

        assertFalse("Entry processor was invoked from backup", fail);

        {
            Transaction tx = startTx(cache.unwrap(Ignite.class), txMode);

            Map<Integer, EntryProcessorResult<TestValue>> resMap = cache.invokeAll(keys, new UserClassValueProcessor());

            if (tx != null)
                tx.commit();

            Map<Object, Object> exp = new HashMap<>();

            for (Integer key : keys)
                exp.put(key, new TestValue("1"));

            checkResult(resMap, exp);

            for (Integer key : keys)
                checkValue(key, 1);
        }

        assertFalse("Entry processor was invoked from backup", fail);

        {
            Transaction tx = startTx(cache.unwrap(Ignite.class), txMode);

            Map<Integer, EntryProcessorResult<Collection<TestValue>>> resMap =
                cache.invokeAll(keys, new CollectionReturnProcessor());

            if (tx != null)
                tx.commit();

            Map<Object, Object> exp = new HashMap<>();

            for (Integer key : keys) {
                List<TestValue> expCol = new ArrayList<>();

                for (int i = 0; i < 10; i++)
                    expCol.add(new TestValue("2"));

                exp.put(key, expCol);
            }

            checkResult(resMap, exp);

            for (Integer key : keys)
                checkValue(key, 1);
        }

        assertFalse("Entry processor was invoked from backup", fail);

        {
            Transaction tx = startTx(cache.unwrap(Ignite.class), txMode);

            Map<Integer, EntryProcessorResult<Integer>> resMap = cache.invokeAll(keys, incProcessor);

            if (tx != null)
                tx.commit();

            Map<Object, Object> exp = new HashMap<>();

            for (Integer key : keys)
                exp.put(key, 1);

            checkResult(resMap, exp);

            for (Integer key : keys)
                checkValue(key, 2);
        }

        assertFalse("Entry processor was invoked from backup", fail);

        {
            Transaction tx = startTx(cache.unwrap(Ignite.class), txMode);

            Map<Integer, EntryProcessorResult<Integer>> resMap =
                cache.invokeAll(keys, new ArgumentsSumProcessor(), 10, 20, 30);

            if (tx != null)
                tx.commit();

            Map<Object, Object> exp = new HashMap<>();

            for (Integer key : keys)
                exp.put(key, 3);

            checkResult(resMap, exp);

            for (Integer key : keys)
                checkValue(key, 62);
        }

        assertFalse("Entry processor was invoked from backup", fail);

        {
            Transaction tx = startTx(cache.unwrap(Ignite.class), txMode);

            Map<Integer, EntryProcessorResult<Integer>> resMap = cache.invokeAll(keys, new ExceptionProcessor(null));

            if (tx != null)
                tx.commit();

            for (Integer key : keys) {
                final EntryProcessorResult<Integer> res = resMap.get(key);

                assertNotNull("No result for " + key);

                GridTestUtils.assertThrows(log, new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        res.get();

                        return null;
                    }
                }, EntryProcessorException.class, "Test processor exception.");
            }

            for (Integer key : keys)
                checkValue(key, 62);
        }

        assertFalse("Entry processor was invoked from backup", fail);

        {
            Transaction tx = startTx(cache.unwrap(Ignite.class), txMode);

            Map<Integer, EntryProcessor<Integer, Integer, Integer>> invokeMap = new LinkedHashMap<>();

            for (Integer key : keys) {
                switch (key % 4) {
                    case 0: invokeMap.put(key, new IncrementProcessor()); break;

                    case 1: invokeMap.put(key, new RemoveProcessor(62)); break;

                    case 2: invokeMap.put(key, new ArgumentsSumProcessor()); break;

                    case 3: invokeMap.put(key, new ExceptionProcessor(62)); break;

                    default:
                        fail();
                }
            }

            Map<Integer, EntryProcessorResult<Integer>> resMap = cache.invokeAll(invokeMap, 10, 20, 30);

            if (tx != null)
                tx.commit();

            for (Integer key : keys) {
                final EntryProcessorResult<Integer> res = resMap.get(key);

                switch (key % 4) {
                    case 0: {
                        assertNotNull("No result for " + key, res);

                        assertEquals(62, (int)res.get());

                        checkValue(key, 63);

                        break;
                    }

                    case 1: {
                        assertNull(res);

                        checkValue(key, null);

                        break;
                    }

                    case 2: {
                        assertNotNull("No result for " + key, res);

                        assertEquals(3, (int)res.get());

                        checkValue(key, 122);

                        break;
                    }

                    case 3: {
                        assertNotNull("No result for " + key, res);

                        GridTestUtils.assertThrows(log, new Callable<Void>() {
                            @Override public Void call() throws Exception {
                                res.get();

                                return null;
                            }
                        }, EntryProcessorException.class, "Test processor exception.");

                        checkValue(key, 62);

                        break;
                    }
                }
            }
        }

        cache.invokeAll(keys, new IncrementProcessor());

        {
            Transaction tx = startTx(cache.unwrap(Ignite.class), txMode);

            Map<Integer, EntryProcessorResult<Integer>> resMap = cache.invokeAll(keys, new RemoveProcessor(null));

            if (tx != null)
                tx.commit();

            assertEquals("Unexpected results: " + resMap, 0, resMap.size());

            for (Integer key : keys)
                checkValue(key, null);
        }

        IgniteCache<Integer, Integer> asyncCache = cache.withAsync();

        assertTrue(asyncCache.isAsync());

        assertNull(asyncCache.invokeAll(keys, new IncrementProcessor()));

        IgniteFuture<Map<Integer, EntryProcessorResult<Integer>>> fut = asyncCache.future();

        Map<Integer, EntryProcessorResult<Integer>> resMap = fut.get();

        Map<Object, Object> exp = new HashMap<>();

        for (Integer key : keys)
            exp.put(key, -1);

        checkResult(resMap, exp);

        for (Integer key : keys)
            checkValue(key, 1);

        Map<Integer, EntryProcessor<Integer, Integer, Integer>> invokeMap = new HashMap<>();

        for (Integer key : keys)
            invokeMap.put(key, incProcessor);

        assertNull(asyncCache.invokeAll(invokeMap));

        fut = asyncCache.future();

        resMap = fut.get();

        for (Integer key : keys)
            exp.put(key, 1);

        checkResult(resMap, exp);

        for (Integer key : keys)
            checkValue(key, 2);

        assertFalse("Entry processor was invoked from backup", fail);
    }

    /**
     * @param resMap Result map.
     * @param exp Expected results.
     */
    @SuppressWarnings("unchecked")
    private void checkResult(Map resMap, Map<Object, Object> exp) {
        assertNotNull(resMap);

        assertEquals(exp.size(), resMap.size());

        for (Map.Entry<Object, Object> expVal : exp.entrySet()) {
            EntryProcessorResult<?> res = (EntryProcessorResult)resMap.get(expVal.getKey());

            assertNotNull("No result for " + expVal.getKey(), res);

            assertEquals("Unexpected result for " + expVal.getKey(), res.get(), expVal.getValue());
        }
    }

    /**
     * @param key Key.
     * @param expVal Expected value.
     */
    protected void checkValue(final Object key, @Nullable final Object expVal) throws Exception {
        if (expVal != null) {
            for (int i = 0; i < getServerNodeCount(); i++) {
                final IgniteCache<Object, Object> cache = grid(i).cache(CACHE_NAME);

                GridTestUtils.waitForCondition(new PA() {
                    @Override public boolean apply() {
                        Object o = cache.localPeek(key, CachePeekMode.ONHEAP);

                        return o != null && o.equals(expVal);
                    }
                }, 100L);

                Object val = cache.localPeek(key, CachePeekMode.ONHEAP);

                if (val == null)
                    assertFalse(ignite(0).affinity(CACHE_NAME).isPrimaryOrBackup(ignite(i).cluster().localNode(), key));
                else
                    assertEquals("Unexpected value for grid " + i, expVal, val);
            }
        }
        else {
            for (int i = 0; i < getServerNodeCount(); i++) {
                final IgniteCache<Object, Object> cache = grid(i).cache(CACHE_NAME);

                GridTestUtils.waitForCondition(new PA() {
                    @Override public boolean apply() {
                        return cache.localPeek(key, CachePeekMode.ONHEAP) == null;
                    }
                }, 100L);

                assertNull("Unexpected non null value for grid " + i, cache.localPeek(key, CachePeekMode.ONHEAP));
            }
        }
    }

    /**
     * @param txMode Transaction concurrency mode.
     * @return Transaction.
     */
    @Nullable private Transaction startTx(Ignite ignite, @Nullable TransactionConcurrency txMode) {
        return txMode == null ? null : ignite.transactions().txStart(txMode, READ_COMMITTED);
    }

    /**
     *
     */
    private static class ArgumentsSumProcessor extends AbstractEntryProcessor<Integer, Integer, Integer> {
        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> e, Object... args)
            throws EntryProcessorException {
            super.process(e, args);

            assertNotNull(args);
            assertEquals(3, args.length);
            assertEquals(10, args[0]);
            assertEquals(20, args[1]);
            assertEquals(30, args[2]);

            assertTrue(e.exists());

            Integer res = e.getValue();

            for (Object arg : args)
                res += (Integer)arg;

            e.setValue(res);

            return args.length;
        }
    }

    /**
     *
     */
    protected static class ToStringProcessor extends AbstractEntryProcessor<Integer, Integer, String> {
        /** {@inheritDoc} */
        @Override public String process(MutableEntry<Integer, Integer> e, Object... args)
            throws EntryProcessorException {
            super.process(e, args);

            return String.valueOf(e.getValue());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ToStringProcessor.class, this);
        }
    }

    /**
     *
     */
    protected static class UserClassValueProcessor extends AbstractEntryProcessor<Integer, Integer, TestValue> {
        /** {@inheritDoc} */
        @Override public TestValue process(MutableEntry<Integer, Integer> e, Object... args)
            throws EntryProcessorException {
            super.process(e, args);

            return new TestValue(String.valueOf(e.getValue()));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(UserClassValueProcessor.class, this);
        }
    }

    /**
     *
     */
    protected static class CollectionReturnProcessor
        extends AbstractEntryProcessor<Integer, Integer, Collection<TestValue>> {
        /** {@inheritDoc} */
        @Override public Collection<TestValue> process(MutableEntry<Integer, Integer> e, Object... args)
            throws EntryProcessorException {
            super.process(e, args);

            List<TestValue> vals = new ArrayList<>();

            for (int i = 0; i < 10; i++)
                vals.add(new TestValue(String.valueOf(e.getValue() + 1)));

            return vals;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CollectionReturnProcessor.class, this);
        }
    }

    /**
     *
     */
    protected static class IncrementProcessor extends AbstractEntryProcessor<Integer, Integer, Integer> {
        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> e, Object... args)
            throws EntryProcessorException {
            super.process(e, args);
            if (e.exists()) {
                Integer val = e.getValue();

                assertNotNull(val);

                e.setValue(val + 1);

                assertTrue(e.exists());

                assertEquals(val + 1, (int) e.getValue());

                return val;
            }
            else {
                e.setValue(1);

                return -1;
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IncrementProcessor.class, this);
        }
    }

    /**
     *
     */
    private static class RemoveProcessor extends AbstractEntryProcessor<Integer, Integer, Integer> {
        /** */
        private Integer expVal;

        /**
         * @param expVal Expected value.
         */
        RemoveProcessor(@Nullable Integer expVal) {
            this.expVal = expVal;
        }

        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> e,
            Object... args) throws EntryProcessorException {
            super.process(e, args);

            assertTrue(e.exists());

            if (expVal != null)
                assertEquals(expVal, e.getValue());

            e.remove();

            assertFalse(e.exists());

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RemoveProcessor.class, this);
        }
    }

    /**
     *
     */
    private static class ExceptionProcessor extends AbstractEntryProcessor<Integer, Integer, Integer> {
        /** */
        private Integer expVal;

        /**
         * @param expVal Expected value.
         */
        ExceptionProcessor(@Nullable Integer expVal) {
            this.expVal = expVal;
        }

        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> e,
            Object... args) throws EntryProcessorException {
            super.process(e, args);

            assertTrue(e.exists());

            if (expVal != null)
                assertEquals(expVal, e.getValue());

            throw new EntryProcessorException("Test processor exception.");
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ExceptionProcessor.class, this);
        }
    }

    /**
     *
     */
    private static class AbstractEntryProcessor<K, V, T> implements EntryProcessor<K, V, T> {
        /** {@inheritDoc} */
        @Override public T process(MutableEntry<K, V> entry, Object... args) throws EntryProcessorException {
            Ignite ignite = entry.unwrap(Ignite.class);

            boolean isPrimary = ignite.affinity(CACHE_NAME).isPrimary(ignite.cluster().localNode(), entry.getKey());
            boolean isBackup = ignite.affinity(CACHE_NAME).isBackup(ignite.cluster().localNode(), entry.getKey());
            boolean isClient = clientNodeId != null && ignite.cluster().localNode().id().equals(clientNodeId);

            if (!isPrimary && !isClient && !(isBackup && readFromBackup)) {
                ignite.log().error("Failed. Entry processor invoked non-primary node: " + ignite.cluster().localNode());

                fail = true;
            }

            return null;
        }
    }

    /**
     *
     */
    static class TestValue {
        /** */
        private String val;

        /**
         * @param val Value.
         */
        public TestValue(String val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public String value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestValue testVal = (TestValue)o;

            return val.equals(testVal.val);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestValue.class, this);
        }
    }

    /**
     *
     */
    private static class DebugCommunicationSpi extends TcpCommunicationSpi {
        /** */
        public static ConcurrentMap<Class, List<Message>> maps = new ConcurrentHashMap<>();

        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                Message msg0 = ((GridIoMessage)msg).message();

                System.out.println("!!!!!!!: Class==" + msg0.getClass());

                List<Message> msgs = maps.get(msg0.getClass());

                if (msgs == null) {
                    List<Message> msgsOld = maps.putIfAbsent(msg0.getClass(), msgs = new CopyOnWriteArrayList<>());

                    if (msgsOld != null)
                        msgs = msgsOld;
                }

                msgs.add(msg0);
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}