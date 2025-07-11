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

package org.apache.ignite.cache.store;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.cache.CacheInterceptorAdapter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Tests to check scenarios with system failures during transaction commit. Internal system failures are modelled with
 * {@link CacheInterceptor} custom implementation throwing an exception during final commit phase.
 */
public class CacheStoreWithIgniteTxFailureTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int GRID_COUNT = 3;

    /** */
    private static final int KEYS_NUMBER = 50;

    /** */
    private static final int FAILING_NODE_IDX = 1;

    /** */
    private static final IntFunction<Integer> KEY_UPDATE_FUNCTION = key -> key + KEYS_NUMBER * 3;

    /** */
    private volatile FailureHandler failureHandler;

    /** */
    private volatile CacheMode cacheMode;

    /** */
    private volatile boolean txCoordinator;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        initStoreStrategy();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        storeStgy.resetStore();
        failureHandler = null;
        cacheMode = null;
        txCoordinator = false;
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return failureHandler != null ? failureHandler : super.getFailureHandler(igniteInstanceName);
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_COUNT;
    }

    /** {@inheritDoc} */
    @Override protected int backups() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return cacheMode != null ? cacheMode : super.cacheMode();
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(igniteInstanceName);

        ccfg.setInterceptor(new FailingCacheInterceptor(igniteInstanceName, FAILING_NODE_IDX, txCoordinator));

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptionOnBackupAfterCommitToStoreWithFailureHandler() throws Exception {
        failureHandler = new StopNodeFailureHandler();

        IgniteEx ig = startGrids(gridCount());
        IgniteCache<Integer, Integer> cache = ig.cache(DEFAULT_CACHE_NAME);

        fillCache(cache, KEYS_NUMBER);

        List<Integer> keysOnBackup = findKeys(grid(FAILING_NODE_IDX).localNode(), cache, 5, 0, 1);

        updateKeysInTx(startClientGrid(), keysOnBackup, KEY_UPDATE_FUNCTION);

        waitForTopology(3);

        checkKeysOnUnaffectedNodes(keysOnBackup);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptionOnBackupAfterCommitToStore() throws Exception {
        IgniteEx ig = startGrids(gridCount());
        IgniteCache<Integer, Integer> cache = ig.cache(DEFAULT_CACHE_NAME);

        fillCache(cache, KEYS_NUMBER);

        List<Integer> keysOnBackup = findKeys(grid(FAILING_NODE_IDX).localNode(), cache, 5, 0, 1);

        updateKeysInTx(startClientGrid(), keysOnBackup, KEY_UPDATE_FUNCTION);

        checkKeysOnAffectedNode(keysOnBackup);
        checkKeysOnUnaffectedNodes(keysOnBackup);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptionOnBackupAfterCommitToStoreOnReplicatedCache() throws Exception {
        cacheMode = CacheMode.REPLICATED;

        IgniteEx ig = startGrids(gridCount());
        IgniteCache<Integer, Integer> cache = ig.cache(DEFAULT_CACHE_NAME);

        fillCache(cache, KEYS_NUMBER);

        List<Integer> keysOnBackup = findKeys(grid(FAILING_NODE_IDX).localNode(), cache, 5, 0, 1);

        updateKeysInTx(startClientGrid(), keysOnBackup, KEY_UPDATE_FUNCTION);

        checkKeysOnAffectedNode(keysOnBackup);
        checkKeysOnUnaffectedNodes(keysOnBackup);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptionOnBackupAfterCommitToStoreOnReplicatedCacheWithFailureHandler() throws Exception {
        cacheMode = CacheMode.REPLICATED;
        failureHandler = new StopNodeFailureHandler();

        IgniteEx ig = startGrids(gridCount());
        IgniteCache<Integer, Integer> cache = ig.cache(DEFAULT_CACHE_NAME);

        fillCache(cache, KEYS_NUMBER);

        List<Integer> keysOnBackup = findKeys(grid(FAILING_NODE_IDX).localNode(), cache, 5, 0, 1);

        updateKeysInTx(startClientGrid(), keysOnBackup, KEY_UPDATE_FUNCTION);

        waitForTopology(3);

        checkKeysOnUnaffectedNodes(keysOnBackup);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptionOnPrimaryAfterCommitToStore() throws Exception {
        IgniteEx ig = startGrids(gridCount());
        IgniteCache<Integer, Integer> cache = ig.cache(DEFAULT_CACHE_NAME);

        fillCache(cache, KEYS_NUMBER);

        List<Integer> keysOnPrimary = findKeys(grid(FAILING_NODE_IDX).localNode(), cache, 5, 0, 0);

        IgniteEx clientNode = startClientGrid();

        try {
            updateKeysInTx(clientNode, keysOnPrimary, KEY_UPDATE_FUNCTION);
        }
        catch (Exception ignored) {
            // Ignored
        }

        checkKeysOnAffectedNode(keysOnPrimary);
        checkKeysOnUnaffectedNodes(keysOnPrimary);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptionOnPrimaryAfterCommitToStoreWithFailureHandler() throws Exception {
        failureHandler = new StopNodeFailureHandler();

        IgniteEx ig = startGrids(gridCount());
        IgniteCache<Integer, Integer> cache = ig.cache(DEFAULT_CACHE_NAME);

        fillCache(cache, KEYS_NUMBER);

        List<Integer> keysOnPrimary = findKeys(grid(FAILING_NODE_IDX).localNode(), cache, 5, 0, 0);

        try {
            updateKeysInTx(startClientGrid(), keysOnPrimary, KEY_UPDATE_FUNCTION);
        }
        catch (Exception ignored) {
            // Ignored
        }

        waitForTopology(3);
        checkKeysOnUnaffectedNodes(keysOnPrimary);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptionOnPrimaryTxCoordinatorAfterCommitToStore() throws Exception {
        txCoordinator = true;

        IgniteEx ig = startGrids(gridCount());
        IgniteCache<Integer, Integer> cache = ig.cache(DEFAULT_CACHE_NAME);

        fillCache(cache, KEYS_NUMBER);

        List<Integer> keysOnPrimary = findKeys(grid(FAILING_NODE_IDX).localNode(), cache, 5, 0, 0);

        try {
            updateKeysInTx(grid(FAILING_NODE_IDX), keysOnPrimary, KEY_UPDATE_FUNCTION);
        }
        catch (Exception ignored) {
            // Ignored
        }

        checkKeysOnAffectedNode(keysOnPrimary); // This should pass as values should be thrown away when tx commit fails
        checkKeysOnUnaffectedNodes(keysOnPrimary);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptionOnBackupTxCoordinatorAfterCommitToStoreWithFailureHandler() throws Exception {
        failureHandler = new StopNodeFailureHandler();
        txCoordinator = true;

        IgniteEx ig = startGrids(gridCount());
        IgniteCache<Integer, Integer> cache = ig.cache(DEFAULT_CACHE_NAME);

        fillCache(cache, KEYS_NUMBER);

        List<Integer> keysOnBackup = findKeys(grid(FAILING_NODE_IDX).localNode(), cache, 5, 0, 1);

        try {
            updateKeysInTx(grid(FAILING_NODE_IDX), keysOnBackup, KEY_UPDATE_FUNCTION);
        }
        catch (Exception ignored) {
            // Ignored
        }

        waitForTopology(2); // this should pass as tx coordinator node should fail after error on tx commit
        checkKeysOnUnaffectedNodes(keysOnBackup);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptionOnPrimaryTxCoordinatorAfterCommitToStoreWithFailureHandler() throws Exception {
        txCoordinator = true;
        failureHandler = new StopNodeFailureHandler();

        IgniteEx ig = startGrids(gridCount());
        IgniteCache<Integer, Integer> cache = ig.cache(DEFAULT_CACHE_NAME);

        fillCache(cache, KEYS_NUMBER);

        List<Integer> keysOnPrimary = findKeys(grid(FAILING_NODE_IDX).localNode(), cache, 5, 0, 0);

        try {
            updateKeysInTx(grid(FAILING_NODE_IDX), keysOnPrimary, KEY_UPDATE_FUNCTION);
        }
        catch (Exception ignored) {
            // Ignored
        }

        checkKeysOnAffectedNode(keysOnPrimary);
        checkKeysOnUnaffectedNodes(keysOnPrimary);
    }

    /** */
    private void checkKeysOnAffectedNode(List<Integer> keysToCheck) {
        IgniteCache<Object, Object> cache = grid(FAILING_NODE_IDX).cache(DEFAULT_CACHE_NAME);

        for (Integer key : keysToCheck) {
            assertEquals(storeStgy.getFromStore(key), cache.get(key));
        }
    }

    /** */
    private void checkKeysOnUnaffectedNodes(List<Integer> keysToCheck) {
        for (int i = 0; i < gridCount(); i++) {
            if (i != FAILING_NODE_IDX) {
                IgniteCache<Object, Object> cache = grid(i).cache(DEFAULT_CACHE_NAME);

                for (Integer key : keysToCheck) {
                    Object val = cache.get(key);
                    assertEquals(storeStgy.getFromStore(key), val);
                }
            }
        }
    }

    /** */
    private void fillCache(IgniteCache<Integer, Integer> cache, int numOfKeys) {
        for (int i = 0; i < numOfKeys; i++) {
            cache.put(i, i);
        }
    }

    /** */
    private void updateKeysInTx(Ignite ig, List<Integer> keys, IntFunction<Integer> keyUpdateFunction) {
        IgniteCache<Object, Object> cache = ig.cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = ig.transactions().txStart()) {
            for (Integer key : keys) {
                cache.put(key, keyUpdateFunction.apply(key));
            }

            tx.commit();
        }
    }

    /** */
    private static class FailingCacheInterceptor extends CacheInterceptorAdapter<Integer, Integer> {
        /** */
        private final String instanceName;

        /** */
        private final String targetNodeSuffix;

        /** */
        private final boolean txCoordinator;

        /** */
        private final Map<Integer, AtomicInteger> map = new ConcurrentHashMap<>();

        /**
         * @param instanceName Ignite node instance name.
         * @param nodeIdx Ignite node index.
         * @param txCoordinator Flag if node is tx coordinator.
         */
        private FailingCacheInterceptor(String instanceName, int nodeIdx, boolean txCoordinator) {
            this.instanceName = instanceName;
            targetNodeSuffix = "Test" + nodeIdx;
            this.txCoordinator = txCoordinator;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Integer onBeforePut(Cache.Entry<Integer, Integer> entry, Integer newVal) {
            if (newVal < 2 * KEYS_NUMBER)
                return newVal;

            if (instanceName.contains(targetNodeSuffix)) {
                if (txCoordinator) {
                    if (!map.containsKey(newVal))
                        map.put(newVal, new AtomicInteger(0));
                    else
                        map.get(newVal).incrementAndGet();

                    if (map.get(newVal).get() == 1)
                        throw new IgniteException("IgniteException from onBeforePut on tx coordinator: " + instanceName);
                }
                else
                    throw new IgniteException("IgniteException from onBeforePut on primary or backup: " + instanceName);
            }

            return newVal;
        }
    }
}
