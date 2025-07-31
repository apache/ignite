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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntFunction;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.cache.CacheInterceptorAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests to check scenarios with system failures during transaction commit. Internal system failures are simulated by
 * {@link CacheInterceptor} custom implementation throwing an exception during final commit phase.
 */
@RunWith(Parameterized.class)
public class CacheStoreWithIgniteTxFailureTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int GRID_COUNT = 3;

    /** */
    private static final int KEYS_NUMBER = 50;

    /** */
    private static final int FAULTY_NODE_IDX = 1;

    /** */
    private static final IntFunction<Integer> KEY_UPDATE_FUNCTION = key -> key + KEYS_NUMBER * 3;

    /**
     * Type of node for keys involved into transaction: primary or backup.
     */
    private enum FaultyNodeType {
        /** */
        PRIMARY,
        /** */
        BACKUP
    }

    /**
     * Role of faulty node in transaction management: tx coordinator or regular node.
     */
    private enum FaultyNodeRole {
        /** */
        REGULAR,
        /** */
        TX_COORDINATOR
    }

    /** */
    @Parameterized.Parameter
    public FaultyNodeType faultyNodeType;

    /** */
    @Parameterized.Parameter(1)
    public FaultyNodeRole faultyNodeRole;

    /** */
    @Parameterized.Parameter(2)
    public boolean withFaulireHnd;

    /** */
    @Parameterized.Parameters(name = "faultyNodeType={0}, faultyNodeRole={1}, withFaulireHandler={2}")
    public static List<Object[]> parameters() {
        List<Object[]> params = new ArrayList<>();

        params.add(new Object[] {FaultyNodeType.PRIMARY, FaultyNodeRole.REGULAR, true});
        params.add(new Object[] {FaultyNodeType.PRIMARY, FaultyNodeRole.REGULAR, false});
        params.add(new Object[] {FaultyNodeType.BACKUP, FaultyNodeRole.REGULAR, true});
        params.add(new Object[] {FaultyNodeType.BACKUP, FaultyNodeRole.REGULAR, false});

        params.add(new Object[] {FaultyNodeType.PRIMARY, FaultyNodeRole.TX_COORDINATOR, false});
        params.add(new Object[] {FaultyNodeType.BACKUP, FaultyNodeRole.TX_COORDINATOR, false});
        params.add(new Object[] {FaultyNodeType.PRIMARY, FaultyNodeRole.TX_COORDINATOR, true});
        params.add(new Object[] {FaultyNodeType.BACKUP, FaultyNodeRole.TX_COORDINATOR, true});

        return params;
    }

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
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return withFaulireHnd ? new StopNodeFailureHandler() : super.getFailureHandler(igniteInstanceName);
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration<?, ?> cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration<Integer, Integer> ccfg = (CacheConfiguration<Integer, Integer>) super.cacheConfiguration(igniteInstanceName);

        ccfg.setInterceptor(
            new FaultyNodeInterceptor(
                igniteInstanceName,
                getTestIgniteInstanceIndex(igniteInstanceName) == FAULTY_NODE_IDX,
                faultyNodeRole
            )
        );

        return ccfg;
    }

    /**
     *
     */
    @Test
    public void testSystemExceptionAfterCacheStoreCommit() throws Exception {
        IgniteEx ig = startGrids(gridCount());
        IgniteCache<Integer, Integer> cache = ig.cache(DEFAULT_CACHE_NAME);

        fillCache(cache, KEYS_NUMBER);

        int keysType = faultyNodeType == FaultyNodeType.PRIMARY ? 0 : 1;

        List<Integer> keysOnFaultyNode = findKeys(grid(FAULTY_NODE_IDX).localNode(), cache, 5, 0, keysType);

        IgniteEx txCoordinator =
            faultyNodeRole == FaultyNodeRole.TX_COORDINATOR
                ? grid(FAULTY_NODE_IDX)
                : startClientGrid(GRID_COUNT + 1);

        if (faultyNodeType == FaultyNodeType.PRIMARY)
            updateKeysInTxWithExceptionCatching(txCoordinator, keysOnFaultyNode);
        else
            updateKeysInTx(txCoordinator, keysOnFaultyNode);

        if (withFaulireHnd) {
            // FH doesn't fail TX coordinator node now, this behavior is wrong and should be fixed here:
            // TODO https://issues.apache.org/jira/browse/IGNITE-26060
            if (faultyNodeRole != FaultyNodeRole.TX_COORDINATOR) {
                waitForTopology(3);

                assertTrue("Client node should survive test scenario",
                    G.allGrids()
                        .stream()
                        .filter(ignite -> ((IgniteEx)ignite).context().clientNode())
                        .count() == 1);
            }
        }
        else
            checkKeysOnFaultyNode(keysOnFaultyNode);

        checkKeysOnHealthyNodes(keysOnFaultyNode);
    }

    /** */
    private void fillCache(IgniteCache<Integer, Integer> cache, int numOfKeys) {
        for (int i = 0; i < numOfKeys; i++)
            cache.put(i, i);
    }

    /** */
    private void checkKeysOnFaultyNode(List<Integer> keysToCheck) {
        IgniteCache<Object, Object> cache = grid(FAULTY_NODE_IDX).cache(DEFAULT_CACHE_NAME);

        for (Integer key : keysToCheck)
            assertEquals(storeStgy.getFromStore(key), cache.get(key));
    }

    /** */
    private void checkKeysOnHealthyNodes(List<Integer> keysToCheck) throws IgniteInterruptedCheckedException {
        for (int i = 0; i < gridCount(); i++) {
            if (i != FAULTY_NODE_IDX) {
                IgniteEx ig = grid(i);

                IgniteCache<Object, Object> cache = ig.cache(DEFAULT_CACHE_NAME);

                for (Integer key : keysToCheck) {
                    boolean checkResult = waitForCondition(
                        () -> {
                            if (storeStgy.getFromStore(key) == null || cache.get(key) == null)
                                return false;

                            return storeStgy.getFromStore(key).equals(cache.get(key));
                        },
                        1_000);

                    assertTrue(
                        String.format("Key inconsistent with CacheStore found on node %d; nodeName: %s. " +
                                "Key in store: %s, key in cache: %s.",
                            i,
                            ig.name(),
                            storeStgy.getFromStore(key),
                            cache.get(key)),
                        checkResult);
                }
            }
        }
    }

    /** */
    private void updateKeysInTx(Ignite ig, List<Integer> keys) {
        IgniteCache<Object, Object> cache = ig.cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = ig.transactions().txStart()) {
            for (Integer key : keys)
                cache.put(key, KEY_UPDATE_FUNCTION.apply(key));

            tx.commit();
        }
    }

    /** */
    private void updateKeysInTxWithExceptionCatching(Ignite ig, List<Integer> keys) {
        try {
            updateKeysInTx(ig, keys);
        }
        catch (Exception ignored) {
            // No-op.
        }
    }

    /** */
    private static class FaultyNodeInterceptor extends CacheInterceptorAdapter<Integer, Integer> {
        /** */
        private final FaultyNodeRole faultyNodeRole;

        /** */
        private final String instanceName;

        /** */
        private final boolean faultyNode;

        /** */
        private final Set<Integer> seenKeys = ConcurrentHashMap.newKeySet();

        /**
         * @param instanceName Ignite node instance name.
         * @param faultyNodeRole Flag if node is tx coordinator.
         */
        private FaultyNodeInterceptor(String instanceName, boolean faultyNode, FaultyNodeRole faultyNodeRole) {
            this.instanceName = instanceName;
            this.faultyNode = faultyNode;
            this.faultyNodeRole = faultyNodeRole;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Integer onBeforePut(Cache.Entry<Integer, Integer> entry, Integer newVal) {
            // It is an initial cache loading, actual test logic will be executed later.
            if (newVal < 2 * KEYS_NUMBER)
                return newVal;

            if (faultyNode) {
                if (faultyNodeRole == FaultyNodeRole.TX_COORDINATOR) {
                    // On TX coordinator node CacheInterceptor#onBeforePut is called twice for the same key
                    // at different stages of TX handling path.
                    if (!seenKeys.add(newVal))
                        throw new IgniteException("IgniteException from onBeforePut on tx coordinator: " + instanceName);
                }
                else
                    throw new IgniteException("IgniteException from onBeforePut on primary or backup: " + instanceName);
            }

            return newVal;
        }
    }
}
