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

package org.apache.ignite.internal.processors.tx;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.internal.processors.cache.CacheLazyEntry;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.MapCacheStoreStrategy;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/** Check per node data consistency after exceptionally interceptor method call. */
@RunWith(Parameterized.class)
public class TxWithExceptionalInterceptorTest extends GridCommonAbstractTest {
    /** Node role. */
    @Parameterized.Parameter(0)
    public TxCoordNodeRole txCoord;

    /** Persistence flag. */
    @Parameterized.Parameter(1)
    public boolean persistence;

    /** Write through flag. */
    @Parameterized.Parameter(2)
    public boolean writeThrough;

    /** */
    private static final String INDEXING_ENGINE = IndexingQueryEngineConfiguration.ENGINE_NAME;

    /** */
    private static final String CALCITE_ENGINE = CalciteQueryEngineConfiguration.ENGINE_NAME;

    /** Num of exception raised from interceptor. */
    private static final AtomicInteger exceptionRaised = new AtomicInteger();

    /** Cache with or without writeThrough configured. */
    private static final String PROC_CACHE_NAME = "PROC_CACHE";

    /** Tx involved cache. */
    private static final String COMMON_CACHE_NAME = "COMMON_CACHE";

    /** Default client name. */
    private static final String CLIENT_NAME = "client";

    /** Write through emulation store strategy. */
    private static final MapCacheStoreStrategy strategy = new MapCacheStoreStrategy();

    /** */
    @Parameterized.Parameters(name = "txCoordRole={0}, persistence={1}, writeThrough={2}")
    public static Collection<?> parameters() {
        return GridTestUtils.cartesianProduct(
            List.of(TxCoordNodeRole.PRIMARY, TxCoordNodeRole.BACKUP, TxCoordNodeRole.COORDINATOR_NO_DATA, TxCoordNodeRole.THICK_CLIENT),
            List.of(true, false),
            List.of(true, false)
        );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        if (persistence)
            cleanPersistenceDir();

        strategy.resetStore();

        exceptionRaised.set(0);
    }

    /** */
    private static class FilterDefinedNode implements IgnitePredicate<ClusterNode> {
        /** */
        Object predicate;

        /** */
        private FilterDefinedNode(Object predicate) {
            this.predicate = predicate;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            assert predicate != null;

            return !node.consistentId().equals(predicate);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setFailureHandler(new StopNodeFailureHandler());

        cfg.getSqlConfiguration().setQueryEnginesConfiguration(
            new IndexingQueryEngineConfiguration(), new CalciteQueryEngineConfiguration().setDefault(true));

        if (persistence) {
            cfg.setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                    ));
        }

        CacheConfiguration<Integer, Integer> ccfg1 = new CacheConfiguration<>(PROC_CACHE_NAME);
        ccfg1.setAtomicityMode(TRANSACTIONAL);
        ccfg1.setCacheMode(REPLICATED);

        ccfg1.setInterceptor(new CustomInterceptor(writeThrough, txCoord, getTestIgniteInstanceName(0)));
        ccfg1.setQueryEntities(List.of(queryEntity(PROC_CACHE_NAME)));

        // All server nodes besides zero-indexed
        if (txCoord == TxCoordNodeRole.COORDINATOR_NO_DATA)
            ccfg1.setNodeFilter(new FilterDefinedNode(getTestIgniteInstanceName(0)));

        if (writeThrough) {
            Factory<? extends CacheStore<Object, Object>> storeFactory = strategy.getStoreFactory();
            ccfg1.setReadThrough(true);
            ccfg1.setWriteThrough(true);
            ccfg1.setCacheStoreFactory(storeFactory);
        }

        CacheConfiguration<Integer, Integer> ccfg2 = new CacheConfiguration<>(COMMON_CACHE_NAME);
        ccfg2.setAtomicityMode(TRANSACTIONAL);
        ccfg2.setCacheMode(REPLICATED);
        ccfg2.setQueryEntities(List.of(queryEntity(COMMON_CACHE_NAME)));

        cfg.setCacheConfiguration(ccfg1, ccfg2);

        return cfg;
    }

    /** */
    private static class CustomInterceptor implements CacheInterceptor<Integer, Integer> {
        /** Initial value holder. */
        private Object initialVal;

        /** First call trigger, with enabled writeThrough interceptor is called twice. */
        private final AtomicBoolean called = new AtomicBoolean();

        /** writeThrough flag */
        private final boolean writeThrough;

        /** Tx coordinator node role. */
        private final TxCoordNodeRole txCoord;

        /** Tx initiator node name. */
        private final String actorNode;

        /** */
        private CustomInterceptor(
            boolean writeThrough,
            TxCoordNodeRole txCoord,
            String actorNode
        ) {
            this.writeThrough = writeThrough;
            this.txCoord = txCoord;
            this.actorNode = actorNode;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Integer onGet(Integer key, @Nullable Integer val) {
            return val;
        }

        /** Raised unchecked exception according to tx initiator node. */
        @Override public @Nullable Integer onBeforePut(Cache.Entry<Integer, Integer> entry, Integer newVal) {
            if (initialVal != null && initialVal != newVal) {
                if (!writeThrough || called.get()) {
                    assertTrue(entry instanceof CacheLazyEntry);

                    GridCacheContext ctx = GridTestUtils.getFieldValue(entry, CacheLazyEntry.class, "cctx");

                    if (txCoord == TxCoordNodeRole.COORDINATOR_NO_DATA) {
                        if (ctx.localNode().consistentId().equals(actorNode)) {
                            // Unexpected interceptor call on node without data
                            fail("Need to be called only on primary or backup nodes.");
                        }
                    }
                    else {
                        if (ctx.localNode().consistentId().equals(actorNode))
                            raiseException();
                    }
                }

                called.set(true);
            }

            if (initialVal == null)
                initialVal = newVal;

            return newVal * 100;
        }

        /** */
        private void raiseException() {
            exceptionRaised.incrementAndGet();

            throw new RuntimeException("Interceptor unchecked exception");
        }

        /** {@inheritDoc} */
        @Override public void onAfterPut(Cache.Entry<Integer, Integer> entry) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public @Nullable IgniteBiTuple<Boolean, Integer> onBeforeRemove(Cache.Entry<Integer, Integer> entry) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Cache.Entry<Integer, Integer> entry) {
            // No-op.
        }
    }

    /** */
    private QueryEntity queryEntity(String cacheName) {
        var entity = new QueryEntity();

        entity.setKeyType(Integer.class.getName());
        entity.setValueType(Integer.class.getName());
        entity.addQueryField("ID", Integer.class.getName(), null);
        entity.addQueryField("VAL", Integer.class.getName(), null);
        entity.setKeyFieldName("ID");
        entity.setValueFieldName("VAL");
        entity.setTableName(cacheName);

        return entity;
    }

    /** */
    @Test
    public void testTxWithExceptionInterceptor() throws Exception {
        Ignite ignite0 = startGrid(0);
        startGrid(1);
        startGrid(2);

        Ignite client = startClientGrid(CLIENT_NAME);

        Ignite txNode = txCoord == TxCoordNodeRole.THICK_CLIENT ? client : ignite0;

        ignite0.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> processedCache = txNode.cache(PROC_CACHE_NAME);
        IgniteCache<Integer, Integer> commonCache = txNode.cache(COMMON_CACHE_NAME);

        Integer primaryKey = primaryKeyCoordAware(PROC_CACHE_NAME);
        Integer primaryKeyCommon = primaryKeyCoordAware(PROC_CACHE_NAME);

        try (Transaction tx = txNode.transactions().txStart()) {
            processedCache.put(primaryKey, 1);
            commonCache.put(primaryKeyCommon, 10);
            tx.commit();
        }

        //noinspection EmptyCatchBlock
        try (Transaction tx = txNode.transactions().txStart()) {
            processedCache.put(primaryKey, 2);
            commonCache.put(primaryKeyCommon, 20);
            tx.commit();
        }
        catch (Throwable th) {
            // No op.
        }

        // 2 server nodes + 1 thick client
        if ((txCoord == TxCoordNodeRole.BACKUP || txCoord == TxCoordNodeRole.PRIMARY) ||
            !writeThrough && txCoord == TxCoordNodeRole.THICK_CLIENT)
            waitForTopology(3);

        checkExceptionRaised();

        // external storage stored result
        Object storeVal = null;

        if (writeThrough)
            storeVal = strategy.getFromStore(primaryKey);

        // Processed cache kv result
        Object kvVal = null;

        List<Ignite> grids = new ArrayList<>(G.allGrids());

        grids.sort(new Comparator<>() {
            @Override public int compare(Ignite ignite, Ignite t1) {
                return ignite.name().compareTo(t1.name());
            }
        });

        // client first
        assertTrue(grids.get(0).name().contains(CLIENT_NAME));

        for (Ignite node : grids) {
            if (txCoord == TxCoordNodeRole.PRIMARY) {
                if (!writeThrough) {
                    getSqlResultByKey(node, PROC_CACHE_NAME, primaryKey, true);
                    getKVResultByKey(node, PROC_CACHE_NAME, primaryKey, true);
                }

                getSqlResultByKey(node, COMMON_CACHE_NAME, primaryKey, true);
                getKVResultByKey(node, COMMON_CACHE_NAME, primaryKey, true);

                continue;
            }

            // obtain sql results first, kv api can eventually recover results, thus for more clear test - let`s check sql first
            Object sqlVal = getSqlResultByKey(node, PROC_CACHE_NAME, primaryKey, false);

            if (kvVal == null)
                kvVal = getKVResultByKey(grid(1), PROC_CACHE_NAME, primaryKey, false);

            if (writeThrough) {
                kvVal = getKVResultByKey(node, PROC_CACHE_NAME, primaryKey, false);
                // TODO: IGNITE-28005 Interceptor is not called if coordinator is not a primary or backup node
                if (txCoord == TxCoordNodeRole.BACKUP) {
                    assertEquals("node: " + node.name() + ", storeVal=" + storeVal + ", cacheVal=" + kvVal,
                        storeVal, kvVal);
                    assertEquals("node: " + node.name() + ", storeVal=" + storeVal + ", sqlVal=" + sqlVal,
                        storeVal, sqlVal);
                }
            }
            else {
                Object cacheVal = getKVResultByKey(node, PROC_CACHE_NAME, primaryKey, false);

                assertEquals("node: " + node.name() + ", refVal=" + kvVal + ", cacheVal=" + cacheVal,
                    kvVal, cacheVal);

                assertEquals("node: " + node.name() + ", refVal=" + kvVal + ", sqlVal=" + sqlVal,
                    kvVal, sqlVal);
            }

            Object commonSqlRes = getSqlResultByKey(node, COMMON_CACHE_NAME, primaryKey, false);
            Object commonKvRes = getKVResultByKey(node, COMMON_CACHE_NAME, primaryKey, false);

            assertEquals("node: " + node.name() + ", commonSqlRes=" + commonSqlRes + ", commonKvRes=" + commonKvRes,
                commonSqlRes, commonKvRes);
        }
    }

    /** */
    private void checkExceptionRaised() {
        if (txCoord == TxCoordNodeRole.PRIMARY || txCoord == TxCoordNodeRole.BACKUP)
            assertEquals(1, exceptionRaised.get());
        else if (txCoord == TxCoordNodeRole.THICK_CLIENT) {
            if (writeThrough) {
                // TODO: IGNITE-28005 Interceptor is not called if coordinator is not a primary or backup node
                assertEquals(0, exceptionRaised.get());
            }
            else
                assertEquals(1, exceptionRaised.get());
        }
        else if (txCoord == TxCoordNodeRole.COORDINATOR_NO_DATA) {
            // TODO: IGNITE-28005 Interceptor is not called if coordinator is not a primary or backup node
            assertEquals(0, exceptionRaised.get());
        }
    }

    /** Get result through kv api. */
    private Object getKVResultByKey(Ignite node, String cacheName, Integer key, boolean checkResIsEmpty) {
        Object kvVal;

        try (Transaction tx = node.transactions().txStart()) {
            kvVal = node.cache(cacheName).get(key);
            tx.commit();
        }

        if (checkResIsEmpty) {
            assertNull(kvVal);

            return null;
        }

        assertNotNull("Value is empty on: " + node.name(), kvVal);

        return kvVal;
    }

    /** Get result through sql api. */
    private Integer getSqlResultByKey(Ignite node, String cacheName, Integer key, boolean checkResIsEmpty) {
        List<List<?>> resCalcite = node.cache(cacheName).query(
            new SqlFieldsQuery("SELECT /*+ QUERY_ENGINE('" + CALCITE_ENGINE + "') */ val FROM " +
                cacheName + " WHERE id = ?").setArgs(key)).getAll();

        List<List<?>> resIdx = node.cache(cacheName).query(
            new SqlFieldsQuery("SELECT /*+ QUERY_ENGINE('" + INDEXING_ENGINE + "') */ val FROM " +
                cacheName + " WHERE id = ?").setArgs(key)).getAll();

        if (checkResIsEmpty) {
            assertTrue("Expect empty result", resCalcite.isEmpty());
            assertTrue("Expect empty result", resIdx.isEmpty());

            return null;
        }
        else {
            Object firstResCalcite = resCalcite.get(0).get(0);
            Object firstResIdx = resIdx.get(0).get(0);

            assertEquals(firstResCalcite, firstResIdx);

            return (Integer)firstResIdx;
        }
    }

    /** Calculate primary key according tx initiator node. */
    protected Integer primaryKeyCoordAware(String cacheName) {
        switch (txCoord) {
            case PRIMARY:
            case THICK_CLIENT: {
                IgniteCache<Integer, Integer> cache = grid(0).cache(cacheName);
                return primaryKey(cache);
            }
            case COORDINATOR_NO_DATA:
            case BACKUP: {
                IgniteCache<Integer, Integer> cache = grid(1).cache(cacheName);
                return primaryKey(cache);
            }
            default:
                throw new IllegalArgumentException(txCoord.name());
        }
    }

    /**
     * Type of tx initiator node
     */
    private enum TxCoordNodeRole {
        /** Tx initiator the same as primary node for given key. */
        PRIMARY,

        /** Tx initiator differ from primary node for given key. */
        BACKUP,

        /** Tx initiator with filtered data.
         *
         * @see CacheConfiguration#setNodeFilter
         */
        COORDINATOR_NO_DATA,

        /** Thick client as tx initiator. */
        THICK_CLIENT
    }
}
