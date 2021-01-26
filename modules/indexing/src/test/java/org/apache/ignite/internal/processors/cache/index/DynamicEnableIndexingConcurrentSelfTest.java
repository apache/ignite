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

package org.apache.ignite.internal.processors.cache.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteClientReconnectAbstractTest;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.processors.query.schema.message.SchemaFinishDiscoveryMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.TransactionSerializationException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test different scnerarions on concurrent enabling indexing.
 */
@RunWith(Parameterized.class)
public class DynamicEnableIndexingConcurrentSelfTest extends DynamicEnableIndexingAbstractTest {
    /** Test parameters. */
    @Parameters(name = "cacheMode={0},atomicityMode={1}")
    public static Iterable<Object[]> params() {
        CacheMode[] cacheModes = new CacheMode[] {CacheMode.PARTITIONED, CacheMode.REPLICATED};

        CacheAtomicityMode[] atomicityModes = new CacheAtomicityMode[] {
            CacheAtomicityMode.ATOMIC,
            CacheAtomicityMode.TRANSACTIONAL,
            CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT
        };

        List<Object[]> res = new ArrayList<>();
        for (CacheMode cacheMode : cacheModes) {
            for (CacheAtomicityMode atomicityMode : atomicityModes)
                res.add(new Object[] {cacheMode, atomicityMode});
        }

        return res;
    }

    /** Latches to block certain index operations. */
    private static final ConcurrentHashMap<UUID, T2<CountDownLatch, CountDownLatch>> BLOCKS =
            new ConcurrentHashMap<>();

    /** Name field index name. */
    private static final String NAME_FIELD_IDX_NAME = "name_idx";

    /** Large number of entries. */
    private static final int LARGE_NUM_ENTRIES = 100_000;

    /** */
    @Parameter(0)
    public CacheMode cacheMode;

    /** */
    @Parameter(1)
    public CacheAtomicityMode atomicityMode;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        GridQueryProcessor.idxCls = null;

        for (T2<CountDownLatch, CountDownLatch> block : BLOCKS.values())
            block.get1().countDown();

        BLOCKS.clear();

        stopAllGrids();

        super.afterTest();
    }

    /**
     * Test pending operation when coordinator change.
     */
    @Test
    public void testCoordinatorChange() throws Exception {
        // Start servers.
        IgniteEx srv1 = ignitionStart(serverConfiguration(1));
        ignitionStart(serverConfiguration(2));
        ignitionStart(serverConfiguration(3));
        ignitionStart(serverConfiguration(4));

        // Start client.
        IgniteEx cli = ignitionStart(clientConfiguration(5));
        cli.cluster().state(ClusterState.ACTIVE);

        createCache(cli);
        loadData(cli, 0, NUM_ENTRIES);

        // Test migration between normal servers.
        UUID id1 = srv1.cluster().localNode().id();

        CountDownLatch idxLatch = blockIndexing(id1);

        IgniteInternalFuture<?> tblFut = enableIndexing(cli);

        idxLatch.await();

        Ignition.stop(srv1.name(), true);

        unblockIndexing(id1);

        tblFut.get();

        for (Ignite g: G.allGrids()) {
            assertTrue(query(g, SELECT_ALL_QUERY).size() >= 3 * NUM_ENTRIES / 4 );

            performQueryingIntegrityCheck(g);

            checkQueryParallelism((IgniteEx)g, cacheMode);
        }
    }

    /** */
    @Test
    public void testClientReconnect() throws Exception {
        // Start servers.
        IgniteEx srv1 = ignitionStart(serverConfiguration(1));
        ignitionStart(serverConfiguration(2));
        ignitionStart(serverConfiguration(3));
        ignitionStart(serverConfiguration(4));

        // Start client.
        IgniteEx cli = ignitionStart(clientConfiguration(5));
        cli.cluster().state(ClusterState.ACTIVE);

        createCache(cli);
        loadData(cli, 0, NUM_ENTRIES);

        // Reconnect client and enable indexing before client connects.
        IgniteClientReconnectAbstractTest.reconnectClientNode(log, cli, srv1, () -> {
            try {
                enableIndexing(srv1).get();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to enable indexing", e);
            }
        });

        assertEquals(NUM_ENTRIES, query(cli, SELECT_ALL_QUERY).size());

        for (Ignite g: G.allGrids()) {
            assertEquals(NUM_ENTRIES, query(g, SELECT_ALL_QUERY).size());

            performQueryingIntegrityCheck(g);

            checkQueryParallelism((IgniteEx)g, cacheMode);
        }
    }

    /** */
    @Test
    public void testNodeJoinOnPendingOperation() throws Exception {
        CountDownLatch finishLatch = new CountDownLatch(3);

        IgniteEx srv1 = ignitionStart(serverConfiguration(1), finishLatch);
        srv1.cluster().state(ClusterState.ACTIVE);

        createCache(srv1);
        loadData(srv1, 0, NUM_ENTRIES);

        CountDownLatch idxLatch = blockIndexing(srv1);

        IgniteInternalFuture<?> tblFut = enableIndexing(srv1);

        U.await(idxLatch);

        ignitionStart(serverConfiguration(2), finishLatch);
        ignitionStart(serverConfiguration(3), finishLatch);

        awaitPartitionMapExchange();

        assertFalse(tblFut.isDone());

        unblockIndexing(srv1);

        tblFut.get();

        U.await(finishLatch);

        for (Ignite g: G.allGrids()) {
            assertEquals(NUM_ENTRIES, query(g, SELECT_ALL_QUERY).size());

            performQueryingIntegrityCheck(g);

            checkQueryParallelism((IgniteEx)g, cacheMode);
        }
    }

    /** Test chaining schema operation with enable indexing. */
    @Test
    public void testOperationChaining() throws Exception {
        IgniteEx srv1 = ignitionStart(serverConfiguration(1));

        ignitionStart(serverConfiguration(2));
        ignitionStart(serverConfiguration(3, true));
        ignitionStart(clientConfiguration(4));

        srv1.cluster().state(ClusterState.ACTIVE);

        createCache(srv1);
        loadData(srv1, 0, NUM_ENTRIES);

        CountDownLatch idxLatch = blockIndexing(srv1);

        IgniteInternalFuture<?> tblFut = enableIndexing(srv1);

        QueryIndex idx = new QueryIndex();
        idx.setName(NAME_FIELD_IDX_NAME.toUpperCase());
        idx.setFieldNames(Collections.singletonList(NAME_FIELD_NAME.toUpperCase()), true);

        IgniteInternalFuture<?> idxFut1 = srv1.context().query().dynamicIndexCreate(POI_CACHE_NAME, POI_SCHEMA_NAME,
                POI_TABLE_NAME, idx, false, 0);

        idxLatch.await();

        // Add more nodes.
        ignitionStart(serverConfiguration(5));
        ignitionStart(serverConfiguration(6, true));
        ignitionStart(clientConfiguration(7));

        assertFalse(tblFut.isDone());
        assertFalse(idxFut1.isDone());

        unblockIndexing(srv1);

        idxFut1.get();

        for (Ignite g: G.allGrids()) {
            assertEquals(NUM_ENTRIES, query(g, SELECT_ALL_QUERY).size());

            performQueryingIntegrityCheck(g);

            checkQueryParallelism((IgniteEx)g, cacheMode);

            IgniteCache<Object, Object> cache = g.cache(POI_CACHE_NAME);

            assertIndexUsed(cache, "SELECT * FROM " + POI_TABLE_NAME + " WHERE name = 'POI_100'", NAME_FIELD_IDX_NAME);

            List<List<?>> res = cache.query(new SqlFieldsQuery("SELECT " + ID_FIELD_NAME + " FROM " + POI_TABLE_NAME +
                    " WHERE name = 'POI_100'").setSchema(POI_SCHEMA_NAME)).getAll();

            assertEquals(1, res.size());
            assertEquals(100, res.get(0).get(0));
        }
    }

    /** Enable indexing on ongoing rebalance. */
    @Test
    public void testConcurrentRebalance() throws Exception {
        // Start cache and populate it with data.
        IgniteEx srv1 = ignitionStart(serverConfiguration(1));
        Ignite srv2 = ignitionStart(serverConfiguration(2));
        srv1.cluster().state(ClusterState.ACTIVE);

        createCache(srv1);
        loadData(srv1, 0, LARGE_NUM_ENTRIES);

        // Start index operation in blocked state.
        CountDownLatch idxLatch1 = blockIndexing(srv1);
        CountDownLatch idxLatch2 = blockIndexing(srv2);

        IgniteInternalFuture<?> tblFut = enableIndexing(srv1);

        U.await(idxLatch1);
        U.await(idxLatch2);

        // Start two more nodes and unblock index operation in the middle.
        ignitionStart(serverConfiguration(3));

        unblockIndexing(srv1);
        unblockIndexing(srv2);

        ignitionStart(serverConfiguration(4));

        awaitPartitionMapExchange();

        tblFut.get();

        for (Ignite g: G.allGrids()) {
            assertEquals(LARGE_NUM_ENTRIES, query(g, SELECT_ALL_QUERY).size());

            performQueryingIntegrityCheck(g);

            checkQueryParallelism((IgniteEx)g, cacheMode);
        }
    }

    /** Test concurrent put remove when enabling indexing. */
    @Test
    public void testConcurrentPutRemove() throws Exception {
        CountDownLatch finishLatch = new CountDownLatch(4);

        // Start several nodes.
        IgniteEx srv1 = ignitionStart(serverConfiguration(1), finishLatch);
        ignitionStart(serverConfiguration(2), finishLatch);
        ignitionStart(serverConfiguration(3), finishLatch);
        ignitionStart(serverConfiguration(4), finishLatch);

        srv1.cluster().state(ClusterState.ACTIVE);

        createCache(srv1);
        loadData(srv1, 0, LARGE_NUM_ENTRIES);

        // Start data change operations from several threads.
        final AtomicBoolean stopped = new AtomicBoolean();
        final CountDownLatch iterations = new CountDownLatch(1000);

        IgniteInternalFuture<?> task = multithreadedAsync(() -> {
            while (!stopped.get()) {
                Ignite node = grid(ThreadLocalRandom.current().nextInt(1, 5));

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                int i = rnd.nextInt(0, LARGE_NUM_ENTRIES);

                BinaryObject val = node.binary().builder(POI_CLASS_NAME)
                    .setField(NAME_FIELD_NAME, "POI_" + i, String.class)
                    .setField(LATITUDE_FIELD_NAME, rnd.nextDouble(), Double.class)
                    .setField(LONGITUDE_FIELD_NAME, rnd.nextDouble(), Double.class)
                    .build();

                IgniteCache<Object, BinaryObject> cache = node.cache(POI_CACHE_NAME).withKeepBinary();

                try {
                    if (ThreadLocalRandom.current().nextBoolean())
                        cache.put(i, val);
                    else
                        cache.remove(i);
                }
                catch (CacheException e) {
                    if (!X.hasCause(e, TransactionSerializationException.class))
                        throw e;
                }
                finally {
                    iterations.countDown();
                }
            }

            return null;
        }, 4);

        // Do some work.
        iterations.await(2, TimeUnit.SECONDS);

        enableIndexing(srv1).get();

        // Stop updates once index is ready.
        stopped.set(true);
        task.get();

        finishLatch.await();

        // Perform integrity check.
        IgniteCache<Object, Object> cache = srv1.cache(POI_CACHE_NAME).withKeepBinary();

        query(srv1, SELECT_ALL_QUERY).forEach(res -> {
            BinaryObject val = (BinaryObject)cache.get(res.get(0));

            assertNotNull(val);

            assertEquals(val.field(NAME_FIELD_NAME), res.get(1));
            assertEquals(val.field(LATITUDE_FIELD_NAME), res.get(2));
            assertEquals(val.field(LONGITUDE_FIELD_NAME), res.get(3));
        });
    }

    /** Test concurrent enabling indexing. Only one attempt should succeed. */
    @Test
    public void testConcurrentEnableIndexing() throws Exception {
        // Start several nodes.
        IgniteEx srv1 = ignitionStart(serverConfiguration(1));
        ignitionStart(serverConfiguration(2));
        ignitionStart(clientConfiguration(3));
        ignitionStart(clientConfiguration(4));

        srv1.cluster().state(ClusterState.ACTIVE);

        createCache(srv1);
        loadData(srv1, 0, LARGE_NUM_ENTRIES);

        // Start enable indexing from several threads.
        final AtomicBoolean stopped = new AtomicBoolean();
        final AtomicInteger success = new AtomicInteger();
        final CountDownLatch iterations = new CountDownLatch(1000);

        IgniteInternalFuture<?> task = multithreadedAsync(() -> {
            while (!stopped.get()) {
                IgniteEx node = grid(ThreadLocalRandom.current().nextInt(1, 4));

                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    break;
                }

                enableIndexing(node).chain((fut) -> {
                    try {
                        fut.get();

                        success.incrementAndGet();
                    }
                    catch (IgniteCheckedException e) {
                        assertTrue(e.hasCause(SchemaOperationException.class));

                        SchemaOperationException opEx = e.getCause(SchemaOperationException.class);

                        assertEquals(SchemaOperationException.CODE_CACHE_ALREADY_INDEXED, opEx.code());
                        assertEquals("Cache is already indexed: " + POI_CACHE_NAME, opEx.getMessage());
                    }

                    return null;
                });

                iterations.countDown();
            }

            return null;
        }, 4);

        // Do attempts.
        iterations.await(2, TimeUnit.SECONDS);

        // Start more server nodes..
        ignitionStart(serverConfiguration(5));
        ignitionStart(serverConfiguration(6));

        // Stop task.
        stopped.set(true);
        task.get();

        // Check that only one successful attempt.
        assertEquals(1, success.get());

        awaitPartitionMapExchange();

        for (Ignite g: G.allGrids()) {
            assertEquals(LARGE_NUM_ENTRIES, query(g, SELECT_ALL_QUERY).size());

            performQueryingIntegrityCheck(g);

            checkQueryParallelism((IgniteEx)g, cacheMode);
        }
    }

    /** */
    private IgniteInternalFuture<?> enableIndexing(IgniteEx node) {
        Integer parallelism = cacheMode == CacheMode.PARTITIONED ? QUERY_PARALLELISM : null;

        return node.context().query().dynamicAddQueryEntity(POI_CACHE_NAME, POI_SCHEMA_NAME, queryEntity(), parallelism,
                false);
    }

    /** */
    private QueryEntity queryEntity() {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put(ID_FIELD_NAME, Integer.class.getName());
        fields.put(NAME_FIELD_NAME, String.class.getName());
        fields.put(LATITUDE_FIELD_NAME, Double.class.getName());
        fields.put(LONGITUDE_FIELD_NAME, Double.class.getName());

        return new QueryEntity()
            .setKeyType(Integer.class.getName())
            .setKeyFieldName(ID_FIELD_NAME)
            .setValueType(POI_CLASS_NAME)
            .setTableName(POI_TABLE_NAME)
            .setFields(fields);
    }

    /** */
    private void createCache(IgniteEx node) throws Exception {
        CacheConfiguration<?, ?> ccfg = testCacheConfiguration(POI_CACHE_NAME, cacheMode, atomicityMode);

        node.context().cache().dynamicStartCache(ccfg, POI_CACHE_NAME, null, true, true, true).get();
    }

    /** */
    private static void awaitIndexing(UUID nodeId) {
        T2<CountDownLatch, CountDownLatch> blocker = BLOCKS.get(nodeId);

        if (blocker != null) {
            blocker.get2().countDown();

            while (true) {
                try {
                    blocker.get1().await();

                    break;
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /** */
    private static CountDownLatch blockIndexing(Ignite node) {
        UUID nodeId = ((IgniteEx)node).localNode().id();

        return blockIndexing(nodeId);
    }

    /** */
    private static CountDownLatch blockIndexing(UUID nodeId) {
        assertFalse(BLOCKS.containsKey(nodeId));

        CountDownLatch idxLatch = new CountDownLatch(1);

        BLOCKS.put(nodeId, new T2<>(new CountDownLatch(1), idxLatch));

        return idxLatch;
    }

    /** */
    private static void unblockIndexing(Ignite node) {
        UUID nodeId = ((IgniteEx)node).localNode().id();

        unblockIndexing(nodeId);
    }

    /** */
    private static void unblockIndexing(UUID nodeId) {
        T2<CountDownLatch, CountDownLatch> blocker = BLOCKS.remove(nodeId);

        assertNotNull(blocker);

        blocker.get1().countDown();
    }

    /** */
    private IgniteEx ignitionStart(IgniteConfiguration cfg) throws Exception {
        return ignitionStart(cfg, null);
    }

    /**
     * Spoof blocking indexing class and start new node.
     * @param cfg Node configuration.
     * @param latch Latch to await schema operation finish.
     * @return New node.
     * @throws Exception If failed.
     */
    private IgniteEx ignitionStart(IgniteConfiguration cfg, final CountDownLatch latch) throws Exception {
        GridQueryProcessor.idxCls = BlockingIndexing.class;

        IgniteEx node = startGrid(cfg);

        if (latch != null) {
            node.context().discovery().setCustomEventListener(SchemaFinishDiscoveryMessage.class,
                new CustomEventListener<SchemaFinishDiscoveryMessage>() {
                    @Override public void onCustomEvent(
                        AffinityTopologyVersion topVer,
                        ClusterNode snd,
                        SchemaFinishDiscoveryMessage msg
                    ) {
                        latch.countDown();
                    }
                });
        }

        return node;
    }

    /**
     * Blocking indexing processor.
     */
    private static class BlockingIndexing extends IgniteH2Indexing {
        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> rebuildIndexesFromHash(GridCacheContext cctx) {
            awaitIndexing(ctx.localNodeId());

            return super.rebuildIndexesFromHash(cctx);
        }
    }
}
