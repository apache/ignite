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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
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
import org.apache.ignite.internal.processors.query.schema.message.SchemaFinishDiscoveryMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
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
    @Parameters(name = "cacheMode={0}, atomicityMode={1}")
    public static Iterable<Object[]> params() {
        return Arrays.asList(
                new Object[] {CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC},
                new Object[] {CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL},
                new Object[] {CacheMode.REPLICATED, CacheAtomicityMode.ATOMIC},
                new Object[] {CacheMode.REPLICATED, CacheAtomicityMode.TRANSACTIONAL});
    }

    /** Latches to block certain index operations. */
    private static final ConcurrentHashMap<UUID, T2<CountDownLatch, CountDownLatch>> BLOCKS =
            new ConcurrentHashMap<>();

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

        for (Ignite g: G.allGrids())
            performQueryingIntegrityCheck((IgniteEx)g);
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

        performQueryingIntegrityCheck(cli);
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

        for (Ignite g: G.allGrids())
            performQueryingIntegrityCheck((IgniteEx)g);
    }

    /** */
    private IgniteInternalFuture<?> enableIndexing(IgniteEx node) {
       return node.context().query().dynamicAddQueryEntity(POI_CACHE_NAME, POI_SCHEMA_NAME, queryEntity(), false);
    }

    /** */
    private QueryEntity queryEntity() {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put(ID_FIELD_NAME, Integer.class.getName());
        fields.put(NAME_FIELD_NAME, String.class.getName());
        fields.put(LATITUDE_FIELD_NAME, Double.class.getName());
        fields.put(LONGITUDE_FIELD_NAME, Double.class.getName());

        return new QueryEntity().setKeyType(Integer.class.getName())
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
    private static IgniteEx ignitionStart(IgniteConfiguration cfg) {
        return ignitionStart(cfg, null);
    }

    /**
     * Spoof blocking indexing class and start new node.
     * @param cfg Node configuration.
     * @param latch Latch to await schema operation finish.
     * @return New node.
     */
    private static IgniteEx ignitionStart(IgniteConfiguration cfg, final CountDownLatch latch) {
        GridQueryProcessor.idxCls = BlockingIndexing.class;

        IgniteEx node = (IgniteEx)Ignition.start(cfg);

        if (latch != null)
            node.context().discovery().setCustomEventListener(SchemaFinishDiscoveryMessage.class,
                    new CustomEventListener<SchemaFinishDiscoveryMessage>() {
                        @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd,
                                                            SchemaFinishDiscoveryMessage msg) {
                            latch.countDown();
                        }
                    });

        return node;
    }

    /**
     * Blocking indexing processor.
     */
    private static class BlockingIndexing extends IgniteH2Indexing {
        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> rebuildIndexesFromHash(GridCacheContext cctx, boolean rebuildInMemory) {
            awaitIndexing(ctx.localNodeId());

            return super.rebuildIndexesFromHash(cctx, rebuildInMemory);
        }
    }
}
