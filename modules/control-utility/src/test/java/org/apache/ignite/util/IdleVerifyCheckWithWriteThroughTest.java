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

package org.apache.ignite.util;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.managers.eventstorage.HighPriorityListener;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;

/** */
public class IdleVerifyCheckWithWriteThroughTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** Node kill trigger. */
    private static CountDownLatch nodeKillLatch;

    /** */
    @Parameterized.Parameter(1)
    public Boolean withPersistence;

    /** */
    @Parameterized.Parameter(2)
    public Boolean multiCache;

    /** */
    private static final String CORRECT_VERIFY_MSG = "The check procedure has finished, no conflicts have been found.";

    /** */
    private static final String WITHOUT_WRITE_THROUGH_CACHE = "withoutWriteThrough";

    /** */
    @Parameterized.Parameters(name = "cmdHnd={0}, withPersistence={1}, multiCache={2}")
    public static Collection<Object[]> parameters() {
        return List.of(
            new Object[] {CLI_CMD_HND, false, false},
            new Object[] {CLI_CMD_HND, false, true},
            new Object[] {CLI_CMD_HND, true, false},
            new Object[] {CLI_CMD_HND, true, true}
        );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        persistenceEnable(withPersistence);

        if (withPersistence)
            cleanPersistenceDir();

        nodeKillLatch = new CountDownLatch(1);
        MapCacheStore.salvagedLatch = new CountDownLatch(1);
        MapCacheStore.txCoordStoreLatch = new CountDownLatch(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try {
            for (Ignite node : G.allGrids()) {
                Collection<IgniteInternalTx> txs = ((IgniteKernal)node).context().cache().context().tm().activeTransactions();

                assertTrue("Unfinished txs [node=" + node.name() + ", txs=" + txs + ']', txs.isEmpty());
            }
        }
        finally {
            stopAllGrids();

            super.afterTest();
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean persistenceEnable() {
        return withPersistence;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi());
    }

    /** */
    @Test
    public void testTxCoordinatorLeftClusterWithEnabledReadWriteThrough() throws Exception {
        // sequential start is important here
        IgniteEx nodeCoord = startGrid(0);
        // near node
        IgniteEx nodePrimary = startGrid(1);
        // backup node
        IgniteEx nodeBackup = startGrid(2);

        int firstVal = 0;
        int secondVal = 1;

        nodeCoord.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> ccfgWithWriteThrough = createCache(DEFAULT_CACHE_NAME, true);
        IgniteCache<Object, Object> cache = nodeCoord.createCache(ccfgWithWriteThrough);

        IgniteCache<Object, Object> cacheWithoutWriteThrough = null;

        if (multiCache) {
            CacheConfiguration<Object, Object> ccfgWithoutWriteThrough = createCache(WITHOUT_WRITE_THROUGH_CACHE, false);
            cacheWithoutWriteThrough = nodeCoord.createCache(ccfgWithoutWriteThrough);
        }

        Integer primaryKey = primaryKey(nodePrimary.cache(DEFAULT_CACHE_NAME));

        try (Transaction tx = nodeCoord.transactions().txStart(OPTIMISTIC, READ_COMMITTED)) {
            cache.put(primaryKey, firstVal);

            if (multiCache) {
                Integer primaryKeyWithoutWriteThrough = primaryKey(nodePrimary.cache(WITHOUT_WRITE_THROUGH_CACHE));
                cacheWithoutWriteThrough.put(primaryKeyWithoutWriteThrough, firstVal);
            }

            tx.commit();
        }

        sqlVisibilityCheck(List.of(nodeCoord, nodeBackup), primaryKey, firstVal);

        nodeCoord.cluster().state(ClusterState.INACTIVE);

        GridMessageListener lsnr = new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                if (msg instanceof GridNearTxPrepareResponse) {
                    Collection<IgniteInternalTx> txs = nodeBackup.context().cache().context().tm().activeTransactions();

                    assertEquals(1, txs.size());
                    assertFalse(txs.iterator().next().local());

                    nodeKillLatch.countDown();

                    try {
                        assertTrue(waitForCondition(() ->
                            !nodeBackup.context().cache().context().tm().hackMap.isEmpty(), 5_000));
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };

        nodeCoord.context().io().removeMessageListener(GridTopic.TOPIC_CACHE); // Remove old cache listener.
        nodeCoord.context().io().addMessageListener(GridTopic.TOPIC_CACHE, lsnr); // Register as first listener.
        nodeCoord.context().cache().context().io().start0(); // Register cache listener again.

        nodeCoord.cluster().state(ClusterState.ACTIVE);

        nodeCoord.context().event().addDiscoveryEventListener(new BeforeRecoveryListener(), EVT_NODE_FAILED, EVT_NODE_LEFT);

        IgniteInternalFuture<Object> stopFut = GridTestUtils.runAsync(() -> {
            nodeKillLatch.await();
            nodePrimary.close();
        });

        injectTestSystemOut();

        try (Transaction tx = nodeCoord.transactions().txStart(OPTIMISTIC, READ_COMMITTED)) {
            cache.put(primaryKey, secondVal);

            if (multiCache) {
                Integer primaryKeyWithoutWriteThrough = primaryKey(nodePrimary.cache(WITHOUT_WRITE_THROUGH_CACHE));
                cacheWithoutWriteThrough.put(primaryKeyWithoutWriteThrough, secondVal);
            }

            tx.commit();
        }
        catch (Throwable th) {
            fail("Unexpected exception: " + th);
        }

        stopFut.get(getTestTimeout());

        awaitPartitionMapExchange();

        sqlVisibilityCheck(List.of(nodeCoord, nodeBackup), primaryKey, secondVal);

        int cacheSize = cache.size();
        assertEquals(1, cacheSize);

        int locSize = -1;
        Object peeked = null;

        for (Ignite g : G.allGrids()) {
            final Affinity<Integer> aff = affinity(g.cache(DEFAULT_CACHE_NAME));

            boolean primary = aff.isPrimary(g.cluster().localNode(), primaryKey);

            Object peeked0;

            if (primary)
                peeked0 = g.cache(DEFAULT_CACHE_NAME).localPeek(primaryKey, CachePeekMode.PRIMARY);
            else
                peeked0 = g.cache(DEFAULT_CACHE_NAME).localPeek(primaryKey, CachePeekMode.BACKUP);

            if (peeked == null)
                peeked = peeked0;
            else {
                assertEquals(peeked, peeked0);
                assertNotNull(peeked);
            }

            int locSize0;

            if (primary)
                locSize0 = g.cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.PRIMARY);
            else
                locSize0 = g.cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.BACKUP);

            if (locSize == -1)
                locSize = locSize0;
            else {
                assertEquals(locSize, locSize0);
            }

            assertNotNull("grid instance: " + g.name(), g.cache(DEFAULT_CACHE_NAME).get(primaryKey));
        }

        assertEquals(EXIT_CODE_OK, execute("--port", connectorPort(grid(2)), "--cache", "idle_verify"));

        String out = testOut.toString();

        // partVerHash can be different
        if (withPersistence) {
            Assert.assertThat(out, anyOf(is(containsString("updateCntr=[lwm=2, missed=[], hwm=2], " +
                "partitionState=OWNING, size=1")), is(containsString(CORRECT_VERIFY_MSG))));
            Assert.assertThat(out, anyOf(is(containsString("updateCntr=[lwm=2, missed=[], hwm=2], " +
                "partitionState=OWNING, size=1")), is(containsString(CORRECT_VERIFY_MSG))));
        }
        else {
            Assert.assertThat(out, anyOf(is(containsString("consistentId=gridCommandHandlerTest0, " +
                "updateCntr=1, partitionState=OWNING, size=1")), is(containsString(CORRECT_VERIFY_MSG))));
            Assert.assertThat(out, anyOf(is(containsString("consistentId=gridCommandHandlerTest2, " +
                "updateCntr=1, partitionState=OWNING, size=1")), is(containsString(CORRECT_VERIFY_MSG))));
        }
        testOut.reset();

        if (withPersistence) {
            stopAllGrids();
            startGridsMultiThreaded(3);

            awaitPartitionMapExchange(true, true, null);

            assertEquals(EXIT_CODE_OK, execute("--port", connectorPort(grid(2)), "--cache", "idle_verify"));
            out = testOut.toString();

            Pattern regexCorrectCheck = Pattern.compile(CORRECT_VERIFY_MSG);
            boolean correctOut = regexCorrectCheck.matcher(out).find();

            // partVerHash are different, thus only regex check here
            String regexCheck = "Partition instances: \\[PartitionHashRecord" +
                ".*?consistentId=%s, updateCntr=\\[lwm=2, missed=\\[\\], hwm=2\\], partitionState=OWNING, size=1";
            Pattern part0Pattern = Pattern.compile(String.format(regexCheck, "gridCommandHandlerTest0"));
            Pattern part1Pattern = Pattern.compile(String.format(regexCheck, "gridCommandHandlerTest1"));
            Pattern part2Pattern = Pattern.compile(String.format(regexCheck, "gridCommandHandlerTest2"));

            boolean matches =
                part0Pattern.matcher(out).find() &&
                part1Pattern.matcher(out).find() &&
                part2Pattern.matcher(out).find();

            assertTrue(out, matches || correctOut);
        }
    }

    /** */
    private void sqlVisibilityCheck(List<Ignite> nodes, int keyToCheck, int referal) {
        for (Ignite node : nodes) {
            Object ret = node.compute(node.cluster().forLocal()).call(new IgniteCallable<>() {
                /** */
                @IgniteInstanceResource
                private Ignite instance;

                /** */
                @Override public Integer call() {
                    String selectSql = "SELECT VAL FROM " + DEFAULT_CACHE_NAME + " WHERE ID=" + keyToCheck;

                    List<List<?>> res = instance.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(selectSql)).getAll();

                    return (int)res.get(0).get(0);
                }
            });

            assertEquals("Unexpected result on node: " + node.name(), referal, ret);
        }
    }

    /** */
    private CacheConfiguration<Object, Object> createCache(String cacheName, boolean writeThrough) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(cacheName);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setCacheMode(CacheMode.REPLICATED);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setQueryEntities(List.of(queryEntity(cacheName)));

        if (writeThrough) {
            ccfg.setReadThrough(true);
            ccfg.setWriteThrough(true);
            ccfg.setCacheStoreFactory(MapCacheStore::new);
        }

        return ccfg;
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
    public static class MapCacheStore extends CacheStoreAdapter<Object, Object> {
        /** Store map. */
        private final Map<Object, Object> map = new ConcurrentHashMap<>();

        /** */
        private static CountDownLatch salvagedLatch;

        /** */
        private static CountDownLatch txCoordStoreLatch;

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Object, Object> clo, Object... args) {
            for (Map.Entry<Object, Object> e : map.entrySet())
                clo.apply(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public Object load(Object key) {
            Object val = map.get(key);

            if (salvagedLatch != null)
                salvagedLatch.countDown();

            return val;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> e) {
            map.put(e.getKey(), e.getValue());

            if (txCoordStoreLatch != null)
                txCoordStoreLatch.countDown();
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            map.remove(key);
        }
    }

    /** */
    private static class BeforeRecoveryListener implements DiscoveryEventListener, HighPriorityListener {
        /** {@inheritDoc} */
        @Override public void onEvent(DiscoveryEvent evt, DiscoCache discoCache) {
            U.awaitQuiet(MapCacheStore.txCoordStoreLatch);
        }

        /** {@inheritDoc} */
        @Override public int order() {
            return -1;
        }
    }
}
