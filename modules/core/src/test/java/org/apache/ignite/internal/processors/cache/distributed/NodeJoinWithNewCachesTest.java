/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests check correct behaviour of node(s) join when it has configured caches that not presented in cluster.
 */
@RunWith(Parameterized.class)
public class NodeJoinWithNewCachesTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameters(name = "Persistence enabled = {0}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        params.add(new Object[]{false});
        params.add(new Object[]{true});

        return params;
    }

    /** Initial nodes. */
    private static final int INITIAL_NODES = 2;

    /** Client mode indicator. */
    private boolean client;

    /** Persistence enabled. */
    @Parameterized.Parameter
    public boolean persistenceEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(256 * 1024 * 1024)
                    .setPersistenceEnabled(persistenceEnabled))
        );

        int cachesCnt = getTestIgniteInstanceIndex(igniteInstanceName) + 1;

        cfg.setCacheConfiguration(cacheConfiguration("cache-", cachesCnt));

        cfg.setClientMode(client);

        cfg.setActiveOnStart(false);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /**
     * @param prefix Prefix.
     * @param cachesNum Number.
     */
    private CacheConfiguration[] cacheConfiguration(String prefix, int cachesNum) {
        return cacheConfiguration(prefix, null, cachesNum);
    }

    /**
     * @param prefix Prefix.
     * @param grpName Group name.
     * @param cachesNum Caches number.
     */
    private CacheConfiguration[] cacheConfiguration(String prefix, String grpName, int cachesNum) {
        CacheConfiguration[] ccfgs = new CacheConfiguration[cachesNum + 1];

        for (int i = 0; i < cachesNum; i++) {
            ccfgs[i] = new CacheConfiguration(prefix + i)
                .setGroupName(grpName)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setBackups(1)
                .setAffinity(new RendezvousAffinityFunction(false, 32));
        }

        // Unique cache.
        ccfgs[cachesNum] = new CacheConfiguration("unique-" + (cachesNum - 1))
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setBackups(1)
            .setAffinity(new RendezvousAffinityFunction(false, 32));

        return ccfgs;
    }

    /**
     *
     */
    @Before
    public void before() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        IgniteEx crd = (IgniteEx) startGridsMultiThreaded(INITIAL_NODES);

        crd.cluster().active(true);

        awaitPartitionMapExchange();
    }

    /**
     *
     */
    @After
    public void after() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Test that server node join with new caches works correctly.
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testNodeJoin() throws Exception {
        startGrid(2);

        assertCachesWorking(INITIAL_NODES + 1);
    }

    /**
     * Test that multiple server nodes join simultaneously with new caches works correctly.
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testMultipleNodeJoin() throws Exception {
        startGridsMultiThreaded(INITIAL_NODES, 3);

        assertCachesWorking(INITIAL_NODES + 3);
    }

    /**
     * Test that multiple client nodes join simultaneously with new caches works correctly.
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testMultipleNodeJoinClient() throws Exception {
        client = true;

        startGridsMultiThreaded(INITIAL_NODES, 3);

        assertCachesWorking(INITIAL_NODES + 3);
    }

    /**
     * Test that new cache from client node works correctly
     * if exchange of that cache start is not processed on other nodes yet.
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testJoinAndStartTransaction() throws Exception {
        IgniteEx crd = grid(0);

        TestRecordingCommunicationSpi.spi(crd).blockMessages((node, msg) -> {
            // Delay exchange finish for unique-2 cache start on second node.
            if (msg instanceof GridDhtPartitionsFullMessage && (node.id().getLeastSignificantBits() & 0xFFFF) == 1) {
                GridDhtPartitionsFullMessage fullMsg = (GridDhtPartitionsFullMessage) msg;

                return fullMsg.exchangeId().topologyVersion().equals(new AffinityTopologyVersion(3, 1));
            }

            return false;
        });

        client = true;

        Ignite client = startGrid(2);

        final IgniteCache<Integer, Integer> txCache = client.cache("unique-" + 2);

        // Start transaction belongs to second node. This transaction should hang until cache on that node is started.
        IgniteInternalFuture txFut = GridTestUtils.runAsync(() -> {
            for (int i = 0; i < 32; ++i)
                txCache.put(i, i);
        });

        assertFalse(GridTestUtils.waitForCondition(txFut::isDone, 5_000));

        TestRecordingCommunicationSpi.spi(crd).stopBlock();

        // Cache should be started on second node after exchange resume and transaction should be completed.
        txFut.get();

        for (int i = 0; i < 32; ++i)
            assertEquals(Integer.valueOf(i), txCache.get(i));

        assertCachesWorking(INITIAL_NODES + 1);
    }

    /**
     * Assert that all configured caches are exist and operable.
     *
     * @param nodes Nodes count where caches are checked.
     */
    private void assertCachesWorking(int nodes) {
        for (int nodeId = nodes - 1; nodeId >= 0; nodeId--) {
            for (int cacheId = nodeId - 1; cacheId >= 0; cacheId--) {
                IgniteCache<Object, Object> cache = grid(nodeId).cache("cache-" + cacheId);

                Assert.assertNotNull(
                    String.format("Cache is null [nodeId=%d, cacheId=%d]", nodeId, cacheId),
                    cache
                );

                cache.put(0, 0);

                Assert.assertEquals(
                    String.format("Cache is not working as expected [nodeId=%d, cacheId=%d]", nodeId, cacheId),
                    0,
                    cache.get(0)
                );
            }

            IgniteCache<Object, Object> cache = grid(nodeId).cache("unique-" + nodeId);

            Assert.assertNotNull(
                String.format("Unique cache is null [nodeId=%d, cacheId=%d]", nodeId, nodeId),
                cache
            );

            cache.put(0, 0);

            Assert.assertEquals(
                String.format("Unique cache is not working as expected [nodeId=%d, cacheId=%d]", nodeId, nodeId),
                0,
                cache.get(0)
            );
        }
    }
}
