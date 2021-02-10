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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.managers.eventstorage.HighPriorityListener;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishResponse;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionHeuristicException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishFuture.ALL_PARTITION_OWNERS_LEFT_GRID_MSG;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.mvccEnabled;

/**
 * Tests check a result of commit when a node fail before send {@link GridNearTxFinishResponse} to transaction
 * coordinator
 */
@RunWith(Parameterized.class)
public class IgniteTxExceptionNodeFailTest extends GridCommonAbstractTest {
    /** Client node name. */
    private static final String CLIENT = "client";

    /** Node leave events for discovery event listener. */
    private static final int[] TYPES = {EVT_NODE_LEFT, EVT_NODE_FAILED};

    /** Parameters. */
    @Parameterized.Parameters(name = "syncMode={0}")
    public static Iterable<CacheWriteSynchronizationMode> data() {
        return Arrays.asList(PRIMARY_SYNC, FULL_SYNC);
    }

    /** syncMode */
    @Parameterized.Parameter()
    public CacheWriteSynchronizationMode syncMode;

    /** Amount backups for cache. */
    public int backups = 0;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(100L * 1024 * 1024)
                    .setPersistenceEnabled(true)))
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setWriteSynchronizationMode(syncMode)
                .setBackups(backups));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * <ul>
     * <li>Start 2 nodes with transactional cache, without backups, with {@link IgniteTxExceptionNodeFailTest#syncMode}
     * <li>Start transaction:
     *  <ul>
     *  <li>put a key to a partition on transaction coordinator
     *  <li>put a key to a partition on other node
     *  <li>try to commit the transaction
     *  </ul>
     * <li>Stop other node when it try to send GridNearTxFinishResponse
     * <li>Check that {@link Transaction#commit()} throw {@link TransactionHeuristicException}
     * </ul>
     *
     * @throws Exception If failed
     */
    @Test
    public void testNodeFailBeforeSendGridNearTxFinishResponse() throws Exception {
        IgniteEx grid0 = startGrids(2);

        grid0.cluster().state(ClusterState.ACTIVE);

        IgniteEx grid1 = grid(1);

        int key0 = primaryKey(grid0.cache(DEFAULT_CACHE_NAME));
        int key1 = primaryKey(grid1.cache(DEFAULT_CACHE_NAME));

        Affinity<Object> aff = grid1.affinity(DEFAULT_CACHE_NAME);

        assertFalse(
            "Keys have the same mapping [key0=" + key0 + ", key1=" + key1 + ']',
            aff.mapKeyToNode(key0).equals(aff.mapKeyToNode(key1))
        );

        spi(grid0).blockMessages(GridNearTxFinishResponse.class, getTestIgniteInstanceName(1));

        IgniteInternalFuture stopNodeFut = GridTestUtils.runAsync(() -> {
                try {
                    spi(grid0).waitForBlocked();
                }
                catch (InterruptedException e) {
                    log.error("Waiting is interrupted.", e);
                }

                info("Stopping node: [" + grid0.name() + ']');

                grid0.close();

            },
            "node-stopper"
        );

        try (Transaction tx = grid1.transactions().txStart()) {
            grid1.cache(DEFAULT_CACHE_NAME).put(key0, 100);
            grid1.cache(DEFAULT_CACHE_NAME).put(key1, 200);

            tx.commit();

            fail("Transaction passed, but no one partition is alive.");

        }
        catch (Exception e) {
            assertTrue(X.hasCause(e, CacheInvalidStateException.class));

            String msg = e.getMessage();

            assertTrue(msg.contains(ALL_PARTITION_OWNERS_LEFT_GRID_MSG));

            if (!mvccEnabled(grid1.context())) {
                Pattern msgPtrn;

                msgPtrn = Pattern.compile(" \\[cacheName=" + DEFAULT_CACHE_NAME +
                    ", partition=\\d+, " +
                    "key=KeyCacheObjectImpl \\[part=\\d+, val=" + key0 +
                    ", hasValBytes=true\\]\\]");

                Matcher matcher = msgPtrn.matcher(msg);

                assertTrue("Message does not match: [msg=" + msg + ']', matcher.find());
            }
        }

        stopNodeFut.get(10_000);
    }

    /**
     * Test checks the all node leave detector when cache has backups enough.
     *
     * @throws Exception If failed.
     */
    @Test
    public void cacheWithBackups() throws Exception {
        backups = 2;

        IgniteEx ignite0 = startGrids(3);

        ignite0.cluster().state(ClusterState.ACTIVE);

        IgniteEx client = startClientGrid(CLIENT);

        awaitPartitionMapExchange();

        int key = primaryKey(ignite(1).cache(DEFAULT_CACHE_NAME));

        spi(ignite(1)).blockMessages(GridNearTxFinishResponse.class, CLIENT);

        spi(ignite(2)).blockMessages(GridDhtTxFinishResponse.class, CLIENT);

        new TestDiscoveryNodeLeftListener(CLIENT);

        IgniteInternalFuture stopNodeFut = GridTestUtils.runAsync(() -> {
                try {
                    spi(ignite(1)).waitForBlocked();
                }
                catch (InterruptedException e) {
                    log.error("Waiting is interrupted.", e);
                }

                info("Stopping node: [" + ignite(2).name() + ']');

                ignite(2).close();

            },
            "node-stopper"
        );

        try (Transaction tx = client.transactions().txStart()) {
            client.cache(DEFAULT_CACHE_NAME).put(key, 100);

            tx.commit();
        }
        catch (Exception e) {
            log.error("Transaction was not committed.", e);

            fail("Transaction should be committed while at last one owner present [err=" + e.getMessage() + ']');
        }

        assertEquals(100, client.cache(DEFAULT_CACHE_NAME).get(key));

        stopNodeFut.get(10_000);
    }

    /**
     * A test discovery listener to freeze handling node left events.
     */
    private class TestDiscoveryNodeLeftListener implements DiscoveryEventListener, HighPriorityListener {
        /** Name node to subscribe listener. */
        private final String nodeToSubscribe;

        /**
         * @param nodeToSubscribe Node to subscribe.
         */
        public TestDiscoveryNodeLeftListener(String nodeToSubscribe) {
            this.nodeToSubscribe = nodeToSubscribe;

            grid(nodeToSubscribe).context().event().addDiscoveryEventListener(this, TYPES);
        }

        /** {@inheritDoc} */
        @Override public void onEvent(DiscoveryEvent evt, DiscoCache discoCache) {
            info("Stopping node: [" + ignite(1).name() + ']');

            ignite(1).close();

            grid(nodeToSubscribe).context().event().removeDiscoveryEventListener(this, TYPES);
        }

        /** {@inheritDoc} */
        @Override public int order() {
            return 0;
        }
    }
}
