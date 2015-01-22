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

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.gridgain.grid.kernal.processors.cache.transactions.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.testframework.*;

import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.transactions.IgniteTxConcurrency.*;

/**
 * Abstract test for originating node failure.
 */
public abstract class IgniteTxOriginatingNodeFailureAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** */
    protected static final int GRID_CNT = 5;

    /** Ignore node ID. */
    private volatile UUID ignoreMsgNodeId;

    /** Ignore message class. */
    private Class<?> ignoreMsgCls;

    /**
     * @throws Exception If failed.
     */
    public void testManyKeysCommit() throws Exception {
        Collection<Integer> keys = new ArrayList<>(200);

        for (int i = 0; i < 200; i++)
            keys.add(i);

        testTxOriginatingNodeFails(keys, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testManyKeysRollback() throws Exception {
        Collection<Integer> keys = new ArrayList<>(200);

        for (int i = 0; i < 200; i++)
            keys.add(i);

        testTxOriginatingNodeFails(keys, true);
    }

    /**
     * @return Index of node starting transaction.
     */
    protected int originatingNode() {
        return 0;
    }

    /**
     * Ignores messages to given node of given type.
     *
     * @param dstNodeId Destination node ID.
     * @param msgCls Message type.
     */
    protected void ignoreMessages(UUID dstNodeId, Class<?> msgCls) {
        ignoreMsgNodeId = dstNodeId;
        ignoreMsgCls = msgCls;
    }

    /**
     * Gets ignore message class to simulate partial prepare message.
     *
     * @return Ignore message class.
     */
    protected abstract Class<?> ignoreMessageClass();

    /**
     * @param keys Keys to update.
     * @param partial Flag indicating whether to simulate partial prepared state.
     * @throws Exception If failed.
     */
    protected void testTxOriginatingNodeFails(Collection<Integer> keys, final boolean partial) throws Exception {
        assertFalse(keys.isEmpty());

        final Collection<GridKernal> grids = new ArrayList<>();

        ClusterNode txNode = grid(originatingNode()).localNode();

        for (int i = 1; i < gridCount(); i++)
            grids.add((GridKernal)grid(i));

        final Map<Integer, String> map = new HashMap<>();

        final String initVal = "initialValue";

        for (Integer key : keys) {
            grid(originatingNode()).cache(null).put(key, initVal);

            map.put(key, String.valueOf(key));
        }

        Map<Integer, Collection<ClusterNode>> nodeMap = new HashMap<>();

        GridCacheAdapter<Integer, String> cache = ((GridKernal)grid(1)).internalCache();

        info("Node being checked: " + grid(1).localNode().id());

        for (Integer key : keys) {
            Collection<ClusterNode> nodes = new ArrayList<>();

            nodes.addAll(cache.affinity().mapKeyToPrimaryAndBackups(key));

            nodes.remove(txNode);

            nodeMap.put(key, nodes);
        }

        info("Starting tx [values=" + map + ", topVer=" +
            ((GridKernal)grid(1)).context().discovery().topologyVersion() + ']');

        if (partial)
            ignoreMessages(grid(1).localNode().id(), ignoreMessageClass());

        final Ignite txIgniteNode = G.ignite(txNode.id());

        GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                GridCache<Integer, String> cache = txIgniteNode.cache(null);

                assertNotNull(cache);

                IgniteTxProxyImpl tx = (IgniteTxProxyImpl)cache.txStart();

                IgniteTxEx txEx = GridTestUtils.getFieldValue(tx, "tx");

                cache.putAll(map);

                try {
                    txEx.prepareAsync().get(3, TimeUnit.SECONDS);
                }
                catch (IgniteFutureTimeoutException ignored) {
                    info("Failed to wait for prepare future completion: " + partial);
                }

                return null;
            }
        }).get();

        info("Stopping originating node " + txNode);

        G.stop(G.ignite(txNode.id()).name(), true);

        info("Stopped grid, waiting for transactions to complete.");

        boolean txFinished = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (GridKernal g : grids) {
                    GridCacheSharedContext<Object, Object> ctx = g.context().cache().context();

                    int txNum = ctx.tm().idMapSize();

                    if (txNum != 0)
                        return false;
                }

                return true;
            }
        }, 10000);

        assertTrue(txFinished);

        info("Transactions finished.");

        for (Map.Entry<Integer, Collection<ClusterNode>> e : nodeMap.entrySet()) {
            final Integer key = e.getKey();

            final String val = map.get(key);

            assertFalse(e.getValue().isEmpty());

            for (ClusterNode node : e.getValue()) {
                compute(G.ignite(node.id()).cluster().forNode(node)).call(new Callable<Void>() {
                    /** */
                    @IgniteInstanceResource
                    private Ignite ignite;

                    @Override public Void call() throws Exception {
                        GridCache<Integer, String> cache = ignite.cache(null);

                        assertNotNull(cache);

                        assertEquals(partial ? initVal : val, cache.peek(key));

                        return null;
                    }
                });
            }
        }

        for (Map.Entry<Integer, String> e : map.entrySet()) {
            for (Ignite g : G.allGrids()) {
                UUID locNodeId = g.cluster().localNode().id();

                assertEquals("Check failed for node: " + locNodeId, partial ? initVal : e.getValue(),
                    g.cache(null).get(e.getKey()));
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCommunicationSpi(new TcpCommunicationSpi() {
            @Override public void sendMessage(ClusterNode node, GridTcpCommunicationMessageAdapter msg)
                throws IgniteSpiException {
                if (!F.eq(ignoreMsgNodeId, node.id()) || !ignoredMessage((GridIoMessage)msg))
                    super.sendMessage(node, msg);
            }
        });

        cfg.getTransactionsConfiguration().setDefaultTxConcurrency(OPTIMISTIC);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setCacheStoreFactory(null);
        cfg.setReadThrough(false);
        cfg.setWriteThrough(false);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected abstract GridCacheMode cacheMode();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        ignoreMsgCls = null;
        ignoreMsgNodeId = null;
    }

    /**
     * Checks if message should be ignored.
     *
     * @param msg Message.
     * @return {@code True} if message should be ignored.
     */
    private boolean ignoredMessage(GridIoMessage msg) {
        return ignoreMsgCls != null && ignoreMsgCls.isAssignableFrom(msg.message().getClass());
    }
}
