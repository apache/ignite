/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.communication.tcp.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.testframework.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;

/**
 * Abstract test for originating node failure.
 */
public abstract class GridCacheTxPessimisticOriginatingNodeFailureAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** */
    protected static final int GRID_CNT = 5;

    /** Ignore node ID. */
    private volatile Collection<UUID> ignoreMsgNodeIds;

    /** Ignore message class. */
    private Collection<Class<?>> ignoreMsgCls;

    /** Failing node ID. */
    private UUID failingNodeId;

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
     * @throws Exception If failed.
     */
    public void testPrimaryNodeFailureCommit() throws Exception {
        checkPrimaryNodeCrash(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryNodeFailureRollback() throws Exception {
        checkPrimaryNodeCrash(false);
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
     * @param dstNodeIds Destination node IDs.
     * @param msgCls Message type.
     */
    protected void ignoreMessages(Collection<Class<?>> msgCls, Collection<UUID> dstNodeIds) {
        ignoreMsgNodeIds = dstNodeIds;
        ignoreMsgCls = msgCls;
    }

    /**
     * Gets ignore message class to simulate partial prepare message.
     *
     * @return Ignore message class.
     */
    protected abstract Collection<Class<?>> ignoreMessageClasses();

    /**
     * @param keys Keys to update.
     * @param fullFailure Flag indicating whether to simulate rollback state.
     * @throws Exception If failed.
     */
    protected void testTxOriginatingNodeFails(Collection<Integer> keys, final boolean fullFailure) throws Exception {
        assertFalse(keys.isEmpty());

        final Collection<GridKernal> grids = new ArrayList<>();

        GridNode txNode = grid(originatingNode()).localNode();

        for (int i = 1; i < gridCount(); i++)
            grids.add((GridKernal)grid(i));

        failingNodeId = grid(0).localNode().id();

        final Map<Integer, String> map = new HashMap<>();

        final String initVal = "initialValue";

        for (Integer key : keys) {
            grid(originatingNode()).cache(null).put(key, initVal);

            map.put(key, String.valueOf(key));
        }

        Map<Integer, Collection<GridNode>> nodeMap = new HashMap<>();

        GridCacheAdapter<Integer, String> cache = ((GridKernal)grid(1)).internalCache();

        info("Node being checked: " + grid(1).localNode().id());

        for (Integer key : keys) {
            Collection<GridNode> nodes = new ArrayList<>();

            nodes.addAll(cache.affinity().mapKeyToPrimaryAndBackups(key));

            nodes.remove(txNode);

            nodeMap.put(key, nodes);
        }

        info("Starting tx [values=" + map + ", topVer=" +
            ((GridKernal)grid(1)).context().discovery().topologyVersion() + ']');

        if (fullFailure)
            ignoreMessages(ignoreMessageClasses(), allNodeIds());
        else
            ignoreMessages(ignoreMessageClasses(), F.asList(grid(1).localNode().id()));

        final GridEx originatingNodeGrid = grid(originatingNode());

        GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                GridCache<Integer, String> cache = originatingNodeGrid.cache(null);

                assertNotNull(cache);

                GridCacheTx tx = cache.txStart();

                try {
                    cache.putAll(map);

                    info("Before commitAsync");

                    GridFuture<GridCacheTx> fut = tx.commitAsync();

                    info("Got future for commitAsync().");

                    fut.get(3, TimeUnit.SECONDS);
                }
                catch (GridFutureTimeoutException ignored) {
                    info("Failed to wait for commit future completion [fullFailure=" + fullFailure + ']');
                }

                return null;
            }
        }).get();

        info(">>> Stopping originating node " + txNode);

        G.stop(grid(originatingNode()).name(), true);

        ignoreMessages(Collections.<Class<?>>emptyList(), Collections.<UUID>emptyList());

        info(">>> Stopped originating node: " + txNode.id());

        boolean txFinished = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (GridKernal g : grids) {
                    GridCacheAdapter<?, ?> cache = g.internalCache();

                    GridCacheTxManager txMgr = cache.isNear() ?
                        ((GridNearCacheAdapter)cache).dht().context().tm() :
                        cache.context().tm();

                    int txNum = txMgr.idMapSize();

                    if (txNum != 0)
                        return false;
                }

                return true;
            }
        }, 10000);

        assertTrue(txFinished);

        info("Transactions finished.");

        for (Map.Entry<Integer, Collection<GridNode>> e : nodeMap.entrySet()) {
            final Integer key = e.getKey();

            final String val = map.get(key);

            assertFalse(e.getValue().isEmpty());

            for (GridNode node : e.getValue()) {
                final UUID checkNodeId = node.id();

                compute(G.grid(checkNodeId).cluster().forNode(node)).call(new Callable<Void>() {
                    /** */
                    @GridInstanceResource
                    private Grid grid;

                    @Override public Void call() throws Exception {
                        GridCache<Integer, String> cache = grid.cache(null);

                        assertNotNull(cache);

                        assertEquals("Failed to check entry value on node: " + checkNodeId,
                            fullFailure ? initVal : val, cache.peek(key));

                        return null;
                    }
                });
            }
        }

        for (Map.Entry<Integer, String> e : map.entrySet()) {
            for (Grid g : G.allGrids())
                assertEquals(fullFailure ? initVal : e.getValue(), g.cache(null).get(e.getKey()));
        }
    }

    /**
     * Checks tx data consistency in case when primary node crashes.
     *
     * @param commmit Whether to commit or rollback a transaction.
     * @throws Exception If failed.
     */
    private void checkPrimaryNodeCrash(final boolean commmit) throws Exception {
        Collection<Integer> keys = new ArrayList<>(20);

        for (int i = 0; i < 20; i++)
            keys.add(i);

        final Collection<GridKernal> grids = new ArrayList<>();

        GridNode primaryNode = grid(1).localNode();

        for (int i = 0; i < gridCount(); i++) {
            if (i != 1)
                grids.add((GridKernal)grid(i));
        }

        failingNodeId = primaryNode.id();

        final Map<Integer, String> map = new HashMap<>();

        final String initVal = "initialValue";

        for (Integer key : keys) {
            grid(originatingNode()).cache(null).put(key, initVal);

            map.put(key, String.valueOf(key));
        }

        Map<Integer, Collection<GridNode>> nodeMap = new HashMap<>();

        GridCache<Integer, String> cache = grid(0).cache(null);

        info("Failing node ID: " + grid(1).localNode().id());

        for (Integer key : keys) {
            Collection<GridNode> nodes = new ArrayList<>();

            nodes.addAll(cache.affinity().mapKeyToPrimaryAndBackups(key));

            nodes.remove(primaryNode);

            nodeMap.put(key, nodes);
        }

        info("Starting tx [values=" + map + ", topVer=" +
            ((GridKernal)grid(1)).context().discovery().topologyVersion() + ']');

        assertNotNull(cache);

        try (GridCacheTx tx = cache.txStart()) {
            cache.getAll(keys);

            // Should not send any messages.
            cache.putAll(map);

            // Fail the node in the middle of transaction.
            info(">>> Stopping primary node " + primaryNode);

            G.stop(G.grid(primaryNode.id()).name(), true);

            info(">>> Stopped originating node, finishing transaction: " + primaryNode.id());

            if (commmit)
                tx.commit();
            else
                tx.rollback();
        }

        boolean txFinished = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (GridKernal g : grids) {
                    GridCacheAdapter<?, ?> cache = g.internalCache();

                    GridCacheTxManager txMgr = cache.isNear() ?
                        ((GridNearCacheAdapter)cache).dht().context().tm() :
                        cache.context().tm();

                    int txNum = txMgr.idMapSize();

                    if (txNum != 0)
                        return false;
                }

                return true;
            }
        }, 10000);

        assertTrue(txFinished);

        info("Transactions finished.");

        for (Map.Entry<Integer, Collection<GridNode>> e : nodeMap.entrySet()) {
            final Integer key = e.getKey();

            final String val = map.get(key);

            assertFalse(e.getValue().isEmpty());

            for (GridNode node : e.getValue()) {
                final UUID checkNodeId = node.id();

                compute(G.grid(checkNodeId).cluster().forNode(node)).call(new Callable<Void>() {
                    /** */
                    @GridInstanceResource
                    private Grid grid;

                    @Override public Void call() throws Exception {
                        GridCache<Integer, String> cache = grid.cache(null);

                        assertNotNull(cache);

                        assertEquals("Failed to check entry value on node: " + checkNodeId,
                            !commmit ? initVal : val, cache.peek(key));

                        return null;
                    }
                });
            }
        }

        for (Map.Entry<Integer, String> e : map.entrySet()) {
            for (Grid g : G.allGrids())
                assertEquals(!commmit ? initVal : e.getValue(), g.cache(null).get(e.getKey()));
        }
    }

    /**
     * @return All node IDs.
     */
    private Collection<UUID> allNodeIds() {
        Collection<UUID> nodeIds = new ArrayList<>(gridCount());

        for (int i = 0; i < gridCount(); i++)
            nodeIds.add(grid(i).localNode().id());

        return nodeIds;
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCommunicationSpi(new GridTcpCommunicationSpi() {
            @Override public void sendMessage(GridNode node, GridTcpCommunicationMessageAdapter msg)
                throws GridSpiException {
                if (getSpiContext().localNode().id().equals(failingNodeId)) {
                    if (ignoredMessage((GridIoMessage)msg) && ignoreMsgNodeIds != null) {
                        for (UUID ignored : ignoreMsgNodeIds) {
                            if (node.id().equals(ignored))
                                return;
                        }
                    }
                }

                super.sendMessage(node, msg);
            }
        });

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setStore(null);
        cfg.setDefaultTxConcurrency(PESSIMISTIC);
        cfg.setDgcFrequency(0);

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
        ignoreMsgNodeIds = null;
    }

    /**
     * Checks if message should be ignored.
     *
     * @param msg Message.
     * @return {@code True} if message should be ignored.
     */
    private boolean ignoredMessage(GridIoMessage msg) {
        Collection<Class<?>> ignoreClss = ignoreMsgCls;

        if (ignoreClss != null) {
            for (Class<?> ignoreCls : ignoreClss) {
                if (ignoreCls.isAssignableFrom(msg.message().getClass()))
                    return true;
            }

            return false;
        }
        else
            return false;
    }
}
