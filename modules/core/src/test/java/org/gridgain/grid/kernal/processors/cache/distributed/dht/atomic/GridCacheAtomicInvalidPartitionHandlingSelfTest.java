/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht.atomic;

import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.communication.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;
import org.jdk8.backport.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicWriteOrderMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Test GridDhtInvalidPartitionException handling in ATOMIC cache during restarts.
 */
@SuppressWarnings("ErrorNotRethrown")
public class GridCacheAtomicInvalidPartitionHandlingSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Delay flag. */
    private static volatile boolean delay;

    /** Write order. */
    private GridCacheAtomicWriteOrderMode writeOrder;

    /** Write sync. */
    private GridCacheWriteSynchronizationMode writeSync;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setCacheConfiguration(cacheConfiguration());

        cfg.setCommunicationSpi(new DelayCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    protected GridCacheConfiguration cacheConfiguration() {
        GridCacheConfiguration ccfg = new GridCacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);

        ccfg.setBackups(1);
        ccfg.setStoreValueBytes(false);
        ccfg.setAtomicWriteOrderMode(writeOrder);
        ccfg.setWriteSynchronizationMode(writeSync);

        ccfg.setPreloadMode(SYNC);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        delay = false;
    }

    /**
     * @throws Exception If failed.
     */
    public void testClockFullSync() throws Exception {
        checkRestarts(CLOCK, FULL_SYNC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClockPrimarySync() throws Exception {
        checkRestarts(CLOCK, PRIMARY_SYNC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClockFullAsync() throws Exception {
        checkRestarts(CLOCK, FULL_ASYNC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryFullSync() throws Exception {
        checkRestarts(PRIMARY, FULL_SYNC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryPrimarySync() throws Exception {
        checkRestarts(PRIMARY, PRIMARY_SYNC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryFullAsync() throws Exception {
        checkRestarts(PRIMARY, FULL_ASYNC);
    }

    /**
     * @param writeOrder Write order to check.
     * @param writeSync Write synchronization mode to check.
     * @throws Exception If failed.
     */
    private void checkRestarts(GridCacheAtomicWriteOrderMode writeOrder, GridCacheWriteSynchronizationMode writeSync)
        throws Exception {
        this.writeOrder = writeOrder;
        this.writeSync = writeSync;

        int gridCnt = 6;

        startGrids(gridCnt);

        try {
            final GridCache<Object, Object> cache = grid(0).cache(null);

            final int range = 100_000;

            for (int i = 0; i < range; i++) {
                cache.put(i, 0);

                if (i > 0 && i % 10_000 == 0)
                    System.err.println("Put: " + i);
            }

            final AtomicBoolean done = new AtomicBoolean();

            delay = true;

            System.err.println("FINISHED PUTS");

            // Start put threads.
            GridFuture<?> fut = multithreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Random rnd = new Random();

                    while (!done.get()) {
                        try {
                            int cnt = rnd.nextInt(5);

                            if (cnt < 2) {
                                int key = rnd.nextInt(range);

                                int val = rnd.nextInt();

                                cache.put(key, val);
                            }
                            else {
                                Map<Integer, Integer> upd = new TreeMap<>();

                                for (int i = 0; i < cnt; i++)
                                    upd.put(rnd.nextInt(range), rnd.nextInt());

                                cache.putAll(upd);
                            }
                        }
                        catch (GridCachePartialUpdateException ignored) {
                            // No-op.
                        }
                    }

                    return null;
                }
            }, 4);

            Random rnd = new Random();

            // Restart random nodes.
            for (int r = 0; r < 20; r++) {
                int idx0 = rnd.nextInt(gridCnt - 1) + 1;

                stopGrid(idx0);

                U.sleep(200);

                startGrid(idx0);
            }

            done.set(true);

            awaitPartitionMapExchange();

            fut.get();

            for (int k = 0; k < range; k++) {
                Collection<ClusterNode> affNodes = cache.affinity().mapKeyToPrimaryAndBackups(k);

                // Test is valid with at least one backup.
                assert affNodes.size() >= 2;

                Object val = null;
                GridCacheVersion ver = null;
                UUID nodeId = null;

                for (int i = 0; i < gridCnt; i++) {
                    ClusterNode locNode = grid(i).localNode();

                    GridCacheAdapter<Object, Object> c = ((GridKernal)grid(i)).internalCache();

                    GridCacheEntryEx<Object, Object> entry = c.peekEx(k);

                    for (int r = 0; r < 3; r++) {
                        try {
                            if (affNodes.contains(locNode)) {
                                assert c.affinity().isPrimaryOrBackup(locNode, k);

                                boolean primary = c.affinity().isPrimary(locNode, k);

                                assertNotNull("Failed to find entry on node for key [locNode=" + locNode.id() +
                                    ", key=" + k + ']', entry);

                                if (val == null) {
                                    assertNull(ver);

                                    val = entry.rawGetOrUnmarshal(false);
                                    ver = entry.version();
                                    nodeId = locNode.id();
                                }
                                else {
                                    assertNotNull(ver);

                                    assertEquals("Failed to check value for key [key=" + k + ", node=" +
                                        locNode.id() + ", primary=" + primary + ", recNodeId=" + nodeId + ']',
                                        val, entry.rawGetOrUnmarshal(false));
                                    assertEquals(ver, entry.version());
                                }
                            }
                            else
                                assertTrue("Invalid entry: " + entry, entry == null || !entry.partitionValid());
                        }
                        catch (AssertionError e) {
                            if (r == 2)
                                throw e;

                            System.err.println("Failed to verify cache contents: " + e.getMessage());

                            // Give some time to finish async updates.
                            U.sleep(1000);
                        }
                    }
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private static class DelayCommunicationSpi extends GridTcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, GridTcpCommunicationMessageAdapter msg)
            throws GridSpiException {
            try {
                if (delayMessage((GridIoMessage)msg))
                    U.sleep(ThreadLocalRandom8.current().nextInt(250) + 1);
            }
            catch (GridInterruptedException e) {
                throw new GridSpiException(e);
            }

            super.sendMessage(node, msg);
        }

        /**
         * Checks if message should be delayed.
         *
         * @param msg Message to check.
         * @return {@code True} if message should be delayed.
         */
        private boolean delayMessage(GridIoMessage msg) {
            Object origMsg = msg.message();

            return delay &&
                ((origMsg instanceof GridNearAtomicUpdateRequest) || (origMsg instanceof GridDhtAtomicUpdateRequest));
        }
    }
}
