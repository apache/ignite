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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CachePartialUpdateException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.ThreadLocalRandom8;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.CLOCK;
import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.TestMemoryMode;

/**
 * Test GridDhtInvalidPartitionException handling in ATOMIC cache during restarts.
 */
@SuppressWarnings("ErrorNotRethrown")
public class GridCacheAtomicInvalidPartitionHandlingSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Delay flag. */
    private static volatile boolean delay;

    /** Write order. */
    private CacheAtomicWriteOrderMode writeOrder;

    /** Write sync. */
    private CacheWriteSynchronizationMode writeSync;

    /** Memory mode. */
    private TestMemoryMode memMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER).setForceServerMode(true));

        CacheConfiguration ccfg = cacheConfiguration();

        cfg.setCacheConfiguration(ccfg);

        DelayCommunicationSpi spi = new DelayCommunicationSpi();

        spi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(spi);

        if (testClientNode() && getTestGridName(0).equals(gridName))
            cfg.setClientMode(true);

        GridTestUtils.setMemoryMode(cfg, ccfg, memMode, 100, 1024);

        return cfg;
    }

    /** {@inheritDoc} */
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);

        ccfg.setBackups(1);
        ccfg.setAtomicWriteOrderMode(writeOrder);
        ccfg.setWriteSynchronizationMode(writeSync);

        ccfg.setRebalanceMode(SYNC);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        delay = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @return {@code True} if test updates from client node.
     */
    protected boolean testClientNode() {
        return false;
    }

    /**
     * @throws Exception If failed.
     */
    public void testClockFullSync() throws Exception {
        checkRestarts(CLOCK, FULL_SYNC, TestMemoryMode.HEAP);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClockFullSyncSwap() throws Exception {
        checkRestarts(CLOCK, FULL_SYNC, TestMemoryMode.SWAP);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClockFullSyncOffheapTiered() throws Exception {
        checkRestarts(CLOCK, FULL_SYNC, TestMemoryMode.OFFHEAP_TIERED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClockFullSyncOffheapSwap() throws Exception {
        checkRestarts(CLOCK, FULL_SYNC, TestMemoryMode.OFFHEAP_EVICT_SWAP);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClockPrimarySync() throws Exception {
        checkRestarts(CLOCK, PRIMARY_SYNC, TestMemoryMode.HEAP);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClockFullAsync() throws Exception {
        checkRestarts(CLOCK, FULL_ASYNC, TestMemoryMode.HEAP);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryFullSync() throws Exception {
        checkRestarts(PRIMARY, FULL_SYNC, TestMemoryMode.HEAP);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryFullSyncSwap() throws Exception {
        checkRestarts(PRIMARY, FULL_SYNC, TestMemoryMode.SWAP);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryFullSyncOffheapTiered() throws Exception {
        checkRestarts(PRIMARY, FULL_SYNC, TestMemoryMode.OFFHEAP_TIERED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryFullSyncOffheapSwap() throws Exception {
        checkRestarts(PRIMARY, FULL_SYNC, TestMemoryMode.OFFHEAP_EVICT_SWAP);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryPrimarySync() throws Exception {
        checkRestarts(PRIMARY, PRIMARY_SYNC, TestMemoryMode.HEAP);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryFullAsync() throws Exception {
        checkRestarts(PRIMARY, FULL_ASYNC, TestMemoryMode.HEAP);
    }

    /**
     * @param writeOrder Write order to check.
     * @param writeSync Write synchronization mode to check.
     * @param memMode Memory mode.
     * @throws Exception If failed.
     */
    private void checkRestarts(CacheAtomicWriteOrderMode writeOrder,
        CacheWriteSynchronizationMode writeSync,
        TestMemoryMode memMode)
        throws Exception {
        this.writeOrder = writeOrder;
        this.writeSync = writeSync;
        this.memMode = memMode;

        final int gridCnt = 6;

        startGrids(gridCnt);

        awaitPartitionMapExchange();

        try {
            assertEquals(testClientNode(), (boolean)grid(0).configuration().isClientMode());

            final IgniteCache<Object, Object> cache = grid(0).cache(null);

            final int range = 100_000;

            final Set<Integer> keys = new LinkedHashSet<>();

            try (IgniteDataStreamer<Integer, Integer> streamer = grid(0).dataStreamer(null)) {
                streamer.allowOverwrite(true);

                for (int i = 0; i < range; i++) {
                    streamer.addData(i, 0);

                    keys.add(i);

                    if (i > 0 && i % 10_000 == 0)
                        System.err.println("Put: " + i);
                }
            }

            final Affinity<Integer> aff = grid(0).affinity(null);

            boolean putDone = GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    Iterator<Integer> it = keys.iterator();

                    while (it.hasNext()) {
                        Integer key = it.next();

                        Collection<ClusterNode> affNodes = aff.mapKeyToPrimaryAndBackups(key);

                        for (int i = 0; i < gridCnt; i++) {
                            ClusterNode locNode = grid(i).localNode();

                            IgniteCache<Object, Object> cache = grid(i).cache(null);

                            Object val = cache.localPeek(key);

                            if (affNodes.contains(locNode)) {
                                if (val == null)
                                    return false;
                            }
                            else
                                assertNull(val);
                        }

                        it.remove();
                    }

                    return true;
                }
            }, 30_000);

            assertTrue(putDone);
            assertTrue(keys.isEmpty());

            final AtomicBoolean done = new AtomicBoolean();

            delay = true;

            System.err.println("FINISHED PUTS");

            // Start put threads.
            IgniteInternalFuture<?> fut = multithreadedAsync(new Callable<Object>() {
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
                        catch (CachePartialUpdateException ignored) {
                            // No-op.
                        }
                    }

                    return null;
                }
            }, 4, "putAll-thread");

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
                Collection<ClusterNode> affNodes = affinity(cache).mapKeyToPrimaryAndBackups(k);

                // Test is valid with at least one backup.
                assert affNodes.size() >= 2;

                Object val = null;
                GridCacheVersion ver = null;
                UUID nodeId = null;

                for (int i = 0; i < gridCnt; i++) {
                    ClusterNode locNode = grid(i).localNode();

                    GridCacheAdapter<Object, Object> c = ((IgniteKernal)grid(i)).internalCache();

                    GridCacheEntryEx entry = null;

                    if (memMode == TestMemoryMode.HEAP)
                        entry = c.peekEx(k);
                    else {
                        try {
                            entry = c.entryEx(k);

                            entry.unswap();
                        }
                        catch (GridDhtInvalidPartitionException ignored) {
                            // Skip key.
                        }
                    }

                    for (int r = 0; r < 10; r++) {
                        try {
                            if (affNodes.contains(locNode)) {
                                assert c.affinity().isPrimaryOrBackup(locNode, k);

                                boolean primary = c.affinity().isPrimary(locNode, k);

                                assertNotNull("Failed to find entry on node for key [locNode=" + locNode.id() +
                                    ", key=" + k + ']', entry);

                                if (val == null) {
                                    assertNull(ver);

                                    val = CU.value(entry.rawGetOrUnmarshal(false), entry.context(), false);
                                    ver = entry.version();
                                    nodeId = locNode.id();
                                }
                                else {
                                    assertNotNull(ver);

                                    assertEquals("Failed to check value for key [key=" + k + ", node=" +
                                        locNode.id() + ", primary=" + primary + ", recNodeId=" + nodeId + ']',
                                        val, CU.value(entry.rawGetOrUnmarshal(false), entry.context(), false));

                                    assertEquals("Failed to check version for key [key=" + k + ", node=" +
                                        locNode.id() + ", primary=" + primary + ", recNodeId=" + nodeId + ']',
                                        ver, entry.version());
                                }
                            }
                            else
                                assertTrue("Invalid entry: " + entry, entry == null || !entry.partitionValid());
                        }
                        catch (AssertionError e) {
                            if (r == 9) {
                                info("Failed to verify cache contents: " + e.getMessage());

                                throw e;
                            }

                            info("Failed to verify cache contents, will retry: " + e.getMessage());

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
    private static class DelayCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            try {
                if (delayMessage((GridIoMessage)msg))
                    U.sleep(ThreadLocalRandom8.current().nextInt(250) + 1);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IgniteSpiException(e);
            }

            super.sendMessage(node, msg, ackC);
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
                ((origMsg instanceof GridNearAtomicAbstractUpdateRequest) || (origMsg instanceof GridDhtAtomicAbstractUpdateRequest));
        }
    }
}
