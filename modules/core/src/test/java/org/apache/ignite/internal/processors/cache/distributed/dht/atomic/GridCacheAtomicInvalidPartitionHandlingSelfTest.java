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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CachePartialUpdateException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.TestDelayingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;

/**
 * Test GridDhtInvalidPartitionException handling in ATOMIC cache during restarts.
 */
@SuppressWarnings("ErrorNotRethrown")
public class GridCacheAtomicInvalidPartitionHandlingSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);
    private static final int BACKUPS = 2;

    /** Delay flag. */
    private static volatile boolean delay;

    /** Write sync. */
    private CacheWriteSynchronizationMode writeSync;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER).setForceServerMode(true));

        CacheConfiguration ccfg = cacheConfiguration();

        cfg.setCacheConfiguration(ccfg);

        DelayCommunicationSpi spi = new DelayCommunicationSpi();

        spi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(spi);

        if (testClientNode() && getTestIgniteInstanceName(0).equals(igniteInstanceName))
            cfg.setClientMode(true);

        return cfg;
    }

    @Override protected long getTestTimeout() {
        return 30 * 60 * 1000;
    }

    /** {@inheritDoc} */
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(PARTITIONED);

        ccfg.setBackups(BACKUPS);
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

    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();
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
    /*public void testPrimaryFullSync() throws Exception {
        checkRestarts(FULL_SYNC);
    }*/

    /**
     * @throws Exception If failed.
     */
    /*public void testPrimaryPrimarySync() throws Exception {
        checkRestarts(PRIMARY_SYNC);
    }*/

    /*public void testMyTest() throws Exception {
        this.writeSync = FULL_ASYNC;
        IgniteEx ignite = startGrid(3);
        IgniteCache<String, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);
        cache.put("@@@MY_KEY@@@", 505);
        Thread.sleep(2 * 1000);
        cache.put("@@@MY_KEY@@@", 2005);
        Thread.sleep(2 * 1000);

        //grid(0).localNode().

        assertEquals(Integer.valueOf(2005), cache.get("@@@MY_KEY@@@"));
    }*/

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryFullAsync0() throws Exception {
        checkRestartsForAsyncMode();
    }

    private void checkRestartsForAsyncMode() throws Exception {
        StringBuilder sb = new StringBuilder();

        this.writeSync = FULL_ASYNC;

        final int gridCnt = 6;

        startGrids(gridCnt);

        awaitPartitionMapExchange();

        assertEquals(testClientNode(), (boolean)grid(0).configuration().isClientMode());

        final int range = 30_000;

        populateCache(grid(0), range);
        //checkWriteValuesSingleThread(gridCnt, range);

        System.err.println("FINISHED PUTS");

        delay = true;

        final IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        final AtomicBoolean done = new AtomicBoolean();
        final AtomicInteger threadIdx = new AtomicInteger(0);
        // Start put threads.
        IgniteInternalFuture<?> fut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Random rnd = new Random();
                Integer thIdx = threadIdx.incrementAndGet() * 1000;
                while (!done.get()) {
                    try {
                        int cnt = rnd.nextInt(5);

                        if (cnt < 2) {
                            int key = rnd.nextInt(range);
                            //int val = rnd.nextInt();
                            cache.put(key, 111);
                        }
                        else {
                            Map<Integer, Integer> upd = new TreeMap<>();

                            for (int i = 0; i < cnt; i++) {
                                int key = rnd.nextInt(range);
                                upd.put(key, 222);
                            }

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
        /*sb.append(" RESTART").append("\n");
        for (int r = 0; r < 20; r++) {
            int idx0 = rnd.nextInt(gridCnt - 1) + 1;
            IgniteEx ignite = grid(idx0);
            sb.append(r)
                .append(":\t")
                .append(idx0)
                .append("\t")
                .append(ignite.localNode().id());

            stopGrid(idx0);

            U.sleep(200);

            ignite = startGrid(idx0);
            sb.append("\t").append("after")
                .append("\t").append(ignite.localNode().id())
                .append("\t").append(ignite.localNode().consistentId())
                .append("\n");
        }
        sb.append("\n");*/

        int[] restartOrder = new int[]{2, 4, 1, 1, 2, 1, 4, 3, 4, 1, 2, 1, 1, 5, 4, 4, 2, 5, 3, 4};
        for (int idx0 : restartOrder) {
            stopGrid(idx0);

            U.sleep(200);

            startGrid(idx0);
        }

//        U.sleep(20 * 200 + 2 * 1000);

        done.set(true);

        awaitPartitionMapExchange(false, false, null);

        fut.get();

//            List<DebugData> debugList = new LinkedList<>();
        for (int k = 0; k < range; k++) {
            boolean wasErr;
            int cnt = 0;
            do {
                wasErr = false;
                Collection<ClusterNode> affNodes = affinity(cache).mapKeyToPrimaryAndBackups(k);
//                debugList.clear();
                final List<GridCacheAdapter> internalCaches = new ArrayList<>();
                for (int i = 0; i < gridCnt; i++) {
                    ClusterNode locNode = grid(i).localNode();
                    if (affNodes.contains(locNode))
                        internalCaches.add(((IgniteKernal)grid(i)).internalCache(DEFAULT_CACHE_NAME));
                }
                try {
                    assertEquals(BACKUPS + 1, internalCaches.size());

                    List<GridCacheEntryEx> entries = new ArrayList<>();

                    for (GridCacheAdapter adapter : internalCaches) {
                        GridCacheEntryEx entry = null;
                        try {
                            entry = adapter.entryEx(k);
                            entry.unswap();
                        }
                        catch (GridDhtInvalidPartitionException ignored) {
                            //no-op
                        }

                        assertNotNull("Failed to find entry on node for key [locNode=" + adapter.context().localNodeId() +
                            ", key=" + k + ']', entry);

                        assertTrue("Partition is invalid [entry=" + entry + ']', entry.partitionValid());

                        entries.add(entry);
                    }

                    GridCacheEntryEx first = entries.get(0);

                    assertNotNull(first.version());

                    for (int i = 1; i < entries.size(); i++)
                        assertEntry(first, entries.get(i));
                }
                catch (AssertionError e) {
                    if (cnt < 10) {
                        cnt++;
                        wasErr = true;
                        System.out.println("Fail [attempt=" + cnt + ", err=" + e.getMessage() + ']');
                        U.sleep(10 * 1000);
                    }
                    else {
                        System.out.println(sb.toString());
                        throw e;
                    }
                }

            }
            while (wasErr);
        }
    }

    private void assertEntry(GridCacheEntryEx exp, GridCacheEntryEx act) throws GridCacheEntryRemovedException {
        assertEquals("Failed to check value for key [key=" + exp.key() +
                ", node=" + exp.context().localNodeId() + ", ver=" + exp.version() +
                ", actNode=" + act.context().localNodeId() + ", actVer" + act.version() + ']',
            CU.<Integer>value(exp.rawGet(), exp.context(), false),
            CU.<Integer>value(act.rawGet(), act.context(), false));

        assertEquals("Failed to check version for key [key=" + exp.key() + ", firstNode=" +
                exp.context().localNodeId() + ", secondNode=" + act.context().localNodeId() + ']',
            exp.version(), act.version());
    }

    private Set<Integer> populateCache(IgniteEx ignite, int range) {
        Set<Integer> set = new LinkedHashSet<>();
        try (IgniteDataStreamer<Integer, Integer> streamer = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            streamer.allowOverwrite(true);
            for (int i = 0; i < range; i++) {
                streamer.addData(i, 0);

                set.add(i);

                if (i > 0 && i % 10_000 == 0)
                    System.err.println("Put: " + i);
            }
        }
        return set;
    }

    private void checkWriteValuesSingleThread(int gridCnt, int range) throws Exception {
        IgniteEx ignite = grid(0);

        final Set<Integer> keys = populateCache(ignite, range);

        final Affinity<Integer> aff = ignite.affinity(DEFAULT_CACHE_NAME);

        boolean putDone = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                Iterator<Integer> it = keys.iterator();

                while (it.hasNext()) {
                    Integer key = it.next();

                    Collection<ClusterNode> affNodes = aff.mapKeyToPrimaryAndBackups(key);

                    for (int i = 0; i < gridCnt; i++) {
                        ClusterNode locNode = grid(i).localNode();

                        IgniteCache<Object, Object> cache = grid(i).cache(DEFAULT_CACHE_NAME);

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
    }

    /**
     * @param writeSync Write synchronization mode to check.
     * @throws Exception If failed.
     */
    /*private void checkRestarts(CacheWriteSynchronizationMode writeSync)
        throws Exception {
        this.writeSync = writeSync;

        final int gridCnt = 6;

        startGrids(gridCnt);

        awaitPartitionMapExchange();

        try {
            assertEquals(testClientNode(), (boolean)grid(0).configuration().isClientMode());

            final int range = 100_000;

            checkWriteValuesSingleThread(gridCnt, range);

            System.err.println("FINISHED PUTS");

            final AtomicBoolean done = new AtomicBoolean();

            delay = true;

            final IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);
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

                    GridCacheAdapter<Object, Object> c = ((IgniteKernal)grid(i)).internalCache(DEFAULT_CACHE_NAME);

                    GridCacheEntryEx entry = null;

                    try {
                        entry = c.entryEx(k);

                        entry.unswap();
                    }
                    catch (GridDhtInvalidPartitionException ignored) {
                        // Skip key.
                    }

                    if (affNodes.contains(locNode)) {
                        assert c.affinity().isPrimaryOrBackup(locNode, k);

                        boolean primary = c.affinity().isPrimary(locNode, k);

                        assertNotNull("Failed to find entry on node for key [locNode=" + locNode.id() +
                            ", key=" + k + ']', entry);

                        if (val == null) {
                            assertNull(ver);

                            val = CU.value(entry.rawGet(), entry.context(), false);
                            ver = entry.version();
                            nodeId = locNode.id();
                        }
                        else {
                            assertNotNull(ver);

                            assertEquals("Failed to check value for key [key=" + k + ", node=" +
                                    locNode.id() + ", primary=" + primary + ", recNodeId=" + nodeId + ']',
                                val, CU.value(entry.rawGet(), entry.context(), false));

                            assertEquals("Failed to check version for key [key=" + k + ", node=" +
                                    locNode.id() + ", primary=" + primary + ", recNodeId=" + nodeId + ']',
                                ver, entry.version());
                        }
                    }
                    else
                        assertTrue("Invalid entry: " + entry, entry == null || !entry.partitionValid());
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }*/

    /**
     *
     */
    private static class DelayCommunicationSpi extends TestDelayingCommunicationSpi {
        /** {@inheritDoc} */
        @Override protected boolean delayMessage(Message msg, GridIoMessage ioMsg) {
            return delay && ((msg instanceof GridNearAtomicAbstractUpdateRequest) ||
                (msg instanceof GridDhtAtomicAbstractUpdateRequest));
        }
    }

    class DebugData {

        private int order;
        private ClusterNode node;
        private GridCacheEntryEx entry;

        public DebugData(int order, ClusterNode node, GridCacheEntryEx entry) {
            this.order = order;
            this.node = node;
            this.entry = entry;
        }

        @Override public String toString() {
            try {
                return "DebugData{" +
                    "order=" + order +
                    ", nodeId=" + node.id() +
                    ", key=" + entry.key() +
                    ", ver=" + entry.version() +
                    ", value=" + CU.value(entry.rawGet(), entry.context(), false) +
                    '}';
            }
            catch (Exception e) {
                return e.getMessage();
            }
        }
    }

}
