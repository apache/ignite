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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CachePartialUpdateException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.TestDelayingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheNearTxRollbackTest;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;

/**
 * Reproducer for atomic cache inconsistency state.
 */
@SuppressWarnings("ErrorNotRethrown")
public class ReproducerAtomicCacheInconsistentState2Test extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Backups. */
    private static final int BACKUPS = 1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setNetworkSendRetryCount(1);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER).setForceServerMode(true));

        CacheConfiguration ccfg = cacheConfiguration();

        cfg.setCacheConfiguration(ccfg);

        TestCommunicationSpi spi = new TestCommunicationSpi();

        spi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(spi);

        return cfg;
    }

    /** {@inheritDoc} */
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(BACKUPS);
        ccfg.setWriteSynchronizationMode(FULL_ASYNC);
        ccfg.setRebalanceMode(SYNC);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testWriteFullAsync() throws Exception {
        int gridCnt = 3;

        startGrids(gridCnt);

        awaitPartitionMapExchange();

        final int range = 20_000;

        populateCache(grid(0), range);

        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        Affinity<Integer> aff = affinity(cache);

        Integer key = 218;

        for (int i = 0; i < gridCnt; i++) {
            ClusterNode locNode = grid(i).localNode();
            if (aff.isPrimary(locNode, key)) {
                TestCommunicationSpi spi = (TestCommunicationSpi)ignite(i).configuration().getCommunicationSpi();
                spi.fail = true;
            }
        }
        cache.put(key, 3111);

        doSleep(2_000);

        boolean wasErr;
        int cnt = 0;

        do {
            wasErr = false;
            Collection<ClusterNode> affNodes = aff.mapKeyToPrimaryAndBackups(key);

            final List<GridCacheAdapter> internalCaches = new ArrayList<>();

            for (int i = 0; i < gridCnt; i++) {
                ClusterNode locNode = grid(i).localNode();
                if (affNodes.contains(locNode))
                    internalCaches.add(((IgniteKernal)grid(i)).internalCache(DEFAULT_CACHE_NAME));
            }

            List<DebugData> entries = new ArrayList<>();
            try {
                assertEquals(BACKUPS + 1, internalCaches.size());

                entries.clear();

                for (GridCacheAdapter adapter : internalCaches) {
                    GridCacheEntryEx entry = null;
                    try {
                        entry = adapter.entryEx(key);
                        entry.unswap();
                    }
                    catch (GridDhtInvalidPartitionException ignored) {
                        //no-op
                    }

                    assertNotNull("Failed to find entry on node for key [locNode=" + adapter.context().localNodeId() +
                        ", key=" + key + ']', entry);

                    ClusterNode locNode = adapter.context().localNode();

                    entries.add(
                        new DebugData(
                            locNode,
                            aff.isPrimary(locNode, key),
                            entry
                        )
                    );
                }

                GridCacheEntryEx first = entries.get(0).entry();

                assertNotNull(first.version());

                for (int i = 1; i < entries.size(); i++)
                    assertEntry(first, entries.get(i).entry());
            }
            catch (AssertionError e) {
                if (cnt < 5) {
                    cnt++;
                    wasErr = true;
                    System.out.println("Fail [attempt=" + cnt + ", err=" + e.getMessage() + ']');
                    U.sleep(5 * 1000);
                }
                else {
                    //printDebugData(entries);
                    throw e;
                }
            }
        }
        while (wasErr);

    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private volatile boolean fail = false;

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                Object msg0 = ((GridIoMessage)msg).message();

                if (fail && msg0 instanceof GridDhtAtomicAbstractUpdateRequest) {
                    fail = false;
                    throw new IgniteSpiException("Test error");
                }
            }

            super.sendMessage(node, msg, ackClosure);
        }
    }

    /**
     * @param exp Expected.
     * @param act Action.
     */
    private void assertEntry(GridCacheEntryEx exp, GridCacheEntryEx act) throws GridCacheEntryRemovedException {
        assertEquals("Failed to check value for key [key=" + exp.key() +
                ",\nnode1=" + exp.context().localNodeId() + ", ver1=" + exp.version() +
                ",\nnode2=" + act.context().localNodeId() + ", ver2=" + act.version() + ']',
            CU.<Integer>value(exp.rawGet(), exp.context(), false),
            CU.<Integer>value(act.rawGet(), act.context(), false));

        assertEquals("Failed to check version for key [key=" + exp.key() + ",\nnode1=" +
                exp.context().localNodeId() + ",\nnode2=" + act.context().localNodeId() + ']',
            exp.version(), act.version());
    }

    /**
     * @param ignite Ignite.
     * @param range Range.
     */
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

    class DebugData {

        private ClusterNode node;
        private boolean primary;
        private GridCacheEntryEx entry;

        /**
         * @param node Node.
         * @param primary Primary.
         * @param entry Entry.
         */
        public DebugData(ClusterNode node, boolean primary, GridCacheEntryEx entry) {
            this.node = node;
            this.primary = primary;
            this.entry = entry;
        }

        GridCacheEntryEx entry() {
            return entry;
        }

        @Override public String toString() {
            try {
                return "DebugData{" +
                    "node=" + node.id() +
                    ",\tprimary=" + primary +
                    ",\tver=" + entry.version() +
                    ",\tvalue=" + CU.value(entry.rawGet(), entry.context(), false) +
                    '}';
            }
            catch (Exception e) {
                return e.getMessage();
            }
        }
    }
}
