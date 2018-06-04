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
    /**
     * @throws Exception If failed.
     */
    public void testWriteFullAsync() throws Exception {
        int gridCnt = 2;

        startGrids(gridCnt);

        Integer key = 100;

        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        cache.put(key, 0);

        Affinity<Integer> aff = affinity(cache);

        for (int i = 0; i < gridCnt; i++) {
            if (aff.isPrimary(grid(i).localNode(), key)) {
                TestCommunicationSpi spi = (TestCommunicationSpi)ignite(i).configuration().getCommunicationSpi();
                spi.fail = true;
            }
        }

        cache.put(key, 5);

        doSleep(2_000);

        Collection<ClusterNode> affNodes = aff.mapKeyToPrimaryAndBackups(key);

        final List<GridCacheAdapter> internalCaches = new ArrayList<>();

        for (int i = 0; i < gridCnt; i++) {
            ClusterNode locNode = grid(i).localNode();
            if (affNodes.contains(locNode))
                internalCaches.add(((IgniteKernal)grid(i)).internalCache(DEFAULT_CACHE_NAME));
        }

        List<GridCacheEntryEx> entries = new ArrayList<>();

        for (GridCacheAdapter adapter : internalCaches) {
            GridCacheEntryEx entry = null;
            try {
                entry = adapter.entryEx(key);
                entry.unswap();
            }
            catch (GridDhtInvalidPartitionException ignored) {
                //no-op
            }

            entries.add(entry);
        }

        assertEquals(
            CU.<Integer>value(entries.get(0).rawGet(), entries.get(0).context(), false),
            CU.<Integer>value(entries.get(1).rawGet(), entries.get(1).context(), false)
        );
    }

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

}
