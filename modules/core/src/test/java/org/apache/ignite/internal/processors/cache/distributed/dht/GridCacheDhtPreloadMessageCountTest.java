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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.rendezvous.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * Test cases for partitioned cache {@link GridDhtPreloader preloader}.
 */
public class GridCacheDhtPreloadMessageCountTest extends GridCommonAbstractTest {
    /** Key count. */
    private static final int KEY_CNT = 1000;

    /** Preload mode. */
    private CacheRebalanceMode preloadMode = CacheRebalanceMode.SYNC;

    /** IP finder. */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        assert preloadMode != null;

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setRebalanceMode(preloadMode);
        cc.setAffinity(new CacheRendezvousAffinityFunction(false, 521));
        cc.setBackups(1);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);
        disco.setMaxMissedHeartbeats(Integer.MAX_VALUE);

        c.setDiscoverySpi(disco);
        c.setCacheConfiguration(cc);

        c.setCommunicationSpi(new TestCommunicationSpi());

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAutomaticPreload() throws Exception {
        Ignite g0 = startGrid(0);

        int cnt = KEY_CNT;

        IgniteCache<String, Integer> c0 = g0.jcache(null);

        for (int i = 0; i < cnt; i++)
            c0.put(Integer.toString(i), i);

        Ignite g1 = startGrid(1);
        Ignite g2 = startGrid(2);

        U.sleep(1000);

        IgniteCache<String, Integer> c1 = g1.jcache(null);
        IgniteCache<String, Integer> c2 = g2.jcache(null);

        TestCommunicationSpi spi0 = (TestCommunicationSpi)g0.configuration().getCommunicationSpi();
        TestCommunicationSpi spi1 = (TestCommunicationSpi)g1.configuration().getCommunicationSpi();
        TestCommunicationSpi spi2 = (TestCommunicationSpi)g2.configuration().getCommunicationSpi();

        info(spi0.sentMessages().size() + " " + spi1.sentMessages().size() + " " + spi2.sentMessages().size());

        checkCache(c0, cnt);
        checkCache(c1, cnt);
        checkCache(c2, cnt);
    }

    /**
     * Checks if keys are present.
     *
     * @param c Cache.
     * @param keyCnt Key count.
     */
    private void checkCache(IgniteCache<String, Integer> c, int keyCnt) {
        Ignite g = c.unwrap(Ignite.class);

        for (int i = 0; i < keyCnt; i++) {
            String key = Integer.toString(i);

            if (affinity(c).isPrimaryOrBackup(g.cluster().localNode(), key))
                assertEquals(Integer.valueOf(i), c.localPeek(key, CachePeekMode.ONHEAP));
        }
    }

    /**
     * Communication SPI that will count single partition update messages.
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** Recorded messages. */
        private Collection<GridDhtPartitionsSingleMessage> sentMsgs = new ConcurrentLinkedQueue<>();

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg)
            throws IgniteSpiException {
            recordMessage((GridIoMessage)msg);

            super.sendMessage(node, msg);
        }

        /**
         * @return Collection of sent messages.
         */
        public Collection<GridDhtPartitionsSingleMessage> sentMessages() {
            return sentMsgs;
        }

        /**
         * Adds message to a list if message is of correct type.
         *
         * @param msg Message.
         */
        private void recordMessage(GridIoMessage msg) {
            if (msg.message() instanceof GridDhtPartitionsSingleMessage) {
                GridDhtPartitionsSingleMessage partSingleMsg = (GridDhtPartitionsSingleMessage)msg.message();

                sentMsgs.add(partSingleMsg);
            }
        }
    }
}
