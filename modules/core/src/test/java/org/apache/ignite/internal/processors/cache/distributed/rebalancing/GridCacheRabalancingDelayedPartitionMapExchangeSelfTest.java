/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.ConcurrentHashMap8;

/**
 *
 */
public class GridCacheRabalancingDelayedPartitionMapExchangeSelfTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private final ConcurrentHashMap8<UUID, Runnable> rs = new ConcurrentHashMap8<>();

    /** */
    private volatile boolean record = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration iCfg = super.getConfiguration(igniteInstanceName);

        TcpCommunicationSpi commSpi = new DelayableCommunicationSpi();

        commSpi.setLocalPort(GridTestUtils.getNextCommPort(getClass()));
        commSpi.setTcpNoDelay(true);

        iCfg.setCommunicationSpi(commSpi);

        return iCfg;
    }

    /**
     * Helps to delay GridDhtPartitionsFullMessages.
     */
    public class DelayableCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(final ClusterNode node, final Message msg,
            final IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            final Object msg0 = ((GridIoMessage)msg).message();

            if (msg0 instanceof GridDhtPartitionsFullMessage && record &&
                ((GridDhtPartitionsFullMessage)msg0).exchangeId() == null) {
                rs.putIfAbsent(node.id(), new Runnable() {
                    @Override public void run() {
                        DelayableCommunicationSpi.super.sendMessage(node, msg, ackC);
                    }
                });
            }
            else
                try {
                    super.sendMessage(node, msg, ackC);
                }
                catch (Exception e) {
                    U.log(null, e);
                }

        }
    }

    /**
     * @throws Exception e.
     */
    public void test() throws Exception {
        IgniteKernal ignite = (IgniteKernal)startGrid(0);

        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        cfg.setBackups(1);

        ignite(0).getOrCreateCache(cfg);

        startGrid(1);
        startGrid(2);
        startGrid(3);

        awaitPartitionMapExchange(true, true, null);

        for (int i = 0; i < 2; i++) {
            stopGrid(3);

            awaitPartitionMapExchange(true, true, null);

            startGrid(3);

            awaitPartitionMapExchange(true, true, null);
        }

        startGrid(4);

        awaitPartitionMapExchange(true, true, null);

        assert rs.isEmpty();

        record = true;

        // Emulate latest GridDhtPartitionsFullMessages.
        ignite.context().cache().context().exchange().scheduleResendPartitions();

        while (rs.size() < 3) { // N - 1 nodes.
            U.sleep(10);
        }

        ignite(0).destroyCache(DEFAULT_CACHE_NAME);

        ignite(0).getOrCreateCache(cfg);

        awaitPartitionMapExchange();

        for (Runnable r : rs.values())
            r.run();

        U.sleep(10000); // Enough time to process delayed GridDhtPartitionsFullMessages.

        stopGrid(3); // Forces exchange at all nodes and cause assertion failure in case obsolete partition map accepted.

        awaitPartitionMapExchange();

        long topVer0 = grid(0).context().cache().context().exchange().readyAffinityVersion().topologyVersion();
        long topVer1 = grid(1).context().cache().context().exchange().readyAffinityVersion().topologyVersion();
        long topVer2 = grid(2).context().cache().context().exchange().readyAffinityVersion().topologyVersion();

        stopGrid(4); // Should force exchange in case exchange manager alive.

        awaitPartitionMapExchange();

        // Will fail in case exchange-workers are dead.
        assert grid(0).context().cache().context().exchange().readyAffinityVersion().topologyVersion() > topVer0;
        assert grid(1).context().cache().context().exchange().readyAffinityVersion().topologyVersion() > topVer1;
        assert grid(2).context().cache().context().exchange().readyAffinityVersion().topologyVersion() > topVer2;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

}
