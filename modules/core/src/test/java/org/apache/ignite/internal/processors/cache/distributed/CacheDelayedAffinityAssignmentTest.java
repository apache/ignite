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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessageV2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CacheDelayedAffinityAssignmentTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

       // startGridsMultiThreaded(NODES);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDelayedAffinityAssignment() throws Exception {
        startGrid(0);
        startGrid(1);

        log.info("Done");

        Thread.sleep(60_000);

        //ignite(0).createCache(new CacheConfiguration<Object, Object>());
//        final CacheConfiguration ccfg = new CacheConfiguration();
//
//        ignite(0).createCache(ccfg);
//
//        for (int i = 0; i < NODES; i++) {
//            Ignite ignite = ignite(i);
//
//            TestRecordingCommunicationSpi spi =
//                (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();
//
//            spi.blockMessages(new IgnitePredicate<GridIoMessage>() {
//                @Override public boolean apply(GridIoMessage ioMsg) {
//                    if (!ioMsg.message().getClass().equals(GridDhtPartitionSupplyMessageV2.class))
//                        return false;
//
//                    GridDhtPartitionSupplyMessageV2 msg = (GridDhtPartitionSupplyMessageV2)ioMsg.message();
//
//                    return msg.cacheId() == CU.cacheId(ccfg.getName());
//                }
//            });
//        }
//
//        startGrid(NODES);
//
//        Ignite ignite = ignite(0);
//
//        Affinity aff = ignite.affinity(ccfg.getName());
    }
}
