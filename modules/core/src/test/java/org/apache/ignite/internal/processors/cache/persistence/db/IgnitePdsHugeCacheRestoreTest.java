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

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.GridDebug;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;

/**
 *
 */
public class IgnitePdsHugeCacheRestoreTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES_CNT = 4;

    /** */
    private static final long KEY_CNT = 2_000_000;

    /** */
    private static final String HAS_CACHE = "HAS_CACHE";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(new TcpDiscoveryVmIpFinder(false)
            .setAddresses(Arrays.asList("127.0.0.1:47500..47550")));

        cfg.setCacheConfiguration(new CacheConfiguration().setName(DEFAULT_CACHE_NAME)
            .setNodeFilter(new TestNodeFilter())
//            .setBackups(1)
        );

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true));

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setUserAttributes(getTestIgniteInstanceIndex(igniteInstanceName) != 0 ? F.asMap(HAS_CACHE, true) : null);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        startGrids(NODES_CNT);

        grid(0).cluster().active(true);

        try (IgniteDataStreamer streamer = grid(0).dataStreamer(DEFAULT_CACHE_NAME)) {
            for (long i = 0; i < KEY_CNT; ++i) {
                streamer.addData(i, new Value(i));

                if (i % 100_000 == 0)
                    log.info("Populate " + i + " values");
            }
        }

        awaitPartitionMapExchange(true, true, null);

        grid(0).cluster().active(false);

        stopAllGrids(false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void test() throws Exception {
        startGrid(0);

        for (int i = 1; i < NODES_CNT; ++i) {
            final CountDownLatch nodeStartedLatch = new CountDownLatch(1);

            final long t0 = U.currentTimeMillis();
            grid(0).events().localListen((IgnitePredicate<Event>)e -> {
                nodeStartedLatch.countDown();

                log.info("+++ JOIN " + ((DiscoveryEvent)e).eventNode().id());
                log.info("+++ START TIME=" + (U.currentTimeMillis()  - t0) / 1000);
                return false;
            }, EventType.EVT_NODE_JOINED);

            log.info("+++ RESTART " + i);

//            GridTestUtils.runAsync(new Runnable() {
//                @Override public void run() {
//                    try {
//                        if (!nodeStartedLatch.await(30, TimeUnit.SECONDS)) {
//                            U.dumpThreads(log);
//
//                            fail("Long node start");
//                        }
//                    }
//                    catch (InterruptedException e) {
//                        fail("Unexpected exception");
//                    }
//                }
//            });

            Ignite ig = startGrid(i);
        }
    }

    /** */
    public static class Value {
        /** */
        long longVal;

        /** */
        String strVal;

        /**
         * @param id ID.
         */
        public Value(long id) {
            this.longVal = id;
            this.strVal = "Value " + id;
        }
    }

    /**
     *
     */
    public static class TestNodeFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return node.attribute(HAS_CACHE) != null;
        }
    }
}
