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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

@SuppressWarnings("unchecked")
public class IgniteDynamicCacheStartFailSelfTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String DYNAMIC_CACHE_NAME = "TestDynamicCache";

    /**
     * @return Number of nodes for this test.
     */
    public int nodeCount() {
        return 3;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setIncludeEventTypes(EventType.EVT_CACHE_STARTED, EventType.EVT_CACHE_STOPPED, EventType.EVT_CACHE_NODES_LEFT);

        return cfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(nodeCount());
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    public  void testGetOrCreateCollectionExceptional() throws Exception {
        final int cacheCnt = 2;

        final int failIdx = 0;

        final IgniteEx grid = grid(0);

        final Collection<CacheConfiguration> ccfgs = new ArrayList<>();

        final AffinityFunction aff = new ExceptionalAffinityFunction();
        ExceptionalAffinityFunction.gridName = getTestGridName(1);

        for (int i = 0; i < cacheCnt; ++i) {

            final CacheConfiguration cfg = new CacheConfiguration();

            cfg.setName(DYNAMIC_CACHE_NAME + Integer.toString(i));

            if (i == failIdx) {
                cfg.setAffinity(aff);
            }

            try {
                IgniteCache cache = grid.getOrCreateCache(cfg);

                if (i == failIdx)
                    fail("Simulated exception must be thrown");
            }
            catch (Exception e) {
                if (i != failIdx)
                    fail("Simulated exception must not be thrown");
            }
        }
    }

    private static class ExceptionalAffinityFunction extends RendezvousAffinityFunction {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Exception should arise on all nodes. */
        boolean exceptionOnAllNodes = false;

        /** Exception should arise on node with certain name. */
        public static String gridName;

        /**
         * Constructor
         */
        public ExceptionalAffinityFunction() {
        }

        /**
         * Throws simulated exception
         * {@inheritDoc}
         */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            if (exceptionOnAllNodes || ignite.name().equals(gridName)) {
                throw new IllegalStateException("Simulated exception");
            }
            else
                return super.assignPartitions(affCtx);
        }
    }
}
