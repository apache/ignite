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

import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cluster.ClusterGroupEmptyException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTaskCancelledException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.failover.always.AlwaysFailoverSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Test for {@link IgniteCompute#affinityCall(String, Object, IgniteCallable)} and
 * {@link IgniteCompute#affinityRun(String, Object, IgniteRunnable)}.
 */
public class CacheAffinityCallSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "myCache";

    /** */
    private static final int MAX_FAILOVER_ATTEMPTS = 105;

    /** */
    private static final int SERVERS_COUNT = 4;

    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(spi);

        AlwaysFailoverSpi failSpi = new AlwaysFailoverSpi();
        failSpi.setMaximumFailoverAttempts(MAX_FAILOVER_ATTEMPTS);
        cfg.setFailoverSpi(failSpi);

        CacheConfiguration ccfg = defaultCacheConfiguration();
        ccfg.setName(CACHE_NAME);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);

        cfg.setCacheConfiguration(ccfg);

        if (gridName.equals(getTestGridName(SERVERS_COUNT))) {
            cfg.setClientMode(true);

            spi.setForceServerMode(true);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityCallRestartNode() throws Exception {
        startGridsMultiThreaded(SERVERS_COUNT);

        final int ITERS = 5;

        for (int i = 0; i < ITERS; i++) {
            log.info("Iteration: " + i);

            Integer key = primaryKey(grid(0).cache(CACHE_NAME));

            long topVer = grid(0).cluster().topologyVersion();

            IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    U.sleep(500);

                    stopGrid(0);

                    return null;
                }
            });

            while (!fut.isDone())
                grid(1).compute().affinityCall(CACHE_NAME, key, new CheckCallable(key, topVer, topVer + 1));

            fut.get();

            if (i < ITERS - 1)
                startGrid(0);
        }

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityCallNoServerNode() throws Exception {
        startGridsMultiThreaded(SERVERS_COUNT + 1);

        final Integer key = 1;

        final Ignite client = grid(SERVERS_COUNT);

        assertTrue(client.configuration().isClientMode());

        final IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int i = 0; i < SERVERS_COUNT; ++i)
                    stopGrid(i);

                return null;
            }
        });

        try {
            while (!fut.isDone())
                client.compute().affinityCall(CACHE_NAME, key, new CheckCallable(key));
        }
        catch (ComputeTaskCancelledException e) {
            assertTrue("Unexpected error: " + e, e.getMessage().contains("stopping"));
        }
        catch(ClusterGroupEmptyException e) {
            assertTrue("Unexpected error: " + e, e.getMessage().contains("Topology projection is empty"));
        }

        stopAllGrids();
    }

    /**
     * Test callable.
     */
    public static class CheckCallable implements IgniteCallable<Object> {
        /** Key. */
        private final Object key;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private long[] topVers;

        /**
         * @param key Key.
         * @param topVers Topology versions to check.
         */
        public CheckCallable(Object key, long... topVers) {
            this.key = key;
            this.topVers = topVers;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws IgniteCheckedException {
            if (topVers.length > 0) {
                boolean pass = false;

                GridCacheAffinityManager aff =
                    ((IgniteKernal)ignite).context().cache().internalCache(CACHE_NAME).context().affinity();

                ClusterNode loc = ignite.cluster().localNode();

                for (long topVer : topVers) {
                    if (loc.equals(aff.primary(key, new AffinityTopologyVersion(topVer, 0)))) {
                        pass = true;

                        break;
                    }
                }

                assertTrue(pass);
            }

            return null;
        }
    }
}