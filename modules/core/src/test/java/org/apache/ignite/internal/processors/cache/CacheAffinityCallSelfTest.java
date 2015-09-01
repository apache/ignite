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
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterGroupEmptyException;
import org.apache.ignite.compute.ComputeTaskCancelledException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
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

        if (gridName.equals(getTestGridName(SERVERS_COUNT)))
            cfg.setClientMode(true);

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
        startGrids(4);

        Integer key = primaryKey(grid(0).cache(CACHE_NAME));

        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                U.sleep(500);
                stopGrid(0);

                return null;
            }
        });

        while (!fut.isDone())
            grid(1).compute().affinityCall(CACHE_NAME, key, new CheckCallable(key));

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityCallNoServerNode() throws Exception {
        startGrids(SERVERS_COUNT + 1);

        final Integer key = 1;

        final Ignite client = grid(SERVERS_COUNT);

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
            assertTrue(e.getMessage().contains("stopping"));
        }
        catch(ClusterGroupEmptyException e) {
            assertTrue(e.getMessage().contains("Topology projection is empty"));
        }
        catch(IgniteException e) {
            assertTrue(e.getMessage().contains("Client node disconnected") ||
                e.getMessage().contains("Failed to reconnect to cluster") ||
                e.getMessage().contains("Failed to execute task, client node disconnected."));
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

        /**
         * @param key Key.
         */
        public CheckCallable(Object key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws IgniteCheckedException {
            assert ignite.cluster().localNode().id().equals(ignite.cluster().mapKeyToNode(CACHE_NAME, key).id());

            return null;
        }
    }
}