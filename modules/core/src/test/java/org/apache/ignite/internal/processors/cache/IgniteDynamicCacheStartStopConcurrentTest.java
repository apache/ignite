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

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class IgniteDynamicCacheStartStopConcurrentTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentStartStop() throws Exception {
        awaitPartitionMapExchange();

        int minorVer = ignite(0).configuration().isLateAffinityAssignment() ? 1 : 0;

        checkTopologyVersion(new AffinityTopologyVersion(NODES, minorVer));

        for (int i = 0; i < 5; i++) {
            log.info("Iteration: " + i);

            GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
                @Override public void apply(Integer idx) {
                    Ignite ignite = ignite(idx);

                    ignite.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));
                }
            }, NODES, "cache-thread");

            minorVer++;

            checkTopologyVersion(new AffinityTopologyVersion(NODES, minorVer));

            ignite(0).compute().affinityRun(DEFAULT_CACHE_NAME, 1, new IgniteRunnable() {
                @Override public void run() {
                    // No-op.
                }
            });

            GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
                @Override public void apply(Integer idx) {
                    Ignite ignite = ignite(idx);

                    ignite.destroyCache(DEFAULT_CACHE_NAME);
                }
            }, NODES, "cache-thread");

            minorVer++;

            checkTopologyVersion(new AffinityTopologyVersion(NODES, minorVer));
        }
    }

    /**
     * @param topVer Expected version.
     */
    private void checkTopologyVersion(AffinityTopologyVersion topVer) {
        for (int i = 0; i < NODES; i++) {
            IgniteKernal ignite = (IgniteKernal)ignite(i);

            assertEquals(ignite.name(), topVer, ignite.context().discovery().topologyVersionEx());
        }
    }
}
