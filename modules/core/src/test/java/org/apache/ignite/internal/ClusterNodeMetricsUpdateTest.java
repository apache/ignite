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

package org.apache.ignite.internal;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class ClusterNodeMetricsUpdateTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setMetricsUpdateFrequency(500);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testMetrics() throws Exception {
        //IgnitionEx.TEST_ZK = false;

        Ignite srv0 = startGrids(3);

        IgniteCompute c1 = srv0.compute(srv0.cluster().forNodeId(nodeId(1)));
        IgniteCompute c2 = srv0.compute(srv0.cluster().forNodeId(nodeId(2)));

        c1.call(new DummyCallable(null));

        Thread.sleep(3000);

        Ignite srv1 = ignite(0);

        System.out.println(srv1.cluster().forNodeId(nodeId(0)).metrics().getAverageCpuLoad());
        System.out.println(srv1.cluster().forNodeId(nodeId(1)).metrics().getAverageCpuLoad());
        System.out.println(srv1.cluster().forNodeId(nodeId(2)).metrics().getAverageCpuLoad());

        Thread.sleep(3000);

        System.out.println(srv1.cluster().forNodeId(nodeId(0)).metrics().getTotalExecutedJobs());
        System.out.println(srv1.cluster().forNodeId(nodeId(1)).metrics().getTotalExecutedJobs());
        System.out.println(srv1.cluster().forNodeId(nodeId(2)).metrics().getTotalExecutedJobs());
    }

    private UUID nodeId(int nodeIdx) {
        return ignite(nodeIdx).cluster().localNode().id();
    }

    /**
     *
     */
    private static class DummyCallable implements IgniteCallable<Object> {
        /** */
        private byte[] data;

        /**
         * @param data Data.
         */
        DummyCallable(byte[] data) {
            this.data = data;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            return data;
        }
    }
}
