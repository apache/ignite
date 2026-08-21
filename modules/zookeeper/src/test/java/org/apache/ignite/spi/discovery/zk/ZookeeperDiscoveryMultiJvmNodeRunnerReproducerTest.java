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

package org.apache.ignite.spi.discovery.zk;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Reproducer for remote-node startup with preprocessed ZooKeeper discovery.
 */
public class ZookeeperDiscoveryMultiJvmNodeRunnerReproducerTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    @BeforeClass
    public static void beforeClass() throws Exception {
        ZookeeperDiscoverySpiTestConfigurator.initTestSuite();
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        spi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(spi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected Ignite startRemoteGrid(
        String igniteInstanceName,
        IgniteConfiguration cfg,
        GridSpringResourceContext ctx,
        boolean resetDiscovery
    ) throws Exception {
        if (ctx != null)
            throw new UnsupportedOperationException("Starting a grid in another JVM with Spring context is not supported.");

        if (cfg == null)
            cfg = optimize(getConfiguration(igniteInstanceName));

        ZookeeperDiscoverySpiTestConfigurator.preprocessConfiguration(cfg);

        return new IgniteProcessProxy(
            cfg,
            cfg.getGridLogger(),
            () -> grid(0),
            true,
            additionalRemoteJvmArgs()
        );
    }

    /**
     * Verifies that a remote node started from preprocessed configuration joins the cluster.
     */
    @Test
    public void testRemoteNodeStartWithPreprocessedDiscovery() throws Exception {
        Ignite n0 = startGrid(0);

        assertTrue(n0.configuration().getDiscoverySpi() instanceof ZookeeperDiscoverySpi);

        Ignite n1 = startGrid(1);

        assertTrue(isMultiJvmObject(n1));
        assertTrue(n1.configuration().getDiscoverySpi() instanceof ZookeeperDiscoverySpi);

        assertTrue(
            "Topology did not reach 2 nodes in time",
            GridTestUtils.waitForCondition(() -> n0.cluster().nodes().size() == 2, 30_000)
        );
    }
}
