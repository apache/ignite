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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests that server nodes do not need class definitions to execute queries.
 */
public class Service3056Test extends GridCommonAbstractTest {
    /** */
    public static final String NOOP_SERVICE_CLS_NAME = "org.apache.ignite.tests.p2p.NoopService";

    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Grid count. */
    public static final int GRID_CNT = 4;

    /** */
    private static ClassLoader extClassLoader;

    /** */
    private Set<String> extClassLoaderGrids = new HashSet<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setMarshaller(new OptimizedMarshaller(false));

        if (getTestGridName(GRID_CNT - 1).equals(gridName))
            cfg.setClientMode(true);

        if (extClassLoaderGrids.contains(gridName))
            cfg.setClassLoader(extClassLoader);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        extClassLoaderGrids.clear();

        extClassLoaderGrids.add(getTestGridName(0));
        extClassLoaderGrids.add(getTestGridName(GRID_CNT - 1));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        extClassLoader = getExternalClassLoader();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        extClassLoader = null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testServiceGroupsDeployment1() throws Exception {
        check(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testServiceGroupsDeployment2() throws Exception {
        check(false);
    }

    /**
     * @param useGrpFilter Use cluster group filter or use service configuration node filter instead.
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private void check(boolean useGrpFilter) throws Exception {
        startGrids(GRID_CNT);

        try {
            final UUID id0 = grid(0).cluster().localNode().id();

            IgniteEx client = grid(GRID_CNT - 1);

            final IgnitePredicate<ClusterNode> nodeFilter = new IgnitePredicate<ClusterNode>() {
                @Override public boolean apply(ClusterNode node) {
//                    return node.id().equals(id0);
                    return true;
                }
            };

            IgniteServices srvs;

            if (useGrpFilter)
                srvs = client.services(client.cluster().forPredicate(nodeFilter));
            else
                srvs = client.services();

            ServiceConfiguration srvCfg = new ServiceConfiguration();

            if (!useGrpFilter)
                srvCfg.setNodeFilter(nodeFilter);

            Class<Service> srvcCls = (Class<Service>)extClassLoader.loadClass(NOOP_SERVICE_CLS_NAME);

            Service srvc = srvcCls.newInstance();

            srvCfg.setService(srvc);

            srvCfg.setName("TestDeploymentService");

            srvCfg.setMaxPerNodeCount(1);

            srvs.deploy(srvCfg);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeStart1() throws Exception {
        checkNodeStart(false);
    }

    /**
     * @param useGrpFilter Use group filter or not.
     * @throws Exception If failed.
     */
    public void checkNodeStart(boolean useGrpFilter) throws Exception {
        for (int i = 0; i < GRID_CNT; i++)
            extClassLoaderGrids.add(getTestGridName(i));

        startGrids(GRID_CNT);

        try {
            final Collection<UUID> ids = F.nodeIds(grid(0).cluster().nodes());

            IgniteEx client = grid(GRID_CNT - 1);

            final IgnitePredicate<ClusterNode> nodeFilter = new IgnitePredicate<ClusterNode>() {
                @Override public boolean apply(ClusterNode node) {
                    return ids.contains(node.id());
                }
            };

            IgniteServices srvs;

            if (useGrpFilter)
                srvs = client.services(client.cluster().forPredicate(nodeFilter));
            else
                srvs = client.services();

            ServiceConfiguration srvCfg = new ServiceConfiguration();

            if (!useGrpFilter)
                srvCfg.setNodeFilter(nodeFilter);

            Class<Service> srvcCls = (Class<Service>)extClassLoader.loadClass(NOOP_SERVICE_CLS_NAME);

            Service srvc = srvcCls.newInstance();

            srvCfg.setService(srvc);

            srvCfg.setName("TestDeploymentService");

            srvCfg.setMaxPerNodeCount(1);

            srvs.deploy(srvCfg);

            // Checks node without extclassloader.
            startGrid(100500);
        }
        finally {
            stopAllGrids();
        }
    }
}
