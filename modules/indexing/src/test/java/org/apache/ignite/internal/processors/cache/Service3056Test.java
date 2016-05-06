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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
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
    private static final String NOOP_SERVICE_CLS_NAME = "org.apache.ignite.tests.p2p.NoopService";

    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Grid count. */
    private static final int GRID_CNT = 4;

    /** */
    private static final int SERVER_NODE = 0;

    /** */
    private static final int SERVER_NODE_WITH_EXT_CLASS_LOADER = 1;

    /** */
    private static final int CLIENT_IDX = 2;

    /** */
    private static final int CLIENT_NODE_WITH_EXT_CLASS_LOADER = 3;

    /** */
    private static final String GRID_NAME_ATTR = "GRID_NAME";

    /** */
    private static ClassLoader extClassLoader;

    /** */
    private Set<String> extClassLoaderGrids = new HashSet<>();

    /** */
    private boolean setServiceConfig;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

//        cfg.setMarshaller(new OptimizedMarshaller(false));
        cfg.setMarshaller(new BinaryMarshaller());

        cfg.setUserAttributes(Collections.singletonMap(GRID_NAME_ATTR, gridName));

        if (getTestGridName(CLIENT_NODE_WITH_EXT_CLASS_LOADER).equals(gridName)
            || getTestGridName(CLIENT_IDX).equals(gridName))
            cfg.setClientMode(true);

        if (extClassLoaderGrids.contains(gridName)) {
            cfg.setClassLoader(extClassLoader);

            if (setServiceConfig)
                cfg.setServiceConfiguration(serviceConfig());
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        extClassLoaderGrids.clear();

        extClassLoaderGrids.add(getTestGridName(SERVER_NODE_WITH_EXT_CLASS_LOADER));
        extClassLoaderGrids.add(getTestGridName(CLIENT_NODE_WITH_EXT_CLASS_LOADER));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        setServiceConfig = false;

        super.afterTest();
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
    public void testServiceDeployment() throws Exception {
        for (int i = 0; i < GRID_CNT; i++) {
            try {
                startGrids(GRID_CNT);

                info("Testing service deploymnet from grid [i=" + i + ", id8="
                    + U.id8(grid(i).cluster().localNode().id()) + "]");

                ServiceConfiguration srvCfg = serviceConfig();

                grid(i).services().deploy(srvCfg);
            }
            finally {
                stopAllGrids();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeploymentOnNodeStart1() throws Exception {
        setServiceConfig = true;

        try {
            startGrid(SERVER_NODE);
            startGrid(SERVER_NODE_WITH_EXT_CLASS_LOADER);
            startGrid(CLIENT_IDX);
            startGrid(CLIENT_NODE_WITH_EXT_CLASS_LOADER);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeploymentOnNodeStart2() throws Exception {
        setServiceConfig = true;

        try {
            startGrid(SERVER_NODE_WITH_EXT_CLASS_LOADER);
            startGrid(SERVER_NODE);
            startGrid(CLIENT_NODE_WITH_EXT_CLASS_LOADER);
            startGrid(CLIENT_IDX);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeploymentOnNodeStart3() throws Exception {
        setServiceConfig = true;

        try {
            startGrid(SERVER_NODE);
            startGrid(CLIENT_NODE_WITH_EXT_CLASS_LOADER);
            startGrid(CLIENT_IDX);
            startGrid(SERVER_NODE_WITH_EXT_CLASS_LOADER);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @return Service configuration.
     * @throws Exception If failed.
     */
    private ServiceConfiguration serviceConfig() throws Exception {
        ServiceConfiguration srvCfg = new ServiceConfiguration();

        final IgnitePredicate<ClusterNode> nodeFilter = new IgnitePredicate<ClusterNode>() {
            @SuppressWarnings("SuspiciousMethodCalls")
            @Override public boolean apply(ClusterNode node) {
                return extClassLoaderGrids.contains(node.attribute(GRID_NAME_ATTR));
            }
        };

        srvCfg.setNodeFilter(nodeFilter);

        Class<Service> srvcCls = (Class<Service>)extClassLoader.loadClass(NOOP_SERVICE_CLS_NAME);

        Service srvc = srvcCls.newInstance();

        srvCfg.setService(srvc);

        srvCfg.setName("TestDeploymentService");

        srvCfg.setMaxPerNodeCount(1);

        return srvCfg;
    }
}
