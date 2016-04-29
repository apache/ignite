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
public class Service3056Test2 extends GridCommonAbstractTest {
    /** */
    public static final String NOOP_SERVICE_CLS_NAME = "org.apache.ignite.tests.p2p.NoopService";

    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Grid count. */
    public static final int GRID_CNT = 3;

    /** */
    public static final String GRID_NAME = "GridName";

    /** */
    private static ClassLoader extClsLdr1;

    /** */
    private static ClassLoader extClsLdr2;

    /** */
    private Set<String> gridsGrp1 = new HashSet<>();

    /** */
    private Set<String> gridsGrp2 = new HashSet<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setMarshaller(new OptimizedMarshaller(false));

//        if (getTestGridName(GRID_CNT - 1).equals(gridName))
//            cfg.setClientMode(true);

        if (gridsGrp1.contains(gridName)) {
            cfg.setClassLoader(extClsLdr1);

            cfg.setServiceConfiguration(serviceConfig(1, new IgnitePredicate<ClusterNode>() {
                @Override public boolean apply(ClusterNode node) {
                    return gridsGrp1.contains(node.attribute(GRID_NAME));
                }
            }));
        }

        cfg.setUserAttributes(Collections.singletonMap(GRID_NAME, gridName));

        return cfg;
    }

    /**
     * @param grpNum Group number.
     * @param nodeFilter Node filter.
     * @return Service configuration.
     * @throws Exception If failed.
     */
    private ServiceConfiguration serviceConfig(int grpNum, IgnitePredicate<ClusterNode> nodeFilter) throws Exception {
        ServiceConfiguration srvCfg = new ServiceConfiguration();

        srvCfg.setNodeFilter(nodeFilter);

        Class<Service> srvcCls = (Class<Service>)extClsLdr1.loadClass(NOOP_SERVICE_CLS_NAME);

        Service srvc = srvcCls.newInstance();

        srvCfg.setService(srvc);

        srvCfg.setName("service" + 1);

        srvCfg.setMaxPerNodeCount(1);

        srvCfg.setTotalCount(100);

        return srvCfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        gridsGrp1.clear();

//        gridsGroup1.add(getTestGridName(0));
        gridsGrp1.add(getTestGridName(1));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        extClsLdr1 = getExternalClassLoader();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        extClsLdr1 = null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testServiceStart1() throws Exception {
        try {
            startGrid(0);
            startGrid(1);
//            startGrid(2);
//            startGrid(3);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testServiceStart2() throws Exception {
        try {
//            startGrid(0);
            startGrid(1);
            startGrid(2);
//            startGrid(3);
        }
        finally {
            stopAllGrids();
        }
    }
}
