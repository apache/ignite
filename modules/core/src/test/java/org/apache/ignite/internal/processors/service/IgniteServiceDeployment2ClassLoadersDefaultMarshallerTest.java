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
package org.apache.ignite.internal.processors.service;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestExternalClassLoader;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests that not all nodes in cluster need user's service definition (only nodes according to filter).
 */
public class IgniteServiceDeployment2ClassLoadersDefaultMarshallerTest extends GridCommonAbstractTest {
    /** */
    private static final String NOOP_SERVICE_CLS_NAME = "org.apache.ignite.tests.p2p.NoopService";

    /** */
    private static final String NOOP_SERVICE_2_CLS_NAME = "org.apache.ignite.tests.p2p.NoopService2";

    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int GRID_CNT = 4;

    /** */
    private static final String GRID_NAME_ATTR = "GRID_NAME";

    /** */
    private static final URL[] URLS;

    /** */
    private static ClassLoader extClsLdr1;

    /** */
    private static ClassLoader extClsLdr2;

    /** */
    private Set<String> grp1 = new HashSet<>();

    /** */
    private Set<String> grp2 = new HashSet<>();

    /**
     * Initialize URLs.
     */
    static {
        try {
            URLS = new URL[] {new URL(GridTestProperties.getProperty("p2p.uri.cls"))};
        }
        catch (MalformedURLException e) {
            throw new RuntimeException("Define property p2p.uri.cls", e);
        }
    }


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setMarshaller(marshaller());

        cfg.setUserAttributes(Collections.singletonMap(GRID_NAME_ATTR, gridName));

        if (getTestGridName(GRID_CNT - 1).equals(gridName) || getTestGridName(GRID_CNT - 2).equals(gridName))
            cfg.setClientMode(true);

        if (grp1.contains(gridName))
            cfg.setClassLoader(extClsLdr1);

        if (grp2.contains(gridName))
            cfg.setClassLoader(extClsLdr2);

        return cfg;
    }

    /**
     * @return Marshaller.
     */
    protected Marshaller marshaller() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        grp1.clear();
        grp2.clear();

        for (int i = 0; i < GRID_CNT; i+=2)
            grp1.add(getTestGridName(i));

        for (int i = 1; i < GRID_CNT; i+=2)
            grp2.add(getTestGridName(i));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        extClsLdr1 = new GridTestExternalClassLoader(URLS, NOOP_SERVICE_2_CLS_NAME);
        extClsLdr2 = new GridTestExternalClassLoader(URLS, NOOP_SERVICE_CLS_NAME);

    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        extClsLdr1 = null;
        extClsLdr2 = null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testServiceDeployment() throws Exception {
        try {
            startGrid(0).services().deployLazy(serviceConfig(true));
            startGrid(1).services().deployLazy(serviceConfig(false));
            startGrid(2).services().deployLazy(serviceConfig(true));
            startGrid(3).services().deployLazy(serviceConfig(false));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param firtsGrp First group flag.
     * @return Service configuration.
     * @throws Exception If failed.
     */
    private ServiceConfiguration serviceConfig(final boolean firtsGrp) throws Exception {
        ServiceConfiguration srvCfg = new ServiceConfiguration();

        srvCfg.setNodeFilter(new TestNodeFilter(firtsGrp ? grp1 : grp2));

        Class<Service> srvcCls;

        if (firtsGrp)
            srvcCls = (Class<Service>)extClsLdr1.loadClass(NOOP_SERVICE_CLS_NAME);
        else
            srvcCls = (Class<Service>)extClsLdr2.loadClass(NOOP_SERVICE_2_CLS_NAME);

        Service srvc = srvcCls.newInstance();

        srvCfg.setService(srvc);

        srvCfg.setName("TestDeploymentService" + (firtsGrp ? 1 : 2));

        srvCfg.setMaxPerNodeCount(1);

        return srvCfg;
    }

    /**
     *
     */
    private static class TestNodeFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private Set<String> grp;

        /**
         * @param grp Group.
         */
        private TestNodeFilter(Set<String> grp) {
            this.grp = grp;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("SuspiciousMethodCalls")
        @Override public boolean apply(ClusterNode node) {
            Object gridName = node.attribute(GRID_NAME_ATTR);

            return grp.contains(gridName);
        }
    }
}
