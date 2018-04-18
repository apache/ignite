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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** */
public class ServiceDeploymentOnActivationTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String SERVICE_NAME = "test-service";

    /** */
    private static final IgnitePredicate<ClusterNode> CLIENT_FILTER = new IgnitePredicate<ClusterNode>() {
        @Override public boolean apply(ClusterNode node) {
            return node.isClient();
        }
    };

    /** */
    private boolean client;

    /** */
    private boolean persistence;

    /** */
    private ServiceConfiguration srvcCfg;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        discoverySpi.setIpFinder(IP_FINDER);
        cfg.setDiscoverySpi(discoverySpi);

        cfg.setClientMode(client);

        if (srvcCfg != null)
            cfg.setServiceConfiguration(srvcCfg);

        if (persistence) {
            cfg.setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                            .setMaxSize(10 * 1024 * 1024)
                    ).setWalMode(WALMode.LOG_ONLY)
            );
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        client = false;
        persistence = false;
        srvcCfg = null;

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testServersWithPersistence() throws Exception {
        persistence = true;

        checkRedeployment(2, 0, F.alwaysTrue(), 2, false, true);
    }

    /**
     * @throws Exception if failed.
     */
    public void testClientsWithPersistence() throws Exception {
        persistence = true;

        checkRedeployment(2, 2, CLIENT_FILTER, 2, false, true);
    }

    /**
     * @throws Exception if failed.
     */
    public void testServersWithoutPersistence() throws Exception {
        persistence = false;

        checkRedeployment(2, 0, F.alwaysTrue(), 2, false, false);
    }

    /**
     * @throws Exception if failed.
     */
    public void testClientsWithoutPersistence() throws Exception {
        persistence = false;

        checkRedeployment(2, 2, CLIENT_FILTER, 2, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testServersStaticConfigWithPersistence() throws Exception {
        persistence = true;

        checkRedeployment(2, 0, F.alwaysTrue(), 2, true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientsStaticConfigWithPersistence() throws Exception {
        persistence = true;

        checkRedeployment(2, 2, CLIENT_FILTER, 2, true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testServersStaticConfigWithoutPersistence() throws Exception {
        persistence = false;

        checkRedeployment(2, 0, F.alwaysTrue(), 2, true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testClientsStaticConfigWithoutPersistence() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-8279");

        persistence = false;

        checkRedeployment(2, 2, CLIENT_FILTER, 2, true, true);
    }

    /**
     * @param srvsNum Number of server nodes to start.
     * @param clientsNum Number of client nodes to start.
     * @param nodeFilter Node filter.
     * @param deps Expected number of deployed services.
     * @param isStatic Static or dynamic service deployment is used.
     * @param expRedep {@code true} if services should be redeployed on activation. {@code false} otherwise.
     * @throws Exception If failed.
     */
    private void checkRedeployment(int srvsNum, int clientsNum, IgnitePredicate<ClusterNode> nodeFilter, int deps,
        boolean isStatic, boolean expRedep) throws Exception {

        if (isStatic)
            srvcCfg = getServiceConfiguration(nodeFilter);

        CountDownLatch exeLatch = new CountDownLatch(deps);
        CountDownLatch cancelLatch = new CountDownLatch(deps);

        DummyService.exeLatch(SERVICE_NAME, exeLatch);
        DummyService.cancelLatch(SERVICE_NAME, cancelLatch);

        for (int i = 0; i < srvsNum; i++)
            startGrid(i);

        client = true;

        for (int i = 0; i < clientsNum; i++)
            startGrid(srvsNum + i);

        Ignite ignite = grid(0);

        ignite.cluster().active(true);

        if (!isStatic) {
            ServiceConfiguration srvcCfg = getServiceConfiguration(nodeFilter);

            ignite.services().deploy(srvcCfg);
        }

        assertTrue(exeLatch.await(10, TimeUnit.SECONDS));

        ignite.cluster().active(false);

        assertTrue(cancelLatch.await(10, TimeUnit.SECONDS));

        exeLatch = new CountDownLatch(expRedep ? deps : 1);

        DummyService.exeLatch(SERVICE_NAME, exeLatch);

        ignite.cluster().active(true);

        if (expRedep)
            assertTrue(exeLatch.await(10, TimeUnit.SECONDS));
        else
            assertFalse(exeLatch.await(1, TimeUnit.SECONDS));
    }

    /**
     * @param nodeFilter Node filter.
     * @return Service configuration.
     */
    private ServiceConfiguration getServiceConfiguration(IgnitePredicate<ClusterNode> nodeFilter) {
        ServiceConfiguration srvcCfg = new ServiceConfiguration();
        srvcCfg.setName(SERVICE_NAME);
        srvcCfg.setMaxPerNodeCount(1);
        srvcCfg.setNodeFilter(nodeFilter);
        srvcCfg.setService(new DummyService());
        return srvcCfg;
    }
}
