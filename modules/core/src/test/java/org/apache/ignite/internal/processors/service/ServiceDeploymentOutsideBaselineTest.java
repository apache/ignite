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
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** */
public class ServiceDeploymentOutsideBaselineTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String SERVICE_NAME = "test-service";

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

        if (srvcCfg != null)
            cfg.setServiceConfiguration(srvcCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
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
    public void testDeployOutsideBaseline() throws Exception {
        checkDeployment(true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployOutsideBaselineNoPersistence() throws Exception {
        checkDeployment(false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployOutsideBaselineStatic() throws Exception {
        checkDeployment(true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployOutsideBaselineStaticNoPersistence() throws Exception {
        checkDeployment(false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployOnNodeAddedToBlt() throws Exception {
        persistence = true;

        Ignite insideNode = startGrid(0);

        IgniteCluster cluster = insideNode.cluster();

        cluster.active(true);

        Ignite outsideNode = startGrid(1);

        cluster.setBaselineTopology(cluster.topologyVersion());

        CountDownLatch exeLatch = new CountDownLatch(1);

        DummyService.exeLatch(SERVICE_NAME, exeLatch);

        IgniteFuture<Void> depFut = outsideNode.services().deployClusterSingletonAsync(SERVICE_NAME, new DummyService());

        depFut.get(10, TimeUnit.SECONDS);

        assertTrue(exeLatch.await(10, TimeUnit.SECONDS));
    }

    /**
     * @param persistence If {@code true}, then persistence will be enabled.
     * @param staticDeploy If {@code true}, then static deployments will be used instead of a dynamic one.
     * @throws Exception If failed.
     */
    private void checkDeployment(boolean persistence, boolean staticDeploy) throws Exception {
        this.persistence = persistence;

        Ignite insideNode = startGrid(0);

        if (persistence)
            insideNode.cluster().active(true);
        else {
            IgniteCluster cluster = insideNode.cluster();

            cluster.setBaselineTopology(cluster.topologyVersion());
        }

        CountDownLatch exeLatch = new CountDownLatch(1);

        DummyService.exeLatch(SERVICE_NAME, exeLatch);

        if (staticDeploy) {
            srvcCfg = getServiceConfiguration();

            startGrid(1);
        }
        else {
            Ignite outsideNode = startGrid(1);

            IgniteFuture<Void> depFut = outsideNode.services().deployClusterSingletonAsync(SERVICE_NAME, new DummyService());

            depFut.get(10, TimeUnit.SECONDS);
        }

        assertTrue(exeLatch.await(10, TimeUnit.SECONDS));
    }

    /**
     * @return Test service configuration;
     */
    private ServiceConfiguration getServiceConfiguration() {
        ServiceConfiguration srvcCfg = new ServiceConfiguration();
        srvcCfg.setName(SERVICE_NAME);
        srvcCfg.setService(new DummyService());
        srvcCfg.setTotalCount(1);

        return srvcCfg;
    }
}
