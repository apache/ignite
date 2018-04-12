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
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Check, that services are redeployed, when cluster is deactivated and activated back again.
 */
public class ServiceDeploymentOnActivationTest extends GridCommonAbstractTest {
    /** */
    private static final String SERVICE_NAME = "test-service";

    /** */
    private boolean client = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(client);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true).setMaxSize(10 * 1024 * 1024)
            ).setWalMode(WALMode.LOG_ONLY)
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        client = false;

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testServiceDeploymentOnActivation() throws Exception {
        Ignite ignite = startGrid(1);
        checkDeploy(ignite, null);
    }

    /**
     * @throws Exception if failed.
     */
    public void testServiceDeploymentOnClientOnActivation() throws Exception {
        Ignite ignite = startGrid(1);

        client = true;
        startGrid(2);

        checkDeploy(ignite, ignite.cluster().forClients().predicate());
    }

    /**
     * @param ignite Ignite.
     * @param nodeFilter Node filter.
     */
    private void checkDeploy(Ignite ignite, IgnitePredicate<ClusterNode> nodeFilter) throws InterruptedException {
        ignite.cluster().active(true);

        CountDownLatch exeLatch = new CountDownLatch(1);
        CountDownLatch cancelLatch = new CountDownLatch(1);

        DummyService.exeLatch(SERVICE_NAME, exeLatch);
        DummyService.cancelLatch(SERVICE_NAME, cancelLatch);

        ServiceConfiguration srvcCfg = new ServiceConfiguration();
        srvcCfg.setName(SERVICE_NAME);
        srvcCfg.setMaxPerNodeCount(1);
        srvcCfg.setNodeFilter(nodeFilter);
        srvcCfg.setService(new DummyService());

        ignite.services().deploy(srvcCfg);

        assertTrue(exeLatch.await(10, TimeUnit.SECONDS));

        ignite.cluster().active(false);

        assertTrue(cancelLatch.await(10, TimeUnit.SECONDS));

        exeLatch = new CountDownLatch(1);

        DummyService.exeLatch(SERVICE_NAME, exeLatch);

        ignite.cluster().active(true);

        assertTrue(exeLatch.await(10, TimeUnit.SECONDS));
    }
}
