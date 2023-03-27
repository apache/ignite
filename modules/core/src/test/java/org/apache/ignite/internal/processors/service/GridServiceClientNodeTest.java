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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class GridServiceClientNodeTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientFailureDetectionTimeout(30000);
        cfg.setMetricsUpdateFrequency(1000);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployFromClient() throws Exception {
        startGrids(3);

        Ignite ignite = startClientGrid(3);

        checkDeploy(ignite, "service1");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployFromClientAfterRouterStop1() throws Exception {
        startGrid(0);

        Ignite ignite = startClientGrid(1);

        startGrid(2);

        U.sleep(1000);

        stopGrid(0);

        awaitPartitionMapExchange();

        checkDeploy(ignite, "service1");

        startGrid(3);

        for (int i = 0; i < 10; i++)
            checkDeploy(ignite, "service2-" + i);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployFromClientAfterRouterStop2() throws Exception {
        startGrid(0);

        Ignite ignite = startClientGrid(1);

        startGrid(2);

        startClientGrid(3);

        startGrid(4);

        U.sleep(1000);

        stopGrid(0);

        awaitPartitionMapExchange();

        checkDeploy(ignite, "service1");

        startGrid(5);

        for (int i = 0; i < 10; i++)
            checkDeploy(ignite, "service2-" + i);
    }

    /**
     * @param client Client node.
     * @param svcName Service name.
     * @throws Exception If failed.
     */
    private void checkDeploy(Ignite client, String svcName) throws Exception {
        assertTrue(client.configuration().isClientMode());

        CountDownLatch latch = new CountDownLatch(1);

        DummyService.exeLatch(svcName, latch);

        client.services().deployClusterSingleton(svcName, new DummyService());

        assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
    }
}
