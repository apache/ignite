/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.service;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class GridServiceClientNodeTest extends GridCommonAbstractTest {
    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientFailureDetectionTimeout(30000);

        cfg.setClientMode(client);
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

        client = true;

        Ignite ignite = startGrid(3);

        checkDeploy(ignite, "service1");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployFromClientAfterRouterStop1() throws Exception {
        startGrid(0);

        client = true;

        Ignite ignite = startGrid(1);

        client = false;

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

        client = true;

        Ignite ignite = startGrid(1);

        client = false;

        startGrid(2);

        client = true;

        startGrid(3);

        client = false;

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
