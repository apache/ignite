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

import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

/** */
public class ServiceDeploymentAfterDeactivationTest extends GridCommonAbstractTest {
    /** */
    private static final String SERVICE_NAME = "test-service";

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        client = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployFromClientAfterReactivation() throws Exception {
        Ignite srvNode = startGrid(0);

        client = true;

        Ignite clientNode = startGrid(1);

        srvNode.cluster().active(false);
        srvNode.cluster().active(true);

        IgniteFuture<Void> depFut = clientNode.services().deployClusterSingletonAsync(SERVICE_NAME, new DummyService());

        depFut.get(10, TimeUnit.SECONDS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployFromClientAfterActivation() throws Exception {
        Ignite srvNode = startGrid(0);

        srvNode.cluster().active(false);

        client = true;

        Ignite clientNode = startGrid(1);

        srvNode.cluster().active(true);

        IgniteFuture<Void> depFut = clientNode.services().deployClusterSingletonAsync(SERVICE_NAME, new DummyService());

        depFut.get(10, TimeUnit.SECONDS);
    }
}
