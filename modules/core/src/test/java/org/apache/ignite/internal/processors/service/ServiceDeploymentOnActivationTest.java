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
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Check, that services are redeployed, when cluster is deactivated and activated back again.
 */
public class ServiceDeploymentOnActivationTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

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
        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    public void testServiceDeploymentOnActivation() throws Exception {
        Ignite ignite = startGrid(1);

        ignite.cluster().active(true);

        String svcName = "test-service";

        CountDownLatch exeLatch = new CountDownLatch(1);
        CountDownLatch cancelLatch = new CountDownLatch(1);

        DummyService.exeLatch(svcName, exeLatch);
        DummyService.cancelLatch(svcName, cancelLatch);

        ignite.services().deployNodeSingleton(svcName, new DummyService());

        assertTrue(exeLatch.await(10, TimeUnit.SECONDS));

        ignite.cluster().active(false);

        assertTrue(cancelLatch.await(10, TimeUnit.SECONDS));

        exeLatch = new CountDownLatch(1);

        DummyService.exeLatch(svcName, exeLatch);

        ignite.cluster().active(true);

        assertTrue(exeLatch.await(10, TimeUnit.SECONDS));
    }
}
