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

package org.apache.ignite.internal.processors.security.service;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.junit.Test;

/** */
public class ServiceStaticConfigTest extends AbstractSecurityTest {
    /** */
    private static final String SVC_NAME = "CounterService";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setAuthenticationEnabled(true);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)));

        ServiceConfiguration srvcCfg = new ServiceConfiguration();

        srvcCfg.setName(SVC_NAME);
        srvcCfg.setMaxPerNodeCount(1);
        srvcCfg.setService(new CounterService());

        cfg.setServiceConfiguration(srvcCfg);

        return cfg;
    }

    /** */
    @Test
    public void testNodeStarted() throws Exception {
        startGrid(0);

        startGrid(1).cluster().state(ClusterState.ACTIVE);

        assertEquals(2, G.allGrids().size());

        for (Ignite ignite : G.allGrids()) {
            CounterService svc = ignite.services().service(SVC_NAME);

            assertNotNull(svc);
            assertNotNull(svc.execLatch);
            assertTrue(svc.execLatch.await(5, TimeUnit.SECONDS));
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** */
    public static class CounterService implements Service {
        /** */
        transient CountDownLatch execLatch;

        /** {@inheritDoc} */
        @Override public void init() throws Exception {
            execLatch = new CountDownLatch(1);
        }

        /** {@inheritDoc} */
        @Override public void execute() throws Exception {
            execLatch.countDown();
        }
    }
}
