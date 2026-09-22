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

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class ServiceLocalStartOrderTest extends GridCommonAbstractTest {
    /** */
    private final int gridCnt = 3;

    /** */
    private boolean staticConfig;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (staticConfig)
            cfg.setServiceConfiguration(serviceConfigs());

        return cfg;
    }

    /** */
    @Test
    public void testStaticConfigDeployment() throws Exception {
        staticConfig = true;

        try {
            startGrids(gridCnt);

            check();
        }
        finally {
            staticConfig = false;
        }
    }

    /** */
    @Test
    public void testRuntimeDeployment() throws Exception {
        IgniteEx ignite = startGrids(gridCnt);

        doTest(ignite);
    }

    /** */
    @Test
    public void testRuntimeDeploymentFromClient() throws Exception {
        startGrids(gridCnt);

        doTest(startClientGrid());
    }

    /** */
    private void doTest(IgniteEx deployFrom) throws Exception {
        deployFrom.services().deployAll(Arrays.asList(serviceConfigs()));

        check();
    }

    /** */
    private void check() throws Exception {
        for (int i = 0; i < 5; i++) {
            for (int node = 0; node < gridCnt; node++) {
                IgniteEx ign = grid(node);

                String srvcName = name(i);

                assertTrue(waitForCondition(() -> ign.services().service(srvcName) != null, 10_000));
            }
        }
    }

    /** */
    private ServiceConfiguration[] serviceConfigs() {
        ServiceConfiguration[] cfgs = new ServiceConfiguration[5];

        for (int i = 0; i < 5; i++) {
            cfgs[i] = new ServiceConfiguration()
                .setName(name(i))
                .setService(new OrderedServiceImpl(i))
                .setLocalStartOrder(i)
                .setMaxPerNodeCount(1)
                .setTotalCount(gridCnt);
        }

        return cfgs;
    }

    /** */
    private static class OrderedServiceImpl implements OrderedService {
        /** */
        private final int order;

        /** */
        private boolean started;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        public OrderedServiceImpl(int order) {
            this.order = order;
        }

        /** {@inheritDoc} */
        @Override public void init() throws Exception {
            for (int i = order - 1; i >= 0; i--) {
                OrderedService srvc = ignite.services().service(name(i));

                assertNotNull("Must be deployed previously: " + i, srvc);
                assertTrue(srvc.started());
            }

            started = true;
        }

        /** {@inheritDoc} */
        @Override public boolean started() {
            return started;
        }
    }

    /** */
    private interface OrderedService extends Service {
        /** */
        boolean started();
    }

    /** */
    private static String name(int i) {
        return "Ordered" + i;
    }
}
