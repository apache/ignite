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

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.services.ServiceDeploymentException;
import org.apache.ignite.services.ServiceDeploymentFailuresPolicy;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.services.ServiceDeploymentFailuresPolicy.CANCEL;
import static org.apache.ignite.services.ServiceDeploymentFailuresPolicy.IGNORE;

/**
 * Tests of Ignite services deployment failures policy.
 */
public class IgniteServicesDeploymentFailuresPolicySelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        grid(0).services().cancelAll();
    }

    /**
     * @throws Exception In case of an error.
     */
    public void testServiceDeploymentIgnorePolicy() throws Exception {
        ServiceConfiguration cfg = serviceConfiguration("testIgnorePolicy", IGNORE);

        IgniteEx ignite = grid(0);

        try {
            ignite.services().deploy(cfg);

            fail("Services deployment exception is expected.");
        }
        catch (ServiceDeploymentException ignored) {
            // No-op.
        }

        assertEquals(1, ignite.services().serviceDescriptors().size());

        Map<UUID, Integer> top = ignite.context().service().serviceTopology("testIgnorePolicy", 0);

        assertEquals(2, top.size());
    }

    /**
     * @throws Exception In case of an error.
     */
    public void testServiceDeploymentCancelPolicy() throws Exception {
        ServiceConfiguration cfg = serviceConfiguration("testCancelPolicy", CANCEL);

        IgniteEx ignite = grid(0);

        try {
            ignite.services().deploy(cfg);

            fail("Services deployment exception is expected.");
        }
        catch (ServiceDeploymentException ignored) {
            // No-op.
        }

        assertEquals(0, ignite.services().serviceDescriptors().size());

        assertNull(ignite.services().service("testCancelPolicy"));
    }

    /**
     * @param name Service name.
     * @param plc Service deployment failures policy.
     * @return Service configuration.
     */
    private ServiceConfiguration serviceConfiguration(String name, ServiceDeploymentFailuresPolicy plc) {
        ServiceConfiguration cfg = new ServiceConfiguration();

        cfg.setName(name);
        cfg.setMaxPerNodeCount(1);
        cfg.setPolicy(plc);
        cfg.setService(new PolicyServiceTest());

        return cfg;
    }

    /**
     * Test service implementation.
     */
    private static class PolicyServiceTest implements Service {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            if (ignite.cluster().localNode().order() == 1)
                throw new RuntimeException();
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            // No-op.
        }
    }
}
