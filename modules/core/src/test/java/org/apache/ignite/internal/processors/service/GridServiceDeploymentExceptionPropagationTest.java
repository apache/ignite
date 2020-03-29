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

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.services.ServiceDeploymentException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/** */
public class GridServiceDeploymentExceptionPropagationTest extends GridCommonAbstractTest {
    /** */
    @Before
    public void check() {
        Assume.assumeTrue(isEventDrivenServiceProcessorEnabled());
    }

    /** */
    @Test
    public void testExceptionPropagation() throws Exception {
        try (IgniteEx srv = startGrid("server")) {
            try (Ignite client = startClientGrid("client", getConfiguration("client"))) {
                final String srvcName = "my-service";

                try {
                    client.services().deployClusterSingleton(srvcName, new ServiceImpl());

                    fail("Deployment exception has been expected.");
                }
                catch (ServiceDeploymentException ex) {
                    String errMsg = ex.getSuppressed()[0].getMessage();

                    // Check that message contains cause node id
                    assertTrue(errMsg.contains(srv.cluster().localNode().id().toString()));

                    // Check that message contains service name
                    assertTrue(errMsg.contains(srvcName));

                    Throwable cause = ex.getSuppressed()[0].getCause();

                    // Check that error's cause contains users message
                    assertTrue(cause.getMessage().contains("ServiceImpl init exception"));
                }
            }
        }
    }

    /**
     * Simple service implementation throwing an exception on init.
     * Doesn't even try to do anything useful because what we're testing here is failure.
     */
    private static class ServiceImpl implements Service {
        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            throw new RuntimeException("ServiceImpl init exception");
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            // No-op.
        }
    }
}
