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
import org.apache.ignite.IgniteException;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** */
public class GridServiceDeploymentExceptionPropagationTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** */
    @SuppressWarnings("unused")
    public void testExceptionPropagation() throws Exception {
        try (Ignite srv = startGrid("server")) {

            GridStringLogger log = new GridStringLogger();

            try (Ignite client = startGrid("client", getConfiguration("client").setGridLogger(log).setClientMode(true))) {

                try {
                    client.services().deployClusterSingleton("my-service", new ServiceImpl());
                }
                catch (IgniteException ignored) {
                    assertTrue(log.toString().contains("ServiceImpl init exception"));

                    return; // Exception is what we expect.
                }

                // Fail explicitly if we've managed to get here though we shouldn't have.
                fail("https://issues.apache.org/jira/browse/IGNITE-3392");
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
