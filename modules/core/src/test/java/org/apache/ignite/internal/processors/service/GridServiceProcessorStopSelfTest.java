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
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.Ignition;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests that {@link GridServiceProcessor} completes deploy/undeploy futures during node stop.
 */
public class GridServiceProcessorStopSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testStopDuringDeployment() throws Exception {
        final CountDownLatch depLatch = new CountDownLatch(1);

        final CountDownLatch finishLatch = new CountDownLatch(1);

        final Ignite ignite = startGrid(0);

        Thread t = new Thread(new Runnable() {
            @Override public void run() {
                IgniteServices svcs = ignite.services();

                IgniteServices services = svcs.withAsync();

                services.deployClusterSingleton("myClusterSingletonService", new TestServiceImpl());

                depLatch.countDown();

                try {
                    services.future().get();
                }
                catch (IgniteException e) {
                    finishLatch.countDown();
                }
                catch (Throwable e) {
                    log.error("Service deployment error: ", e);
                }
            }
        });

        t.start();

        depLatch.await();

        Ignition.stopAll(true);

        assertTrue("Deploy future isn't completed", finishLatch.await(15, TimeUnit.SECONDS));
    }

    /**
     * Simple map service.
     */
    public interface TestService {
    }

    /**
     *
     */
    public class TestServiceImpl implements Service, TestService {
        /** Serial version UID. */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            // No-op.
        }
    }
}
