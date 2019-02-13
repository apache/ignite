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

package org.apache.ignite.internal.processor.security.compute.closure;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteRunnable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Testing permissions when the service task is executed cache operations on remote node.
 */
@RunWith(JUnit4.class)
public class ExecutorServiceRemoteSecurityContextCheckTest extends AbstractComputeRemoteSecurityContextCheckTest {
    /**
     *
     */
    @Test
    public void test() {
        runAndCheck(grid(SRV_INITIATOR));
        runAndCheck(grid(CLNT_INITIATOR));
    }

    /**
     * Performs test case.
     */
    private void runAndCheck(IgniteEx initiator) {
        runAndCheck(initiator,
            () -> {
                for (UUID nodeId : transitions()) {
                    ExecutorService svc = initiator.executorService(initiator.cluster().forNodeId(nodeId));

                    try {
                        svc.submit(new TestIgniteRunnable(endpoints())).get();
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
    }

    /**
     * Runnable for tests.
     */
    static class TestIgniteRunnable implements IgniteRunnable {
        /** Collection of endpoint node ids. */
        private final Collection<UUID> endpoints;

        /**
         * @param endpoints Collection of endpoint node ids.
         */
        public TestIgniteRunnable(Collection<UUID> endpoints) {
            this.endpoints = endpoints;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            Ignite ignite = Ignition.localIgnite();

            VERIFIER.verify(ignite);

            if (!endpoints.isEmpty()) {
                try {
                    for (UUID nodeId : endpoints) {
                        ignite.executorService(ignite.cluster().forNodeId(nodeId))
                            .submit(new TestIgniteRunnable(Collections.emptyList()))
                            .get();
                    }
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
