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

package org.apache.ignite.internal;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test cancellation of a job that depends on service.
 */
@RunWith(JUnit4.class)
public class ComputeJobCancelWithServiceSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJobCancel() throws Exception {
        Ignite server = startGrid("server");

        server.services().deployNodeSingleton("my-service", new MyService());

        Ignition.setClientMode(true);

        Ignite client = startGrid("client");

        ComputeTaskFuture<Integer> fut = client.compute().executeAsync(new MyTask(), null);

        Thread.sleep(3000);

        server.close();

        assertEquals(42, fut.get().intValue());
    }

    /** */
    private static class MyService implements Service {
        /** */
        private volatile boolean cancelled;

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            cancelled = true;
        }

        /**
         * @return Response.
         */
        public int hello() {
            assertFalse("Service already cancelled!", cancelled);

            return 42;
        }
    }

    /** */
    private static class MyTask extends ComputeTaskSplitAdapter<Object, Integer> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            return Collections.singletonList(new ComputeJobAdapter() {
                @IgniteInstanceResource
                private Ignite ignite;

                @Override public Object execute() throws IgniteException {
                    MyService svc = ignite.services().service("my-service");

                    while (!isCancelled()) {
                        try {
                            Thread.sleep(1000);

                            svc.hello();
                        }
                        catch (InterruptedException ignored) {
                            // No-op.
                        }
                    }

                    assertTrue(isCancelled());

                    return svc.hello();
                }
            });
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<ComputeJobResult> results) {
            assertEquals(1, results.size());

            return results.get(0).getData();
        }
    }
}
