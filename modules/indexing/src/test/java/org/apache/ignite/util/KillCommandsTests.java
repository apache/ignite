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

package org.apache.ignite.util;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.spi.systemview.view.ServiceView;
import org.apache.ignite.spi.systemview.view.SystemView;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.processors.service.IgniteServiceProcessor.SVCS_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.util.KillCommandsSQLTest.execute;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * General tests for the cancel command.
 */
class KillCommandsTests {
    /** Service name. */
    public static final String SVC_NAME = "my-svc";

    /** Operations timeout. */
    public static final int TIMEOUT = 10_000;

    /** Latch to block compute task execution. */
    private static CountDownLatch computeLatch;

    /** Service start barrier. */
    private static volatile CyclicBarrier svcStartBarrier;

    /** Service cancel barriers. */
    private static volatile CyclicBarrier svcCancelBarrier;

    /**
     * Test cancel of the compute task.
     *
     * @param cli Client node that starts tasks.
     * @param srvs Server nodes.
     * @param qryCanceler Query cancel closure.
     */
    public static void doTestCancelComputeTask(IgniteEx cli, List<IgniteEx> srvs, Consumer<String> qryCanceler)
        throws Exception {
        computeLatch = new CountDownLatch(1);

        IgniteFuture<Collection<Integer>> fut = cli.compute().broadcastAsync(() -> {
            computeLatch.await();

            return 1;
        });

        try {
            String[] id = new String[1];

            boolean res = waitForCondition(() -> {
                for (IgniteEx srv : srvs) {
                    List<List<?>> tasks = execute(srv, "SELECT SESSION_ID FROM SYS.JOBS");

                    if (tasks.size() == 1)
                        id[0] = (String)tasks.get(0).get(0);
                    else
                        return false;
                }

                return true;
            }, TIMEOUT);

            assertTrue(res);

            qryCanceler.accept(id[0]);

            for (IgniteEx srv : srvs) {
                res = waitForCondition(() -> {
                    List<List<?>> tasks = execute(srv, "SELECT SESSION_ID FROM SYS.JOBS");

                    return tasks.isEmpty();
                }, TIMEOUT);

                assertTrue(srv.configuration().getIgniteInstanceName(), res);
            }

            assertThrowsWithCause(() -> fut.get(TIMEOUT), IgniteException.class);
        } finally {
            computeLatch.countDown();
        }
    }

    /**
     * Test cancel of the service.
     *
     * @param cli Client node.
     * @param srv Server node.
     * @param svcCanceler Service cancel closure.
     */
    public static void doTestCancelService(IgniteEx cli, IgniteEx srv, Consumer<String> svcCanceler) throws Exception {
        ServiceConfiguration scfg = new ServiceConfiguration();

        scfg.setName(SVC_NAME);
        scfg.setMaxPerNodeCount(1);
        scfg.setNodeFilter(n -> n.id().equals(srv.localNode().id()));
        scfg.setService(new TestServiceImpl());

        cli.services().deploy(scfg);

        SystemView<ServiceView> svcView = srv.context().systemView().view(SVCS_VIEW);

        boolean res = waitForCondition(() -> svcView.size() == 1, TIMEOUT);

        assertTrue(res);

        TestService svc = cli.services().serviceProxy("my-svc", TestService.class, true);

        assertNotNull(svc);

        svcStartBarrier = new CyclicBarrier(2);
        svcCancelBarrier = new CyclicBarrier(2);

        IgniteInternalFuture<?> fut = runAsync(svc::doTheJob);

        svcStartBarrier.await(TIMEOUT, MILLISECONDS);

        svcCanceler.accept(SVC_NAME);

        res = waitForCondition(() -> svcView.size() == 0, TIMEOUT);

        assertTrue(res);

        fut.get(TIMEOUT);
    }

    /** */
    public interface TestService extends Service {
        /** */
        public void doTheJob();
    }

    /** */
    public static class TestServiceImpl implements TestService {
        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            try {
                if (svcCancelBarrier != null)
                    svcCancelBarrier.await(TIMEOUT, MILLISECONDS);
            }
            catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void doTheJob() {
            try {
                svcStartBarrier.await(TIMEOUT, MILLISECONDS);

                svcCancelBarrier.await(TIMEOUT, MILLISECONDS);
            }
            catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
