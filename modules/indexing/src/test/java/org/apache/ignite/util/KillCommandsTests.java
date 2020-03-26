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
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.spi.systemview.view.ServiceView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.internal.processors.service.IgniteServiceProcessor.SVCS_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.util.KillCommandsSQLTest.execute;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * General tests for the cancel command.
 */
class KillCommandsTests {
    /** Service name. */
    public static final String SVC_NAME = "my-svc";

    /** Cache name. */
    public static final String DEFAULT_CACHE_NAME = "default";

    /** Operations timeout. */
    public static final int TIMEOUT = 10_000;

    /** Latch to block compute task execution. */
    private static CountDownLatch computeLatch;

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
     * Test cancel of the transaction.
     *
     * @param cli Client node.
     * @param srvs Server nodes.
     * @param txCanceler Transaction cancel closure.
     */
    public static void doTestCancelTx(IgniteEx cli, List<IgniteEx> srvs, Consumer<String> txCanceler) {
        IgniteCache<Object, Object> cache = cli.cache(DEFAULT_CACHE_NAME);

        int testKey = 42;

        try (Transaction tx = cli.transactions().txStart()) {
            cache.put(testKey, 1);

            List<List<?>> txs = execute(cli, "SELECT xid FROM SYS.TRANSACTIONS");

            assertEquals(1, txs.size());

            String xid = (String)txs.get(0).get(0);

            txCanceler.accept(xid);

            assertThrowsWithCause(tx::commit, IgniteException.class);

            for (int i = 0; i < srvs.size(); i++) {
                txs = execute(srvs.get(i), "SELECT xid FROM SYS.TRANSACTIONS");

                assertEquals(0, txs.size());
            }
        }

        assertNull(cache.get(testKey));
    }

    /**
     * Test cancel of the service.
     *
     * @param startCli Client node to start service.
     * @param killCli Client node to kill service.
     * @param srv Server node.
     * @param svcCanceler Service cancel closure.
     */
    public static void doTestCancelService(IgniteEx startCli, IgniteEx killCli, IgniteEx srv,
        Consumer<String> svcCanceler) throws Exception {
        ServiceConfiguration scfg = new ServiceConfiguration();

        scfg.setName(SVC_NAME);
        scfg.setMaxPerNodeCount(1);
        scfg.setNodeFilter(srv.cluster().predicate());
        scfg.setService(new TestServiceImpl());

        startCli.services().deploy(scfg);

        SystemView<ServiceView> svcView = srv.context().systemView().view(SVCS_VIEW);
        SystemView<ServiceView> killCliSvcView = killCli.context().systemView().view(SVCS_VIEW);

        boolean res = waitForCondition(() -> svcView.size() == 1 && killCliSvcView.size() == 1, TIMEOUT);

        assertTrue(res);

        TestService svc = startCli.services().serviceProxy(SVC_NAME, TestService.class, true);

        assertNotNull(svc);

        svcCanceler.accept(SVC_NAME);

        res = waitForCondition(() -> svcView.size() == 0, TIMEOUT);

        assertTrue(res);
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
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void doTheJob() {
            // No-op.
        }
    }
}
