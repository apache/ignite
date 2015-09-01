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

package org.apache.ignite.session;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 *
 */
@SuppressWarnings({"CatchGenericClass"})
@GridCommonTest(group = "Task Session")
public class GridSessionJobWaitTaskAttributeSelfTest extends GridCommonAbstractTest {
    /** */
    public static final int SPLIT_COUNT = 5;

    /** */
    public static final int EXEC_COUNT = 5;

    /** */
    public GridSessionJobWaitTaskAttributeSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(discoSpi);

        c.setPublicThreadPoolSize(SPLIT_COUNT * EXEC_COUNT);

        return c;
    }

    /**
     * @throws Exception if failed.
     */
    public void testSetAttribute() throws Exception {
        for (int i = 0; i < EXEC_COUNT; i++)
            checkTask(i);
    }

    /**
     * @throws Exception if failed.
     */
    public void testMultiThreaded() throws Exception {
        final GridThreadSerialNumber sNum = new GridThreadSerialNumber();

        final AtomicBoolean failed = new AtomicBoolean(false);

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                int num = sNum.get();

                try {
                    checkTask(num);
                }
                catch (Throwable e) {
                    error("Failed to execute task.", e);

                    failed.set(true);
                }
            }
        }, EXEC_COUNT, "grid-session-test");

        assert !failed.get() : "Multithreaded test failed";
    }

    /**
     * @param num Number.
     * @throws IgniteCheckedException if failed.
     */
    private void checkTask(int num) throws IgniteCheckedException {
        Ignite ignite = G.ignite(getTestGridName());

        ComputeTaskFuture<?> fut = executeAsync(ignite.compute(), GridTaskSessionTestTask.class.getName(), null);

        int exp = SPLIT_COUNT - 1;

        Object res = fut.get();

        assert (Integer)res == exp : "Invalid result [expected=" + exp +
            ", actual=" + res + ", iter=" + num + ", fut=" + fut + ']';
    }


    /**
     *
     */
    @ComputeTaskSessionFullSupport
    private static class GridTaskSessionTestTask extends ComputeTaskSplitAdapter<Serializable, Integer> {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        @TaskSessionResource
        private ComputeTaskSession taskSes;

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Serializable arg) {
            assert taskSes != null;

            if (log.isInfoEnabled())
                log.info("Splitting job [job=" + this + ", gridSize=" + gridSize + ", arg=" + arg + ']');

            Collection<ComputeJob> jobs = new ArrayList<>(SPLIT_COUNT);

            for (int i = 1; i <= SPLIT_COUNT; i++) {
                jobs.add(new ComputeJobAdapter(i) {
                    @Override public Serializable execute() {
                        assert taskSes != null;

                        if (log.isInfoEnabled())
                            log.info("Computing job [job=" + this + ", arg=" + argument(0) + ']');

                        if (this.<Integer>argument(0) != 1) {
                            try {
                                String val = (String)taskSes.waitForAttribute("testName", 20000);

                                if (log.isInfoEnabled())
                                    log.info("Received attribute 'testName': " + val);

                                if ("testVal".equals(val))
                                    return 1;

                                fail("Invalid test session value: " + val);
                            }
                            catch (InterruptedException e) {
                                throw new IgniteException("Failed to get attribute due to interruption.", e);
                            }
                        }

                        return 0;
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult result, List<ComputeJobResult> received) {
            if (result.getException() != null)
                throw result.getException();

            if (received.size() == 1) {
                log.info("Got result from setting job: " + result);

                taskSes.setAttribute("testName", "testVal");
            }
            else
                log.info("Got result from waiting job: " + result);

            return received.size() == SPLIT_COUNT ? ComputeJobResultPolicy.REDUCE : ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<ComputeJobResult> results) {
            if (log.isInfoEnabled())
                log.info("Reducing job [job=" + this + ", results=" + results + ']');

            if (results.size() < SPLIT_COUNT)
                fail("Results size is less than split count: " + results.size());

            int sum = 0;

            for (ComputeJobResult res : results) {
                if (res.getData() == null)
                    fail("Got null result data: " + res);
                else
                    log.info("Reducing result: " + res.getData());

                sum += (Integer)res.getData();
            }

            return sum;
        }
    }
}