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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSessionAttributeListener;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 *
 */
@GridCommonTest(group = "Task Session")
public class GridSessionSetJobAttributeWaitListenerSelfTest extends GridCommonAbstractTest {
    /** */
    public static final int SPLIT_COUNT = 5;

    /** */
    private static final long WAIT_TIME = 20000;

    /** */
    private static volatile CountDownLatch startSignal;

    /** */
    public GridSessionSetJobAttributeWaitListenerSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(discoSpi);

        c.setPublicThreadPoolSize(SPLIT_COUNT * 2);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSetAttribute() throws Exception {
        Ignite ignite = G.ignite(getTestGridName());

        ignite.compute().localDeployTask(GridTaskSessionTestTask.class, GridTaskSessionTestTask.class.getClassLoader());

        for (int i = 0; i < 5; i++) {
            refreshInitialData();

            IgniteCompute comp = ignite.compute().withAsync();

            comp.execute(GridTaskSessionTestTask.class.getName(), null);

            ComputeTaskFuture<?> fut = comp.future();

            assert fut != null;

            try {
                // Wait until jobs begin execution.
                boolean await = startSignal.await(WAIT_TIME, TimeUnit.MILLISECONDS);

                assert await : "Jobs did not start.";

                Object res = fut.get();

                assert (Integer)res == SPLIT_COUNT : "Invalid result [i=" + i + ", fut=" + fut + ']';
            }
            finally {
                // We must wait for the jobs to be sure that they have completed
                // their execution since they use static variable (shared for the tests).
                fut.get();
            }
        }
    }

    /** */
    private void refreshInitialData() {
        startSignal = new CountDownLatch(SPLIT_COUNT);
    }

    /**
     *
     */
    @ComputeTaskSessionFullSupport
    public static class GridTaskSessionTestTask extends ComputeTaskSplitAdapter<Serializable, Integer> {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        @TaskSessionResource
        private ComputeTaskSession taskSes;

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Serializable arg) {
            if (log.isInfoEnabled())
                log.info("Splitting job [job=" + this + ", gridSize=" + gridSize + ", arg=" + arg + ']');

            Collection<ComputeJob> jobs = new ArrayList<>(SPLIT_COUNT);

            for (int i = 1; i <= SPLIT_COUNT; i++) {
                jobs.add(new ComputeJobAdapter(i) {
                    @SuppressWarnings({"UnconditionalWait"})
                    public Serializable execute() {
                        assert taskSes != null;

                        if (log.isInfoEnabled())
                            log.info("Computing job [job=" + this + ", arg=" + argument(0) + ']');

                        startSignal.countDown();

                        try {
                            if (startSignal.await(WAIT_TIME, TimeUnit.MILLISECONDS) == false)
                                fail();

                            GridTaskSessionAttributeTestListener lsnr =
                                new GridTaskSessionAttributeTestListener(log);

                            taskSes.addAttributeListener(lsnr, false);

                            if (log.isInfoEnabled())
                                log.info("Set attribute 'testName'.");

                            taskSes.setAttribute("testName", "testVal");

                            synchronized (lsnr) {
                                lsnr.wait(WAIT_TIME);
                            }

                            return lsnr.getAttributes().size() == 0 ? 0 : 1;
                        }
                        catch (InterruptedException e) {
                            throw new IgniteException("Failed to wait for listener due to interruption.", e);
                        }
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult result, List<ComputeJobResult> received) {
            if (result.getException() != null)
                throw result.getException();

            return received.size() == SPLIT_COUNT ? ComputeJobResultPolicy.REDUCE : ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<ComputeJobResult> results) {
            if (log.isInfoEnabled())
                log.info("Reducing job [job=" + this + ", results=" + results + ']');

            if (results.size() < SPLIT_COUNT)
                fail();

            int sum = 0;

            for (ComputeJobResult result : results) {
                if (result.getData() != null)
                    sum += (Integer)result.getData();
            }

            return sum;
        }
    }

    /**
     *
     */
    private static class GridTaskSessionAttributeTestListener implements ComputeTaskSessionAttributeListener {
        /** */
        private Map<Object, Object> attrs = new HashMap<>();

        /** */
        private IgniteLogger log;

        /**
         * @param log Grid logger.
         */
        GridTaskSessionAttributeTestListener(IgniteLogger log) {
            assert log != null;

            this.log = log;
        }

        /** {@inheritDoc} */
        @Override public synchronized void onAttributeSet(Object key, Object val) {
            assert key != null;

            if (log.isInfoEnabled())
                log.info("Received attribute [name=" + key + ", val=" + val + ']');

            attrs.put(key, val);

            notifyAll();
        }

        /**
         * Getter for property 'attrs'.
         *
         * @return Attributes map.
         */
        public synchronized Map<Object, Object> getAttributes() {
            return attrs;
        }
    }
}