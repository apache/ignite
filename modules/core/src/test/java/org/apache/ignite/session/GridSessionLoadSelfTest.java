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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Task session load self test.
 */
@GridCommonTest(group = "Task Session")
public class GridSessionLoadSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int THREAD_CNT = 40;

    /** */
    private static final int EXEC_CNT = 10;

    /** */
    private boolean locMarsh;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.setMarshalLocalJobs(locMarsh);
        c.setPeerClassLoadingEnabled(false);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSessionLoad() throws Exception {
        locMarsh = true;

        checkSessionLoad();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSessionLoadNoLocalMarshalling() throws Exception {
        locMarsh = false;

        checkSessionLoad();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkSessionLoad() throws Exception {
        final Ignite ignite = grid(0);

        assert ignite != null;
        assert ignite.cluster().nodes().size() == 2;

        info("Thread count: " + THREAD_CNT);

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                ComputeTaskFuture f = null;

                try {
                    for (int i = 0; i < EXEC_CNT; i++)
                        assertEquals(Boolean.TRUE,
                            (f = executeAsync(ignite.compute().withName("task-name"),
                                SessionLoadTestTask.class,
                                ignite.cluster().nodes().size() * 2)).get(20000));
                }
                catch (Exception e) {
                    U.error(log, "Task failed: " +
                        f != null ? f.getTaskSession().getId() : "N/A", e);

                    throw e;
                }
                finally {
                    info("Thread finished.");
                }

                return null;
            }
        }, THREAD_CNT, "grid-load-test-thread");
    }

    /**
     *
     */
    @ComputeTaskSessionFullSupport
    private static class SessionLoadTestTask extends ComputeTaskAdapter<Integer, Boolean> {
        /** */
        @TaskSessionResource
        private ComputeTaskSession taskSes;

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        private Map<String, Integer> params;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Integer arg) {
            assert taskSes != null;
            assert arg != null;
            assert arg > 1;

            Map<SessionLoadTestJob, ClusterNode> map = new HashMap<>(subgrid.size());

            Iterator<ClusterNode> iter = subgrid.iterator();

            Random rnd = new Random();

            params = new HashMap<>(arg);

            for (int i = 0; i < arg; i++) {
                // Recycle iterator.
                if (!iter.hasNext())
                    iter = subgrid.iterator();

                String paramName = UUID.randomUUID().toString();

                int paramVal = rnd.nextInt();

                taskSes.setAttribute(paramName, paramVal);

                map.put(new SessionLoadTestJob(paramName), iter.next());

                params.put(paramName, paramVal);

                if (log.isDebugEnabled())
                    log.debug("Set session attribute [name=" + paramName + ", value=" + paramVal + ']');
            }

            return map;
        }

        /** {@inheritDoc} */
        @Override public Boolean reduce(List<ComputeJobResult> results) {
            assert taskSes != null;
            assert results != null;
            assert params != null;
            assert !params.isEmpty();
            assert results.size() == params.size();

            if (log.isDebugEnabled())
                log.debug("Reducing: " + params);

            Map<String, Integer> receivedParams = new HashMap<>();

            boolean allAttrReceived = false;

            for (int i = 0; i < 3 && !allAttrReceived; i++) {
                allAttrReceived = true;

                for (Map.Entry<String, Integer> entry : params.entrySet()) {
                    Serializable attr = taskSes.getAttribute(entry.getKey());

                    assert attr != null;

                    int newVal = (Integer)attr;

                    receivedParams.put(entry.getKey(), newVal);

                    // New value is expected to be +1 to argument value.
                    if (newVal != entry.getValue() + 1)
                        allAttrReceived = false;
                }

                if (!allAttrReceived) {
                    try {
                        U.sleep(1000);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        throw new IgniteException(e);
                    }
                }
            }

            if (log.isDebugEnabled()) {
                for (Map.Entry<String, Integer> entry : receivedParams.entrySet())
                    log.debug("Received session attribute value [name=" + entry.getKey() + ", val=" + entry.getValue()
                        + ", expected=" + (params.get(entry.getKey()) + 1) + ']');
            }

            return allAttrReceived;
        }
    }

    /**
     *
     */
    private static class SessionLoadTestJob extends ComputeJobAdapter {
        /** */
        @TaskSessionResource
        private ComputeTaskSession taskSes;

        /** */
        @LoggerResource
        private IgniteLogger log;

        /**
         * @param arg Argument.
         */
        private SessionLoadTestJob(String arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() {
            assert taskSes != null;
            assert argument(0) != null;

            Serializable ser = taskSes.getAttribute(argument(0));

            assert ser != null;

            int val = (Integer)ser + 1;

            if (log.isDebugEnabled())
                log.debug("Executing session load job: " + val);

            // Generate garbage.
            for (int i = 0; i < 10; i++)
                taskSes.setAttribute(argument(0), i);

            // Set final value (+1 to original value).
            taskSes.setAttribute(argument(0), val);

            if (log.isDebugEnabled())
                log.debug("Set session attribute [name=" + argument(0) + ", value=" + val + ']');

            return val;
        }
    }
}