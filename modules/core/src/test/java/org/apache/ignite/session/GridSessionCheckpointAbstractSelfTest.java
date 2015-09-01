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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskName;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.compute.ComputeTaskSessionScope;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.spi.checkpoint.CheckpointSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Grid session checkpoint self test.
 */
@GridCommonTest(group = "Task Session")
public abstract class GridSessionCheckpointAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    protected static CheckpointSpi spi;

    /** */
    private static final int SPLIT_COUNT = 5;

    /** */
    protected GridSessionCheckpointAbstractSelfTest() {
        super(/*start grid*/false);
    }

     /**
     * @param sesKey Session key.
     * @param globalKey Global key.
     * @param globalState Global state.
     * @throws Exception If check failed.
     */
    private void checkFinishedState(String sesKey, String globalKey, String globalState) throws Exception {
        byte[] serState = spi.loadCheckpoint(sesKey);

        assert serState == null : "Session scope variable is not null: " + Arrays.toString(serState);

        serState = spi.loadCheckpoint(globalKey);

        Marshaller marshaller = getTestResources().getMarshaller();

        assert marshaller != null;

        String state = marshaller.unmarshal(serState, getClass().getClassLoader());

        assert state != null : "Global state is missing: " + globalKey;
        assert state.equals(globalState) : "Invalid state value: " + state;

        spi.removeCheckpoint(globalKey);

        Object cp = spi.loadCheckpoint(globalKey);

        assert cp == null;
    }

    /**
     * @param sesKey Session key.
     * @param sesState Session state.
     * @param globalKey Global key.
     * @param globalState Global state.
     * @param marsh Marshaller.
     * @param cl Class loader.
     * @throws Exception If check failed.
     */
    private static void checkRunningState(String sesKey, String sesState, String globalKey, String globalState,
        Marshaller marsh, ClassLoader cl) throws Exception {
        assert marsh != null;
        assert cl != null;

        byte[] serState = spi.loadCheckpoint(sesKey);

        String state = marsh.unmarshal(serState, cl);

        assert state != null : "Session state is missing: " + sesKey;
        assert state.equals(sesState) : "Invalid state value: " + state;

        serState = spi.loadCheckpoint(globalKey);

        state = marsh.unmarshal(serState, cl);

        assert state != null : "Global state is missing: " + globalKey;
        assert state.equals(globalState) : "Invalid state value: " + state;
    }

    /**
     * @param cfg Configuration.
     * @throws Exception If check failed.
     */
    protected void checkCheckpoints(IgniteConfiguration cfg) throws Exception {
        Ignite ignite = G.start(cfg);

        try {
            ignite.compute().localDeployTask(GridCheckpointTestTask.class, GridCheckpointTestTask.class.getClassLoader());

            ComputeTaskFuture<?> fut = executeAsync(ignite.compute(), "GridCheckpointTestTask", null);

            fut.getTaskSession().saveCheckpoint("future:session:key", "future:session:testval");
            fut.getTaskSession().saveCheckpoint("future:global:key", "future:global:testval",
                ComputeTaskSessionScope.GLOBAL_SCOPE, 0);

            int res = (Integer) fut.get();

            assert res == SPLIT_COUNT : "Invalid result: " + res;

            // Check fut states.
            checkFinishedState("future:session:key", "future:global:key", "future:global:testval");

            // Check states saved by jobs.
            for (int i = 0; i < SPLIT_COUNT; i++)
                checkFinishedState("job:session:key:" + i, "job:global:key:" + i, "job:global:testval:" + i);
            // Check states saved by map(..).
            for (int i = 0; i < SPLIT_COUNT; i++)
                checkFinishedState("map:session:key:" + i, "map:global:key:" + i, "map:global:testval:" + i);
            // Check states saved by reduce(..).
            for (int i = 0; i < SPLIT_COUNT; i++)
                checkFinishedState("reduce:session:key:" + i, "reduce:global:key:" + i, "reduce:global:testval:" + i);
        }
        finally {
            G.stop(getTestGridName(), false);
        }
    }

    /** */
    @ComputeTaskName("GridCheckpointTestTask")
    @ComputeTaskSessionFullSupport
    private static class GridCheckpointTestTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** */
        @TaskSessionResource
        private ComputeTaskSession ses;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override protected Collection<ComputeJobAdapter> split(int gridSize, Object arg) {
            for (int i = 0; i < SPLIT_COUNT; i++) {
                ses.saveCheckpoint("map:session:key:" + i, "map:session:testval:" + i);
                ses.saveCheckpoint("map:global:key:" + i, "map:global:testval:" + i,
                    ComputeTaskSessionScope.GLOBAL_SCOPE, 0);
            }

            Collection<ComputeJobAdapter> jobs = new ArrayList<>(SPLIT_COUNT);

            for (int i = 0; i < SPLIT_COUNT; i++) {
                jobs.add(new ComputeJobAdapter(i) {
                    /** */
                    private static final long serialVersionUID = -9118687978815477993L;

                    /** {@inheritDoc} */
                    @Override public Serializable execute() {
                        ses.saveCheckpoint("job:session:key:" + argument(0), "job:session:testval:" + argument(0));
                        ses.saveCheckpoint("job:global:key:" + argument(0), "job:global:testval:" + argument(0),
                            ComputeTaskSessionScope.GLOBAL_SCOPE, 0);

                        return 1;
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            int res = 0;

            for (ComputeJobResult result : results) {
                res += (Integer)result.getData();
            }

            for (int i = 0; i < SPLIT_COUNT; i++) {
                ses.saveCheckpoint("reduce:session:key:" + i, "reduce:session:testval:" + i);
                ses.saveCheckpoint("reduce:global:key:" + i, "reduce:global:testval:" + i,
                    ComputeTaskSessionScope.GLOBAL_SCOPE, 0);
            }

            // Sleep to let task future store a session attribute.
            try {
                Thread.sleep(200);
            }
            catch (InterruptedException e) {
                throw new IgniteException("Got interrupted during reducing.", e);
            }

            try {
                // Check task and job states.
                for (int i =  0; i < SPLIT_COUNT; i++) {
                    // Check task map state.
                    checkRunningState("map:session:key:" + i, "map:session:testval:" + i, "map:global:key:" + i,
                        "map:global:testval:" + i, ignite.configuration().getMarshaller(), getClass().getClassLoader());

                    // Check task reduce state.
                    checkRunningState("reduce:session:key:" + i, "reduce:session:testval:" + i,
                            "reduce:global:key:" + i, "reduce:global:testval:" + i,
                            ignite.configuration().getMarshaller(), getClass().getClassLoader());

                    // Check task map state.
                    checkRunningState("job:session:key:" + i, "job:session:testval:" + i, "job:global:key:" + i,
                        "job:global:testval:" + i, ignite.configuration().getMarshaller(), getClass().getClassLoader());
                }
            }
            catch (Exception e) {
                throw new IgniteException("Running state check failure.", e);
            }

            return res;
        }
    }
}