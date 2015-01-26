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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.testframework.junits.common.*;

import java.io.*;
import java.util.*;

import static org.apache.ignite.events.IgniteEventType.*;

/**
 * Tests runtime exception.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "Kernal Self")
public class GridRuntimeExceptionSelfTest extends GridCommonAbstractTest {
    /** */
    private enum FailType {
        /** */
        MAP,

        /** */
        RESULT,

        /** */
        REDUCE,

        /** */
        EXECUTE
    }

    /** */
    public GridRuntimeExceptionSelfTest() {
        super(/*start grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopGrid();
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecuteFailed() throws Exception {
        Ignite ignite = G.ignite(getTestGridName());

        ignite.compute().localDeployTask(GridTaskFailedTestTask.class, GridTaskFailedTestTask.class.getClassLoader());

        ComputeTaskFuture<?> fut =
            executeAsync(ignite.compute(), GridTaskFailedTestTask.class.getName(), FailType.EXECUTE);

        try {
            fut.get();

            assert false;
        }
        catch (IgniteCheckedException e) {
            info("Got expected grid exception: " + e);
        }

        IgniteUuid sesId = fut.getTaskSession().getId();

        // Query for correct events.
        List<IgniteEvent> evts = ignite.events().remoteQuery(new TaskFailedEventFilter(sesId), 0);

        info("Job failed event: " + evts.get(0));

        assert evts.size() == 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testMapFailed() throws Exception {
        Ignite ignite = G.ignite(getTestGridName());

        ignite.compute().localDeployTask(GridTaskFailedTestTask.class, GridTaskFailedTestTask.class.getClassLoader());

        ComputeTaskFuture<?> fut =
            executeAsync(ignite.compute(), GridTaskFailedTestTask.class.getName(), FailType.MAP);

        try {
            fut.get();

            assert false;
        }
        catch (IgniteCheckedException e) {
            info("Got expected grid exception: " + e);
        }

        IgniteUuid sesId = fut.getTaskSession().getId();

        // Query for correct events.
        List<IgniteEvent> evts = ignite.events().remoteQuery(new TaskFailedEventFilter(sesId), 0);

        assert evts.size() == 1;

        info("Task failed event: " + evts.get(0));
    }

    /**
     * @throws Exception If failed.
     */
    public void testResultFailed() throws Exception {
        Ignite ignite = G.ignite(getTestGridName());

        ignite.compute().localDeployTask(GridTaskFailedTestTask.class, GridTaskFailedTestTask.class.getClassLoader());

        ComputeTaskFuture<?> fut =
            executeAsync(ignite.compute(), GridTaskFailedTestTask.class.getName(), FailType.RESULT);

        try {
            fut.get();

            assert false;
        }
        catch (IgniteCheckedException e) {
            info("Got expected grid exception: " + e);
        }

        IgniteUuid sesId = fut.getTaskSession().getId();

        // Query for correct events.
        List<IgniteEvent> evts = ignite.events().remoteQuery(new TaskFailedEventFilter(sesId), 0);

        assert evts.size() == 1;

        info("Task failed event: " + evts.get(0));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReduceFailed() throws Exception {
        Ignite ignite = G.ignite(getTestGridName());

        ignite.compute().localDeployTask(GridTaskFailedTestTask.class, GridTaskFailedTestTask.class.getClassLoader());

        ComputeTaskFuture<?> fut =
            executeAsync(ignite.compute(), GridTaskFailedTestTask.class.getName(), FailType.RESULT);

        try {
            fut.get();

            assert false;
        }
        catch (IgniteCheckedException e) {
            info("Got expected grid exception: " + e);
        }

        IgniteUuid sesId = fut.getTaskSession().getId();

        // Query for correct events.
        List<IgniteEvent> evts = ignite.events().remoteQuery(new TaskFailedEventFilter(sesId), 0);

        assert evts.size() == 1;

        info("Task failed event: " + evts.get(0));
    }

    /** */
    private static class TaskFailedEventFilter implements IgnitePredicate<IgniteEvent> {
        /** */
        private IgniteUuid sesId;

        /**
         * @param sesId Session ID.
         */
        TaskFailedEventFilter(IgniteUuid sesId) {
            this.sesId = sesId;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(IgniteEvent evt) {
            return evt instanceof IgniteTaskEvent &&
                ((IgniteTaskEvent)evt).taskSessionId() != null &&
                ((IgniteTaskEvent)evt).taskSessionId().equals(sesId) &&
                evt.type() == EVT_TASK_FAILED;
        }
    }

    /** */
    private static class GridTaskFailedTestTask extends ComputeTaskAdapter<Serializable, Serializable> {
        /** */
        @IgniteLoggerResource
        private IgniteLogger log;

        /** Ignite instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private FailType failType;

        /** {@inheritDoc} */
        @SuppressWarnings({"ProhibitedExceptionThrown"})
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Serializable arg)
            throws IgniteCheckedException {
            if (log.isInfoEnabled())
                log.info("Mapping job [job=" + this + ", grid=" + subgrid + ", arg=" + arg + ']');

            failType = (FailType)arg;

            if (failType == FailType.MAP)
                throw new RuntimeException("Failed out of map method.");

            Map<ComputeJob, ClusterNode> map = new HashMap<>(2);

            assert subgrid.size() == 1;
            assert subgrid.get(0).id().equals(ignite.configuration().getNodeId());

            map.put(new GridTaskFailedTestJob(null), subgrid.get(0));
            map.put(new GridTaskFailedTestJob(failType), subgrid.get(0));

            return map;
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"ProhibitedExceptionThrown"})
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> received) throws IgniteCheckedException {
            if (failType == FailType.RESULT)
                throw new RuntimeException("Failing out of result method.");

            if (res.getException() != null)
                throw res.getException();

            return ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"ProhibitedExceptionThrown"})
        @Override public Serializable reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            assert results != null;

            if (failType == FailType.REDUCE)
                throw new RuntimeException("Failed out of reduce method.");

            return (Serializable)results;
        }
    }

    /** */
    private static class GridTaskFailedTestJob extends ComputeJobAdapter {
        /** */
        @IgniteLoggerResource
        private IgniteLogger log;

        /** */
        GridTaskFailedTestJob() {
            // No-op.
        }

        /**
         * @param arg Job argument.
         */
        GridTaskFailedTestJob(FailType arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() {
            if (log.isInfoEnabled())
                log.info("Executing job [job=" + this + ", arg=" + argument(0) + ']');

            if (argument(0) != null && argument(0) == FailType.EXECUTE) {
                // Throw exception.
                throw new RuntimeException("GridTaskFailedTestJob expected exception.");
            }

            return true;
        }
    }
}
