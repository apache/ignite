/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.TaskEvent;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.events.EventType.EVT_TASK_FAILED;

/**
 * Tests runtime exception.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "Kernal Self")
@RunWith(JUnit4.class)
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

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExecuteFailed() throws Exception {
        Ignite ignite = G.ignite(getTestIgniteInstanceName());

        ignite.compute().localDeployTask(GridTaskFailedTestTask.class, GridTaskFailedTestTask.class.getClassLoader());

        ComputeTaskFuture<?> fut =
            executeAsync(ignite.compute(), GridTaskFailedTestTask.class.getName(), FailType.EXECUTE);

        try {
            fut.get();

            assert false;
        }
        catch (IgniteException e) {
            info("Got expected grid exception: " + e);
        }

        IgniteUuid sesId = fut.getTaskSession().getId();

        // Query for correct events.
        List<Event> evts = ignite.events().remoteQuery(new TaskFailedEventFilter(sesId), 0);

        info("Job failed event: " + evts.get(0));

        assert evts.size() == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMapFailed() throws Exception {
        Ignite ignite = G.ignite(getTestIgniteInstanceName());

        ignite.compute().localDeployTask(GridTaskFailedTestTask.class, GridTaskFailedTestTask.class.getClassLoader());

        ComputeTaskFuture<?> fut =
            executeAsync(ignite.compute(), GridTaskFailedTestTask.class.getName(), FailType.MAP);

        try {
            fut.get();

            assert false;
        }
        catch (IgniteException e) {
            info("Got expected grid exception: " + e);
        }

        IgniteUuid sesId = fut.getTaskSession().getId();

        // Query for correct events.
        List<Event> evts = ignite.events().remoteQuery(new TaskFailedEventFilter(sesId), 0);

        assert evts.size() == 1;

        info("Task failed event: " + evts.get(0));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testResultFailed() throws Exception {
        Ignite ignite = G.ignite(getTestIgniteInstanceName());

        ignite.compute().localDeployTask(GridTaskFailedTestTask.class, GridTaskFailedTestTask.class.getClassLoader());

        ComputeTaskFuture<?> fut =
            executeAsync(ignite.compute(), GridTaskFailedTestTask.class.getName(), FailType.RESULT);

        try {
            fut.get();

            assert false;
        }
        catch (IgniteException e) {
            info("Got expected grid exception: " + e);
        }

        IgniteUuid sesId = fut.getTaskSession().getId();

        // Query for correct events.
        List<Event> evts = ignite.events().remoteQuery(new TaskFailedEventFilter(sesId), 0);

        assert evts.size() == 1;

        info("Task failed event: " + evts.get(0));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReduceFailed() throws Exception {
        Ignite ignite = G.ignite(getTestIgniteInstanceName());

        ignite.compute().localDeployTask(GridTaskFailedTestTask.class, GridTaskFailedTestTask.class.getClassLoader());

        ComputeTaskFuture<?> fut =
            executeAsync(ignite.compute(), GridTaskFailedTestTask.class.getName(), FailType.RESULT);

        try {
            fut.get();

            assert false;
        }
        catch (IgniteException e) {
            info("Got expected grid exception: " + e);
        }

        IgniteUuid sesId = fut.getTaskSession().getId();

        // Query for correct events.
        List<Event> evts = ignite.events().remoteQuery(new TaskFailedEventFilter(sesId), 0);

        assert evts.size() == 1;

        info("Task failed event: " + evts.get(0));
    }

    /** */
    private static class TaskFailedEventFilter implements IgnitePredicate<Event> {
        /** */
        private IgniteUuid sesId;

        /**
         * @param sesId Session ID.
         */
        TaskFailedEventFilter(IgniteUuid sesId) {
            this.sesId = sesId;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            return evt instanceof TaskEvent &&
                ((TaskEvent)evt).taskSessionId() != null &&
                ((TaskEvent)evt).taskSessionId().equals(sesId) &&
                evt.type() == EVT_TASK_FAILED;
        }
    }

    /** */
    private static class GridTaskFailedTestTask extends ComputeTaskAdapter<Serializable, Serializable> {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** Ignite instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private FailType failType;

        /** {@inheritDoc} */
        @SuppressWarnings({"ProhibitedExceptionThrown"})
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Serializable arg) {
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
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> received) {
            if (failType == FailType.RESULT)
                throw new RuntimeException("Failing out of result method.");

            if (res.getException() != null)
                throw res.getException();

            return ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"ProhibitedExceptionThrown"})
        @Override public Serializable reduce(List<ComputeJobResult> results) {
            assert results != null;

            if (failType == FailType.REDUCE)
                throw new RuntimeException("Failed out of reduce method.");

            return (Serializable)results;
        }
    }

    /** */
    private static class GridTaskFailedTestJob extends ComputeJobAdapter {
        /** */
        @LoggerResource
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
