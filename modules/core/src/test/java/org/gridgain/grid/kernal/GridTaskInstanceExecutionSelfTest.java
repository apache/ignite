/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

/**
 * Task instance execution test.
 */
@SuppressWarnings("PublicInnerClass")
@GridCommonTest(group = "Kernal Self")
public class GridTaskInstanceExecutionSelfTest extends GridCommonAbstractTest {
    /** */
    private static Object testState;

    /** */
    public GridTaskInstanceExecutionSelfTest() {
        super(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSynchronousExecute() throws Exception {
        Ignite ignite = G.ignite(getTestGridName());

        testState = 12345;

        GridStatefulTask task = new GridStatefulTask(testState);

        assert task.getState() != null;
        assert task.getState() == testState;

        IgniteCompute comp = ignite.compute().enableAsync();

        assertNull(comp.execute(task,  "testArg"));

        ComputeTaskFuture<?> fut = comp.future();

        assert fut != null;

        info("Task result: " + fut.get());
    }

    /**
     * Stateful task.
     */
    public static class GridStatefulTask extends GridTestTask {
        /** */
        private Object state;

        /** */
        @IgniteLoggerResource
        private IgniteLogger log;

        /**
         * @param state State.
         */
        public GridStatefulTask(Object state) {
            this.state = state;
        }

        /**
         * @return The state.
         */
        public Object getState() {
            return state;
        }

        /** {@inheritDoc} */
        @Override public Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            log.info("Task split state: " + state);

            assert state != null;
            assert state == testState;

            return super.split(gridSize, arg);
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> received) throws GridException {
            log.info("Task result state: " + state);

            assert state != null;
            assert state == testState;

            return super.result(res, received);
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws GridException {
            log.info("Task reduce state: " + state);

            assert state != null;
            assert state == testState;

            return super.reduce(results);
        }
    }
}
