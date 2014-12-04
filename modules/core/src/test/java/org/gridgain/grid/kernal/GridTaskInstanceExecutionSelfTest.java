/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
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
        Ignite ignite = G.grid(getTestGridName());

        testState = 12345;

        GridStatefulTask task = new GridStatefulTask(testState);

        assert task.getState() != null;
        assert task.getState() == testState;

        GridCompute comp = ignite.compute().enableAsync();

        assertNull(comp.execute(task,  "testArg"));

        GridComputeTaskFuture<?> fut = comp.future();

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
        @GridLoggerResource private GridLogger log;

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
        @Override public Collection<? extends GridComputeJob> split(int gridSize, Object arg) {
            log.info("Task split state: " + state);

            assert state != null;
            assert state == testState;

            return super.split(gridSize, arg);
        }

        /** {@inheritDoc} */
        @Override public GridComputeJobResultPolicy result(GridComputeJobResult res, List<GridComputeJobResult> received) throws GridException {
            log.info("Task result state: " + state);

            assert state != null;
            assert state == testState;

            return super.result(res, received);
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            log.info("Task reduce state: " + state);

            assert state != null;
            assert state == testState;

            return super.reduce(results);
        }
    }
}
