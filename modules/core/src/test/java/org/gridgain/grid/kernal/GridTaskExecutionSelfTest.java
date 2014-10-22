/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.testframework.junits.common.*;

/**
 * Task execution test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridTaskExecutionSelfTest extends GridCommonAbstractTest {
    /** Grid instance. */
    private Grid grid;

    /** */
    public GridTaskExecutionSelfTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        grid = startGrid(1);

        startGrid(2);
        startGrid(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopGrid(1);
        stopGrid(2);
        stopGrid(3);

        grid = null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSynchronousExecute() throws Exception {
        GridCompute comp = grid.compute().enableAsync();

        assertNull(comp.execute(GridTestTask.class,  "testArg"));

        GridComputeTaskFuture<?> fut = comp.future();

        assert fut != null;

        info("Task result: " + fut.get());
    }
}
