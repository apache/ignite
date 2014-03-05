/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples;

import org.gridgain.examples.compute.*;
import org.gridgain.testframework.junits.common.*;

/**
 * Closure examples self test.
 */
public class GridBasicExamplesSelfTest extends GridAbstractExamplesTest {
    /**
     * @throws Exception If failed.
     */
    public void testGridBroadcastExample() throws Exception {
        ComputeBroadcastExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridCallableExample() throws Exception {
        ComputeCallableExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridClosureExample() throws Exception {
        ComputeClosureExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridExecutorExample() throws Exception {
        ComputeExecutorServiceExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridReducerExample() throws Exception {
        ComputeReducerExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridRunnableExample() throws Exception {
        ComputeRunnableExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridTaskMapExample() throws Exception {
        ComputeTaskMapExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridTaskSplitExample() throws Exception {
        ComputeTaskSplitExample.main(EMPTY_ARGS);
    }
}
