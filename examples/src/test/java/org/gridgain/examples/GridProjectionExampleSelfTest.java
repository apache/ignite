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
 *
 */
public class GridProjectionExampleSelfTest extends GridAbstractExamplesTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        // Start up a grid node.
        startGrid("grid-projection-example", DFLT_CFG);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridProjectionExample() throws Exception {
        ComputeProjectionExample.main(EMPTY_ARGS);
    }
}
