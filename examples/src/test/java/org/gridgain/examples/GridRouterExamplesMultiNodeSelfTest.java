/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples;

/**
 * GridRouterExample multi-node self test.
 */
public class GridRouterExamplesMultiNodeSelfTest extends GridRouterExamplesSelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid(getTestGridName(0), "examples/config/example-cache.xml");

        startRouter("config/router/default-router.xml");

        for (int i = 1; i < RMT_NODES_CNT; i++)
            startGrid(getTestGridName(i), "examples/config/example-cache.xml");
    }
}
