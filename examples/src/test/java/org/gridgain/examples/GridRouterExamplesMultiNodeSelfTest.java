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
        startRemoteNodes();
        // Start up a router.
        startRouter("modules/clients/config/router/default-router.xml");
    }
}
