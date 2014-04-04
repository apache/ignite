/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples;

/**
 * Deployment examples multi-node self test.
 */
public class GridDeploymentExamplesMultiNodeSelfTest extends GridDeploymentExamplesSelfTest {
    /** {@inheritDoc} */
    @Override public void testGridDeploymentExample() throws Exception {
        startRemoteNodes();

        super.testGridDeploymentExample();
    }
}
