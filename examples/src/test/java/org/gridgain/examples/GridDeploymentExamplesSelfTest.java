/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples;

import org.gridgain.examples.misc.deployment.*;
import org.gridgain.examples.misc.deployment.gar.*;
import org.gridgain.testframework.junits.common.*;

/**
 * Deployment examples self test.
 */
public class GridDeploymentExamplesSelfTest extends GridAbstractExamplesTest {
    /**
     * @throws Exception If failed.
     */
    public void testGridDeploymentExample() throws Exception {
        DeploymentExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridGarHelloWorldExample() throws Exception {
        GarDeploymentExample.main(EMPTY_ARGS);
    }
}
