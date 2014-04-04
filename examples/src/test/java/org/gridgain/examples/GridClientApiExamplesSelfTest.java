/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples;

import org.gridgain.examples.misc.client.api.*;
import org.gridgain.testframework.junits.common.*;

/**
 * Client Java API example self test.
 */
public class GridClientApiExamplesSelfTest extends GridAbstractExamplesTest {
    /**
     * @throws Exception If failed.
     */
    @Override protected void beforeTest() throws Exception {
        // Start up a grid node.
        startGrid("client-api-examples", "examples/config/example-compute.xml");
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridClientApiExample() throws Exception {
        ClientApiExample.main(EMPTY_ARGS);
    }
}
