/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples;

import org.gridgain.examples.misc.client.interceptor.*;
import org.gridgain.testframework.junits.common.*;

/**
 * Tests {@link ClientMessageInterceptorExample} example.
 */
public class GridClientMessageInterceptorExampleSelfTest extends GridAbstractExamplesTest {
    /**
     * @throws Exception If failed.
     */
    @Override protected void beforeTest() throws Exception {
        // Start up a grid node.
        startGrid("client-api-examples", ClientMessageInterceptorExampleNodeStartup.configuration());
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridClientMessageInterceptorExample() throws Exception {
        ClientMessageInterceptorExample.main(EMPTY_ARGS);
    }
}
