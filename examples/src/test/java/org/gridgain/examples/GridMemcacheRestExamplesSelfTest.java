/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples;

import org.gridgain.examples.misc.client.memcache.*;
import org.gridgain.testframework.junits.common.*;

/**
 * GridMemcacheRestExample self test.
 */
public class GridMemcacheRestExamplesSelfTest extends GridAbstractExamplesTest {
    /**
     * @throws Exception If failed.
     */
    @Override protected void beforeTest() throws Exception {
        // Start up a grid node.
        startGrid("memcache-rest-examples", MemcacheRestExampleNodeStartup.configuration());
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridMemcacheRestExample() throws Exception {
        MemcacheRestExample.main(EMPTY_ARGS);
    }
}
