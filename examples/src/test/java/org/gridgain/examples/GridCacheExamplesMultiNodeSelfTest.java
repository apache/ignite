/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples;

/**
 * Cache examples multi-node self test.
 */
public class GridCacheExamplesMultiNodeSelfTest extends GridCacheExamplesSelfTest {
    /** {@inheritDoc} */
    @Override protected String defaultConfig() {
        return "examples/config/example-cache.xml";
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startRemoteNodes();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60 * 1000;
    }

    /** {@inheritDoc} */
    @Override public void testGridCacheStoreExample() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void testGridCacheStoreLoaderExample() throws Exception {
        // No-op.
    }
}
