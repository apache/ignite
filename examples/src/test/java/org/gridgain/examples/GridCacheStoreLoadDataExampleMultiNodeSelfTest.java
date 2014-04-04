/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples;

import org.gridgain.examples.datagrid.store.*;
import org.gridgain.testframework.junits.common.*;

/**
 *
 */
public class GridCacheStoreLoadDataExampleMultiNodeSelfTest extends GridAbstractExamplesTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        for (int i = 0; i < RMT_NODES_CNT; i++)
            startGrid("node-" + i, CacheNodeWithStoreStartup.configure());
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridCacheStoreLoaderExample() throws Exception {
        CacheStoreLoadDataExample.main(EMPTY_ARGS);
    }
}
