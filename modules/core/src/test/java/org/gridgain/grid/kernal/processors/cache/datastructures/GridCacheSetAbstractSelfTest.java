/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

/**
 * Cache set tests.
 */
public abstract class GridCacheSetAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final String SET_NAME = "testSet";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cache().dataStructures().removeSet(SET_NAME);

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateRemove() throws Exception {
        cache().dataStructures().set(SET_NAME, false);
    }
}
