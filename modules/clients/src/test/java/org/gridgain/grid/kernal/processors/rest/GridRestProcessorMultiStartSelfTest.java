/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest;

import org.gridgain.grid.*;
import org.gridgain.testframework.junits.common.*;

/**
 * Rest processor test.
 */
public class GridRestProcessorMultiStartSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setRestEnabled(true);

        return cfg;
    }

    /**
     * Test that multiple nodes can start with JETTY enabled.
     *
     * @throws Exception If failed.
     */
    public void testMultiStart() throws Exception {
        try {
            for (int i = 0; i < GRID_CNT; i++)
                startGrid(i);

            stopGrid(0);
        }
        finally {
            stopAllGrids();
        }
    }
}
