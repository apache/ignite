/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.Grid;

/**
 * Test kernal utils.
 */
public class GridKernalTestUtils {
    /**
     * Ensures singleton. 
     */
    private GridKernalTestUtils() {
        /* No-op. */
    }

    /**
     * Returns context by grid.
     *
     * @param grid Grid.
     * @return Kernal context.
     */
    public static GridKernalContext context(Grid grid) {
        assert grid != null;

        return ((GridKernal)grid).context();
    }
}
