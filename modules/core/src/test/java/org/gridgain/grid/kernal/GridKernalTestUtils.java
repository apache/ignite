/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.Ignite;

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
     * @param ignite Grid.
     * @return Kernal context.
     */
    public static GridKernalContext context(Ignite ignite) {
        assert ignite != null;

        return ((GridKernal) ignite).context();
    }
}
