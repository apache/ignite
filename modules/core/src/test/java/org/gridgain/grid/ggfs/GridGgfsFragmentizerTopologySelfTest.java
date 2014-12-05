/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs;

import org.apache.ignite.*;

/**
 * Tests coordinator transfer from one node to other.
 */
public class GridGgfsFragmentizerTopologySelfTest extends GridGgfsFragmentizerAbstractSelfTest {
    /**
     * @throws Exception If failed.
     */
    public void testCoordinatorLeave() throws Exception {
        stopGrid(0);

        // Now node 1 should be coordinator.
        try {
            IgniteFsPath path = new IgniteFsPath("/someFile");

            IgniteFs ggfs = grid(1).fileSystem("ggfs");

            try (GridGgfsOutputStream out = ggfs.create(path, true)) {
                for (int i = 0; i < 10 * GGFS_GROUP_SIZE; i++)
                    out.write(new byte[GGFS_BLOCK_SIZE]);
            }

            awaitFileFragmenting(1, path);
        }
        finally {
            startGrid(0);
        }
    }
}
