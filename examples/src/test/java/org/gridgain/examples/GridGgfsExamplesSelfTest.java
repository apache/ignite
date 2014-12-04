/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples;

import org.gridgain.examples.ggfs.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

/**
 * GGFS examples self test.
 */
public class GridGgfsExamplesSelfTest extends GridAbstractExamplesTest {
    /** Grid name for light client example. */
    private static final String CLIENT_LIGHT_GRID_NAME = "client-light-grid";

    /** GGFS config with shared memory IPC. */
    private static final String GGFS_SHMEM_CFG = "modules/core/src/test/config/ggfs-shmem.xml";

    /** GGFS config with loopback IPC. */
    private static final String GGFS_LOOPBACK_CFG = "modules/core/src/test/config/ggfs-loopback.xml";

    /** GGFS no endpoint config. */
    private static final String GGFS_NO_ENDPOINT_CFG = "modules/core/src/test/config/ggfs-no-endpoint.xml";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        String cfgPath = gridName == null ? (U.isWindows() ? GGFS_LOOPBACK_CFG : GGFS_SHMEM_CFG) :
            GGFS_NO_ENDPOINT_CFG;

        IgniteConfiguration cfg = GridGainEx.loadConfiguration(cfgPath).get1();

        cfg.setGridName(gridName);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGgfsApiExample() throws Exception {
        startGrids(3);

        try {
            GgfsExample.main(EMPTY_ARGS);
        }
        finally {
            stopAllGrids();
        }
    }
}
