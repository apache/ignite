/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.util.typedef.*;

import static org.gridgain.grid.ggfs.IgniteFsConfiguration.*;

/**
 * Tests for {@link GridGgfsServer} that checks all IPC endpoint registration types
 * permitted for Linux and Mac OS.
 */
public class GridGgfsServerManagerIpcEndpointRegistrationOnLinuxAndMacSelfTest
    extends GridGgfsServerManagerIpcEndpointRegistrationAbstractSelfTest {
    /**
     * @throws Exception If failed.
     */
    public void testLoopbackAndShmemEndpointsRegistration() throws Exception {
        IgniteConfiguration cfg = gridConfiguration();

        cfg.setGgfsConfiguration(
            gridGgfsConfiguration(null), // Check null IPC endpoint config won't bring any hassles.
            gridGgfsConfiguration("{type:'tcp', port:" + (DFLT_IPC_PORT + 1) + "}"),
            gridGgfsConfiguration("{type:'shmem', port:" + (DFLT_IPC_PORT + 2) + "}"));

        G.start(cfg);

        T2<Integer, Integer> res = checkRegisteredIpcEndpoints();

        // 1 regular + 3 management TCP endpoins.
        assertEquals(4, res.get1().intValue());
        assertEquals(2, res.get2().intValue());
    }
}
