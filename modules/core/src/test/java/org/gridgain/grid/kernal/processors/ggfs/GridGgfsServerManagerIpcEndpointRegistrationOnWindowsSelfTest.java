/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.ipc.loopback.*;
import org.gridgain.grid.util.ipc.shmem.*;

import java.util.concurrent.*;

import static org.gridgain.testframework.GridTestUtils.*;

/**
 * Tests for {@link GridGgfsServerManager} that checks shmem IPC endpoint registration
 * forbidden for Windows.
 */
public class GridGgfsServerManagerIpcEndpointRegistrationOnWindowsSelfTest
    extends GridGgfsServerManagerIpcEndpointRegistrationAbstractSelfTest {
    /**
     * @throws Exception If failed.
     */
    public void testShmemEndpointsRegistration() throws Exception {
        Throwable e = assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgniteConfiguration cfg = gridConfiguration();

                cfg.setGgfsConfiguration(gridGgfsConfiguration(
                    "{type:'shmem', port:" + GridIpcSharedMemoryServerEndpoint.DFLT_IPC_PORT + "}"));

                return G.start(cfg);
            }
        }, GridException.class, null);

        assert e.getCause().getMessage().contains(" should not be configured on Windows (configure " +
            GridIpcServerTcpEndpoint.class.getSimpleName() + ")");
    }
}
