/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs;

import org.apache.ignite.*;
import org.apache.ignite.fs.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.ipc.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.util.ipc.shmem.GridIpcSharedMemoryServerEndpoint.*;

/**
 * GGFS Hadoop file system IPC self test.
 */
public abstract class GridGgfsHadoopFileSystemShmemAbstractSelfTest extends GridGgfsHadoopFileSystemAbstractSelfTest {
    /**
     * Constructor.
     *
     * @param mode GGFS mode.
     * @param skipEmbed Skip embedded mode flag.
     */
    protected GridGgfsHadoopFileSystemShmemAbstractSelfTest(IgniteFsMode mode, boolean skipEmbed) {
        super(mode, skipEmbed, false);
    }

    /** {@inheritDoc} */
    @Override protected String primaryIpcEndpointConfiguration(String gridName) {
        return "{type:'shmem', port:" + (DFLT_IPC_PORT + getTestGridIndex(gridName)) + "}";
    }

    /**
     * Checks correct behaviour in case when we run out of system
     * resources.
     *
     * @throws Exception If error occurred.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testOutOfResources() throws Exception {
        final Collection<GridIpcEndpoint> eps = new LinkedList<>();

        try {
            IgniteCheckedException e = (IgniteCheckedException)GridTestUtils.assertThrows(log, new Callable<Object>() {
                @SuppressWarnings("InfiniteLoopStatement")
                @Override public Object call() throws Exception {
                    while (true) {
                        GridIpcEndpoint ep = GridIpcEndpointFactory.connectEndpoint("shmem:10500", log);

                        eps.add(ep);
                    }
                }
            }, IgniteCheckedException.class, null);

            assertNotNull(e);

            String msg = e.getMessage();

            assertTrue("Invalid exception: " + X.getFullStackTrace(e),
                msg.contains("(error code: 28)") ||
                msg.contains("(error code: 24)") ||
                msg.contains("(error code: 12)"));
        }
        finally {
            for (GridIpcEndpoint ep : eps)
                ep.close();
        }
    }
}
