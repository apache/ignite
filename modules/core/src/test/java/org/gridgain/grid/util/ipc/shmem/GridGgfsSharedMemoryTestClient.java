/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.ipc.shmem;

import org.apache.ignite.logger.java.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.ipc.*;

import java.io.*;

/**
 * Test-purposed app launching {@link GridIpcSharedMemoryClientEndpoint} and designed
 * to be used with conjunction to {@link GridJavaProcess}.
 */
public class GridGgfsSharedMemoryTestClient {
    /**
     * Internal protocol message prefix saying that the next text in the outputted line
     * are comma-separated shared memory ids.
     */
    static final String SHMEM_IDS_MSG_PREFIX = "SHMEM_IDS_MSG_PREFIX";

    /**
     * @param args Args.
     */
    @SuppressWarnings({"BusyWait", "InfiniteLoopStatement"})
    public static void main(String[] args) {
        X.println("Starting client ...");

        // Tell our process PID to the wrapper.
        X.println(GridJavaProcess.PID_MSG_PREFIX + U.jvmPid());

        OutputStream os = null;

        try {
            GridIpcSharedMemoryClientEndpoint client = (GridIpcSharedMemoryClientEndpoint)GridIpcEndpointFactory.connectEndpoint(
                "shmem:" + GridIpcSharedMemoryServerEndpoint.DFLT_IPC_PORT, new GridJavaLogger());

            os = client.outputStream();

            // Tell our shmem ids.
            X.println(SHMEM_IDS_MSG_PREFIX + client.inSpace().sharedMemoryId() + "," +
                client.outSpace().sharedMemoryId());

            for (;;) {
                X.println("Write: 123");

                os.write(123);

                Thread.sleep(GridIpcSharedMemoryCrashDetectionSelfTest.RW_SLEEP_TIMEOUT);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            U.closeQuiet(os);
        }
    }
}
