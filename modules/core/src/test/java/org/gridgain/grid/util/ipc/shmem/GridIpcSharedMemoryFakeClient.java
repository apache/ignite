/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.ipc.shmem;

import org.apache.ignite.logger.java.*;
import org.gridgain.grid.util.ipc.*;

/**
 *
 */
public class GridIpcSharedMemoryFakeClient {
    /**
     * @param args Args.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception{
        GridIpcEndpointFactory.connectEndpoint("shmem:10500", new GridJavaLogger());
        GridIpcEndpointFactory.connectEndpoint("shmem:10500", new GridJavaLogger());
        GridIpcEndpointFactory.connectEndpoint("shmem:10500", new GridJavaLogger());
    }
}
